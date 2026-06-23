# 02 · HTTP API 层：路由、鉴权、中间件

涵盖 `cmd/server/router.go`、`internal/handler`、`internal/middleware`、`internal/auth`。

---

## 1. Router 构造

- `NewRouter(pool, hub, bus, analyticsClient, rdb)`（`cmd/server/router.go:99`）：测试/简单部署用的高层入口。
- `NewRouterWithOptions(pool, hub, bus, analytics, rdb, opts)`（`router.go:123`）：完整工厂，返回 `(chi.Router, *handler.Handler)`——返回 handler 是为了让 `main` 在优雅关停时驱动 LarkHub 等后台生命周期。

### `handler.Handler` 结构体（`internal/handler/handler.go:98`）
一个"大依赖容器"，集中持有所有 handler 需要的协作对象：
- **DB/事务**：`Queries`、`DB`、`TxStarter`
- **实时/事件**：`Hub`、`DaemonHub`、`Bus`
- **业务服务**：`TaskService`、`IssueService`、`AutopilotService`、`EmailService`
- **存储**：`Storage`（S3 或本地）、`CFSigner`（CloudFront 签名 cookie）
- **鉴权缓存**：`PATCache`、`DaemonTokenCache`、`MembershipCache`
- **Lark**（可选）：`LarkInstallations`、`LarkBindingTokens`、`LarkRegistration`、`LarkAPIClient`、`LarkHub`
- **限流/可观测**：`WebhookRateLimiter`、`WebhookIPRateLimiter`、`Analytics`、`Metrics`
- **云**：`CloudRuntime`（SaaS 远端 fleet 代理）、`HeartbeatScheduler`

### 全局中间件链（`router.go:396-423`，对所有路由生效，顺序如下）
1. `chi middleware.RequestID` —— 生成 `X-Request-ID`
2. `middleware.ClientMetadata` —— 解析 `X-Client-Platform/Version/OS`
3. `middleware.RequestLogger` —— 结构化 slog（按状态码分级，脱敏 webhook token）
4. `HTTPMetrics.Middleware`（可选）—— 直方图 + 请求/响应计数
5. `chi middleware.Recoverer` —— panic → 500
6. `middleware.ContentSecurityPolicy` —— 严格 CSP（`default-src 'self'`、`connect-src wss:`、`frame-ancestors 'none'`）
7. `cors.Handler` —— 允许凭证，方法 GET/POST/PUT/PATCH/DELETE，允许头含 `Authorization`、`X-Workspace-ID/Slug`、`X-Agent-ID`、`X-Task-ID`、`X-CSRF-Token` 等

---

## 2. 鉴权模型：5 种 token

Multica 不是单一鉴权方式，而是按调用方区分 **5 类 Bearer token**（均经 `Authorization: Bearer <token>` 或特定路径）：

| 类型 | 前缀/形式 | 绑定 | 使用者 | 中间件 | 设置的上下文 |
|------|-----------|------|--------|--------|--------------|
| **JWT** | `multica_auth` cookie 或 Bearer JWT | `sub`=user_id | Web / 桌面端 | `middleware.Auth` | `X-User-ID` |
| **PAT** | `mul_` + 40 hex（存 SHA-256） | user_id | CLI / daemon / 外部集成 | `middleware.Auth`(+PATCache) | `X-User-ID` |
| **Agent Task Token** | `mat_` + 40 hex | (user, agent, task, workspace) | agent 子进程（daemon 注入） | `middleware.Auth` | `X-User-ID`+`X-Agent-ID`+`X-Task-ID`+`X-Workspace-ID`+`X-Actor-Source: task_token` |
| **Daemon Token** | `mdt_` + 40 hex | (workspace, daemon) | daemon 注册/认领/回报 | `middleware.DaemonAuth` | `X-User-ID`（解析自 workspace owner） |
| **Cloud Node PAT** | `mcn_` + base64 | owner_id（远端 Fleet 校验） | cloud runtime 节点 | `Auth`+`DaemonAuth` | `X-User-ID`+`X-Actor-Source: cloud_pat` |

实现位置：`internal/middleware/auth.go:20`、`internal/middleware/daemon_auth.go:62`；缓存与校验器在 `internal/auth/`（`jwt.go`、`pat_cache.go`、`daemon_token_cache.go`、`cloud_pat.go`）。

### Actor 归属与横向移动防护
- **`X-Actor-Source`** 头只能由服务端设置：中间件在重新盖章前会先剥离客户端传入的值。空=人类；`task_token`=以 `mat_` 运行的 agent；`cloud_pat`=以 `mcn_` 运行的 cloud 节点。
- **`RequireHumanActor`**（`internal/handler/actor_guards.go:96`）守卫账户级敏感操作（如 cloud billing 余额、checkout），**拒绝 `task_token` 与 `cloud_pat`**——防止 prompt injection 或节点逃逸导致的横向移动。
- 自托管（未配置 Fleet URL）时 `mcn_` token 直接 401 拒绝。

---

## 3. 工作区多租户（`internal/middleware/workspace.go:68`）

每个请求的 workspace 解析有明确优先级（`ResolveWorkspaceIDFromRequest`）：
1. **task-token 绑定**（`X-Actor-Source == task_token`）—— 权威，agent 无法覆盖
2. 中间件已注入的上下文（快路径）
3. `X-Workspace-Slug` 头 → slug 查 UUID（前端新方案）
4. `?workspace_slug` query
5. `X-Workspace-ID` 头（CLI/daemon 兼容）
6. `?workspace_id` query

成员校验：
- `RequireWorkspaceMember(queries)` —— 解析 workspace + 校验 membership，注入 `(workspaceID, member)`
- `RequireWorkspaceMemberFromURL(queries, "id")` —— 从 URL 参数取 workspace（如 `/api/workspaces/{id}/...`）
- `RequireWorkspaceRole(queries, "admin", "owner")` —— 在 membership 之上再查角色

**纵深防御**（`workspace.go:213`）：即便 workspace 来自 URL 参数，task-token 请求也必须校验 token 绑定的正是该 workspace，否则 403「task token is bound to a different workspace」。

---

## 4. 中间件清单（`internal/middleware/`，~10 文件）

| 文件 | 中间件 | 作用 |
|------|--------|------|
| `auth.go` | `Auth` | JWT/PAT/task-token/cloud-PAT 校验，盖 `X-User-ID` |
| `daemon_auth.go` | `DaemonAuth` | daemon 专用：依次试 `mdt_`→`mcn_`→`mul_`→JWT，存 workspace/daemon 到上下文 |
| `workspace.go` | `RequireWorkspaceMember[FromURL]`、`RequireWorkspaceRole*` | 工作区解析 + 成员/角色校验 |
| `request_logger.go` | `RequestLogger` | 结构化日志；跳过 `/health`；按状态码分级（5xx→Error，软 404→Info）；脱敏 webhook token |
| `client.go` | `ClientMetadata` | 提取客户端平台/版本/OS |
| `cloudfront.go` | `RefreshCloudFrontCookies` | CDN 签名 cookie 过期时续签 |
| `csp.go` | `ContentSecurityPolicy` | CSP 头 |
| `ratelimit.go` | `RateLimit` | 每 IP 固定窗口限流（Redis 实现，nil 则 no-op），支持可信代理 CIDR |

**限流应用点**：`/auth/send-code` 5/分、`/auth/verify-code` 20/分、`/auth/google` 5/分、`/api/contact-sales` 5/时、webhook 入口按 URL + IP 限流。

---

## 5. 路由地图（按鉴权层级分组）

完整路由在 `cmd/server/router.go:426-1002`。下面给出结构与代表性端点。

### 公开路由（无需鉴权，`router.go:426`）
```
GET  /health  /readyz  /healthz
GET  /health/realtime          (metrics token 守卫)
GET  /ws                       (浏览器 WS，query 参数鉴权)
GET  /uploads/*                (本地存储时的文件服务)
GET  /api/config               (前端配置：注册开关/CDN/分析)
POST /api/contact-sales        (限流)
POST /auth/send-code  /auth/verify-code  /auth/google  /auth/logout
```

### Webhook 入口（Bearer token / 签名校验，`router.go:480`）
```
POST /api/webhooks/autopilots/{token}   (autopilot webhook，URL 里带 token)
POST /api/webhooks/github               (HMAC-SHA256 校验)
POST /api/webhooks/stripe               (签名校验)
GET  /api/github/setup                  (GitHub App 安装回调)
```

### Daemon API（`middleware.DaemonAuth`，`router.go:495`）
daemon 全生命周期都走这组：
```
POST /api/daemon/register  /deregister  /heartbeat
GET  /api/daemon/ws                                   (daemon WS upgrade)
GET  /api/daemon/workspaces/{wsId}/repos  /runtime-profiles
POST /api/daemon/runtimes/{rtId}/tasks/claim          (认领下一个 task)
GET  /api/daemon/runtimes/{rtId}/tasks/pending
POST /api/daemon/tasks/{taskId}/start /progress /complete /fail /usage
POST /api/daemon/tasks/{taskId}/messages              (agent 调试 transcript)
POST /api/daemon/runtimes/{rtId}/recover-orphans      (孤儿 task 恢复)
GET  /api/daemon/{type}/{id}/gc-check                 (生命周期校验)
```

### 受保护路由（`middleware.Auth` + 可选工作区成员）
**用户级**（无需 workspace）：`/api/me`(+`/onboarding`)、`/api/tokens`(PAT CRUD)、`/api/invitations/*`、`/api/upload-file`、`/api/cli-token`、`/api/cloud-billing/*`（需 `RequireHumanActor`）。

**工作区级**（`RequireWorkspaceMember`，资源族）：

| 资源族 | 前缀 | 代表端点 |
|--------|------|----------|
| Workspaces | `/api/workspaces` | CRUD、`/members`、`/invitations`、`/github/*`、`/lark/*`、`/runtime-profiles/*` |
| Issues | `/api/issues` | CRUD、`/search`、`/grouped`、`/quick-create`、`/batch-update`、`{id}/comments`、`/timeline`、`/subscribe`、`/active-task`、`/rerun`、`/task-runs`、`/labels`、`/metadata/{key}`、`/pull-requests` |
| Labels | `/api/labels` | CRUD |
| Projects | `/api/projects` | CRUD、`/search`、`{id}/resources` |
| Squads | `/api/squads` | CRUD、`{id}/members`(+`/status`/`/role`) |
| Autopilots | `/api/autopilots` | CRUD、`/trigger`、`{id}/runs`、`/deliveries`(+`/replay`)、`/triggers`(+`/rotate-webhook-token`/`/signing-secret`) |
| Agents | `/api/agents` | CRUD、`/from-template`、`/archive`、`/restore`、`/cancel-tasks`、`{id}/skills`、`{id}/env`(owner-only，审计) |
| Agent Templates | `/api/agent-templates` | 列表、`{slug}` |
| Skills | `/api/skills` | CRUD、`/search`、`/import`、`{id}/files` |
| Runtimes | `/api/runtimes` | 列表、`{id}` CRUD、`/usage`(+`/by-agent`/`/by-hour`)、`/activity`、`/update`、`/models`、`/local-skills`(+`/import`) |
| Cloud Runtime | `/api/cloud-runtime` | fleet 状态、`/nodes`(start/stop/reboot/exec)（SaaS-only，自托管 503） |
| Chat | `/api/chat/sessions` | CRUD、`{id}/messages`、`/pending-task`、`/read` |
| Inbox | `/api/inbox` | 列表、`/unread-count`、`/mark-all-read`、`/archive-*`、`{id}/read`、`{id}/archive`、`/notification-preferences` |
| Dashboard | `/api/dashboard` | `/usage/daily`、`/usage/by-agent`、`/agent-runtime`、`/runtime/daily` |
| Pins | `/api/pins` | 列表、创建、`/reorder`、删除 |
| Attachments / Comments | `/api/attachments`、`/api/comments` | 元数据/内容/删除、评论编辑/resolve/reactions |

---

## 6. Handler 组织约定

- **按领域分文件**（`internal/handler/`，~59 个非测试文件）：`issue.go`、`agent.go`、`skill.go`、`autopilot.go`、`chat.go`、`runtime.go`、`workspace.go`、`project.go`、`squad.go`、`daemon.go`、`github.go`、`lark.go`、`cloud_billing.go`、`inbox.go` 等。
- **响应/错误辅助**（`handler.go:227`）：`writeJSON(w, status, v)`、`writeMeasuredJSON`（返回 body 大小用于慢端点诊断）、`writeError(w, status, msg)` → `{"error": msg}`。全 JSON，无 HTML 模板。
- **输入校验**（`handler.go:278`）：`parseUUIDOrBadRequest` / `parseUUIDSliceOrBadRequest`（无效写 400），`json.NewDecoder(r.Body).Decode`，`chi.URLParam`。
- **事件发布**（`handler.go:307`）：`h.publish(eventType, workspaceID, actorType, actorID, payload)` 与 `h.publishTask(...)` 经 `h.Bus` 发到事件总线，驱动实时 / activity / 通知 / webhook / 分析。
- **分页**：无统一封装，多为 `?limit&offset` 或 `?page&per_page`，部分端点有硬上限（如 children 200）。
- **Redis 加速（多节点）**：PAT/daemon-token/membership 缓存、update/model/skill 请求队列、webhook 限流、liveness store 在有 Redis 时切到 Redis 实现，多节点共享（`router.go:360-390`）。
