# 06 · 外部集成、Skills 与 Agent 模板

涵盖 `internal/integrations`（~20K LOC，绝大部分是 Lark）、`internal/skill`、`internal/agenttmpl`、`internal/storage`、邮件、webhook、密钥加密。

> 设计原则：**部分装配（partial wiring）**——缺哪个 env/key 就优雅关掉哪个功能（Lark 未配 master key → 不起 goroutine；S3 未配 → 落本地盘），而非启动崩溃。

---

## 1. 集成总览

| 集成 | 位置 | 对接 | 类型 |
|------|------|------|------|
| **Lark / 飞书** | `internal/integrations/lark/`（56 文件） | Lark(国际) + Feishu(国内) OAPI + WS 长连 | 平台消息（最复杂） |
| **GitHub** | `internal/handler/github.go` | GitHub App OAuth + PR/issue webhook | DevOps |
| **邮件** | `internal/service/email.go` | Resend SaaS 或 SMTP relay | 验证码/邀请 |
| **Webhook 触发器** | `internal/handler/autopilot_webhook.go` | 通用 webhook（GitHub/GitLab…） | autopilot 入站触发 |
| **Stripe** | `internal/handler/cloud_billing.go` | Stripe billing webhook | 计费（SaaS） |
| **S3 / CloudFront** | `internal/storage/s3.go` | AWS S3 或 MinIO + 可选 CDN | 对象存储 |
| **secretbox** | `internal/util/secretbox/` | AES-256-GCM 静态加密 | 凭证保护 |

---

## 2. Lark / 飞书集成（深入）

这是整个后端最复杂的子系统，核心难点是**多副本协调**：必须保证全局**至多一个**进程副本持有某 installation 的 WebSocket 租约并消费其事件（doc.go 称之为 §4.4 不变量），否则消息重复处理。

### 组件层次
```
Hub (§4.4 WS 租约 supervisor, hub.go:193)
 ├─ supervisors[installation_id] → supervise() goroutine
 │     ├─ acquireLease (CAS, per-supervisor token)
 │     ├─ renewLeaseUntil (后台 ticker)
 │     ├─ EventConnector.Run (ws_connector.go: 长连 I/O)
 │     └─ releaseLease
 └─ Dispatcher (入站管线, dispatcher.go)
       ├─ ChatSessionService (append + dedup)
       ├─ AuditLogger (只记元数据, 不记消息体)
       ├─ IssueService (处理 /issue)
       └─ TaskService (chat / slash 任务入队)
```

### 租约协调（多副本安全）
- **Per-supervisor token**（`hub.go:488`）：每个 supervisor 启动时铸唯一 token（`nodeID + "-g" + generation`），靠 `ReleaseLarkWSLease` 的 token 围栏防止轮换后的旧 supervisor 误清新 supervisor 刚拿到的租约。
- **租约 TTL/续期**（`hub.go:156`）：默认 `LeaseTTL=90s`、`LeaseRenewInterval=30s`，60s 缓冲吸收瞬时 DB 抖动，单次续期失败不丢租约。
- **租约丢失检测**（`hub.go:680`）：`acquireLease` CAS 失败（被别的副本抢走）→ `cancelRun` 取消 connector 上下文 → 看门狗（`ws_connector.go:47`）打断阻塞的 `ReadMessage`，强制 `conn.Run` 立即退出。
- **优雅关停**（`main.go:428-441`）：SIGTERM 后先排空 HTTP，再 `sweepCtx` 取消停所有 supervisor，`Hub.WaitWithTimeout`（默认 15s）join；未及时释放的租约靠 TTL 在另一副本侧自然过期。

### 入站事件管线（WS 帧 → 入队任务，`dispatcher.go:78`）
1. **帧解码**：gorilla 读二进制 protobuf Frame，抽出 JSON payload（`ws_frame_decoder.go` + `ws_chunk_assembler.go`）。
2. **enrich**（`inbound_enricher.go`）：可选 HTTP 回查展开引用回复 / 转发捆绑。
3. **dispatch**：installation 状态检查（revoked 丢弃）→ message id 去重（两阶段 claim，应对 WS 重放）→ 群聊过滤（仅 @ 提及或卡片回复）→ 身份解析（open_id → `lark_user_binding`）→ ensure session（`lark_chat_session_binding`）→ append message（事务内带去重标记）→ `/issue` 命令解析 → 任务入队。返回带类型化 Outcome 的 `DispatchResult`（Ingested/NeedsBinding/AgentOffline/AgentArchived/Dropped）。
4. **debounce**：连续消息在短窗口内合并为一次 agent run。
5. **outcome 回复**（`hub.go:794`）：detached goroutine 发 NeedsBinding 卡片 / 离线提示 / typing 指示器——脱离入站 ACK 关键路径（Lark 3 秒 ACK 死线）。

### 其它子系统
- **绑定 token**（`binding_token.go:74`）：为新 Lark 用户铸单次、15 分钟令牌绑定到 Multica 账号；哈希兑换、事务内绑定。
- **设备流注册**（`registration_service.go:90`）：RFC 8628 扫码安装，成功后铸 app_id/app_secret 并写 `lark_installation` + 自动绑定安装者。
- **出站卡片同步**（`outbound.go` + `patcher.go`）：订阅事件总线，节流后 PATCH 更新用户侧交互卡片，保持任务进度同步。
- **审计**（`audit.go`）：所有入站事件记 `lark_inbound_audit`（含 drop_reason），但**刻意不记消息体**（隐私）。
- **多区域/回填**：`region_backfill.go`、`union_id_backfill.go` 桥接 per-installation region 与 union_id。

`app_secret` 用 `secretbox`（AES-256-GCM）静态加密存库，明文从不出现在行/日志/测试夹具。

---

## 3. GitHub 集成（`internal/handler/github.go`）

- **GitHub App OAuth**（`github.go:173`）：用 `GITHUB_APP_SLUG` + `GITHUB_WEBHOOK_SECRET`；`GitHubConnect` 生成安装 URL，`GitHubSetupCallback` 处理回调。
- **安装管理**（`github.go:101`）：存 `github_installation`（installation_id、account_login/type/avatar）；非管理员 WS 广播剥离 `InstallationID`。
- **PR 跟踪**（`github.go:127`）：从 webhook 同步 PR 元数据（标题、state、check 状态、diff 统计）到 `github_pull_request`，聚合 check-suite 结论。
- **webhook handler**：HMAC-SHA256 校验签名，消费 push/pull_request/check_run，更新库并 WS 广播。

---

## 4. 邮件（`internal/service/email.go`）

双后端，按 env 选择：
- **SMTP relay**（首选，`email.go:163`）：支持 STARTTLS（默认）/ 隐式 TLS(465)；无认证 relay 或 LOGIN/PLAIN（PLAIN 不被识别时回退 LOGIN，兼容阿里云/老服务器）；8BITMIME 不可用时对非 ASCII 名字走 quoted-printable。
- **Resend API**：`RESEND_API_KEY`。
- **DEV**：都不配则验证码打 stdout。

发送内容：
- 验证码（`email.go:326`）：6 位服务端生成、10 分钟过期。
- 邀请（`email.go:354`）：HTML 转义 inviter/workspace 名（防 XSS）+ deep-link；subject 去控制字符、截到 60 runes（防钓鱼塞料）。

配置 env：`RESEND_API_KEY`、`RESEND_FROM_EMAIL`(默认 noreply@multica.ai)、`SMTP_HOST/PORT/USERNAME/PASSWORD`、`SMTP_TLS`(starttls/implicit/smtps)、`SMTP_TLS_INSECURE`、`SMTP_EHLO_NAME`。

---

## 5. Webhook 触发器（`internal/handler/autopilot_webhook.go`）

驱动 autopilot 的通用入站 webhook：
- **签名校验**：provider 感知（GitHub/GitLab HMAC-SHA256…）。
- **事件归一**：转成统一 `WebhookEnvelope`（provider + event + 元数据）做过滤匹配。
- **过滤**：按事件类型/分支/action 等可配过滤，scope 限定只匹配相关事件。
- **投递审计**：`webhook_delivery` 行（时间、状态、错误）。
- **限流**：per-workspace + per-source-IP。

支持在 autopilot 触发器上轮换 webhook token / 设置 signing secret，并可 replay 历史投递（`/api/autopilots/{id}/deliveries/{deliveryId}/replay`）。

---

## 6. 对象存储（`internal/storage`）

抽象接口（`storage.go:9`）：`Upload`、`Delete`/`DeleteKeys`、`KeyFromURL`、`CdnDomain`、`GetReader`（流式代理下载）；presign 接口 `PresignGet` / `PresignGetWithContentDisposition`。

- **S3 实现**（`s3.go:20`）：`aws-sdk-go-v2`，env `S3_BUCKET`(必需)、`S3_REGION`(默认 us-west-2)、`AWS_ACCESS_KEY_ID/SECRET`(缺则默认凭证链)、`CLOUDFRONT_DOMAIN`(走 CDN)、`AWS_ENDPOINT_URL`(MinIO 走 path-style)。真实 AWS 用 INTELLIGENT_TIERING，MinIO 用 STANDARD。`S3_BUCKET` 未设则返回 nil，优雅回退本地存储。
- **本地实现**（`local.go`）：文件系统回退，开发/单机用。

存的内容：附件、头像、任务产物等。

---

## 7. Skills 系统（`internal/skill`）

一个 **Skill** = 可复用的 agent 能力：一份 `SKILL.md`（YAML frontmatter `name`/`description` + 正文指令）+ 可选附属文件。**无版本、不可变**——按 source URL（GitHub repo / skills.sh registry）导入，不按版本 pin。

- **frontmatter 解析**（`frontmatter.go`）：`ParseSkillFrontmatter` 抽 YAML 块，软 schema（map[string]any），与 TS 侧 `packages/core/skills/frontmatter.ts` 对齐。
- **保留路径**（`reserved.go`）：`ContentFilename = "SKILL.md"` 保留；`IsReservedContentPath` 用 `filepath.Clean` + 大小写不敏感比较防 `sub/../SKILL.md` 绕过。
- **存储**：workspace 本地副本存 `skill` 表（name/description/source_url/content）+ `skill_file`（多文件 path/content），按 source_url 去重。
- **内置 skill**：随 server 二进制嵌入（`internal/service/builtin_skills.go`），新 workspace 默认获得。

导入流（`handler/skill.go`，经 `/api/skills/import`）：解析 source URL → 拉 SKILL.md + 附属文件 → 解析 frontmatter → upsert skill 行。

---

## 8. Agent 模板（`internal/agenttmpl`）

**Agent Template** = agent 创建的策展启动套件：`Slug`(=文件名)、`Name`、`Description`、`Category`/`Icon`/`Accent`(视觉)、`Instructions`(写入新 agent 的 instructions)、`Skills`(待导入的 skill 引用列表，含 source URL + 缓存的 name/description)。

- **静态、仓库内嵌**（`types.go`）：`//go:embed templates/*.json`，启动时一次性加载，无运行时改动、无 admin UI。
- **Registry 加载**（`loader.go:30`）：扫描嵌入 FS，**严格校验**（slug 与文件名一致、必填字段、kebab-case、skill 必有 source URL、无重复 slug），任一失败则启动中止（loud-fail）。
- **物化**：用户从"创建 agent"选模板 → `GET /api/agent-templates/{slug}` → `POST /api/agents`（带 instructions + skills）→ handler 建 agent 行 + 逐个导入 skill（即 `/api/agents/from-template` 一个事务完成）。
