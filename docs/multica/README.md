# Multica Server 架构分析

> 分析对象：`/srv/gimhq/multica/server`（Multica monorepo 中的 Go 后端目录）
> 代码规模：约 **131K 行 Go**（非测试），159 对 Postgres migration（最新编号 122）
> 模块路径：`github.com/multica-ai/multica/server`，`go 1.26.1`
> 本文档基于对源码的系统性走读生成，所有结论可在代码中找到 `file:line` 对应。

---

## 这是什么

Multica 是一个 **"托管智能体（managed agents）"平台**：把编码 agent（Claude Code、Codex、Copilot CLI、Gemini、Cursor……）变成和人一样的团队成员——出现在 issue 看板的 assignee 下拉里、在评论区发言、自己认领任务写代码、汇报进度。

`server/` 这个目录虽然叫 "server"，但它实际上同时包含了**三类可执行程序**和一套共享库。理解这一点是读懂整个目录的关键：

| 二进制 | 入口 | 角色 | 运行位置 |
|--------|------|------|----------|
| **API Server** | `cmd/server` | 多租户云后端：HTTP API + WebSocket + 后台 worker | 云端 / 自托管服务器 |
| **`multica` CLI + Daemon** | `cmd/multica` | 命令行工具，且内嵌 **daemon**（`internal/daemon`）——真正在用户机器上拉起 agent 进程的执行器 | 用户本地机器 |
| **Migrator / Backfill** | `cmd/migrate`、`cmd/backfill_*` | 数据库迁移与历史数据回填工具 | 部署期 / 运维 |

也就是说：**云端 API Server 负责编排与持久化，本地 Daemon 负责真正执行 agent**，两者通过 HTTP（REST 轮询）+ WebSocket（实时唤醒）双通道协作。Agent 永远跑在用户自己的机器上（或可选的 cloud runtime fleet），代码、密钥、git 仓库都不离开用户环境——这是产品的核心信任模型。

```
                 ┌────────────────────────────────────────────┐
   浏览器/桌面端  │              API Server (cmd/server)         │
   ───WS+REST──▶ │  chi router · 中间件鉴权 · handler · service │
                 │  events.Bus → realtime.Hub → Redis relay     │
                 │  Postgres(pgx+sqlc) · 后台 sweeper/scheduler  │
                 └───────▲──────────────────────┬───────────────┘
                         │ daemon WS (唤醒)       │ REST (注册/认领/回报)
                         │                        ▼
                 ┌───────┴────────────────────────────────────┐
   用户本地机器   │          Daemon (internal/daemon)            │
                 │  注册 runtime · 轮询认领 task · 起 agent 进程 │
                 │  git worktree 隔离 · 流式回报 · 失败分类       │
                 │      pkg/agent: 13+ 种 agent provider 抽象    │
                 └─────────────────────────────────────────────┘
```

---

## 文档导航

| 文档 | 内容 |
|------|------|
| [01-architecture.md](01-architecture.md) | **系统总览**：三类二进制、进程模型、启动序列、后台 worker 全清单、单机 vs 多节点拓扑、环境变量 |
| [02-http-api.md](02-http-api.md) | **HTTP API 层**：chi router 构造、5 种 token 鉴权模型、中间件链、工作区多租户、完整路由地图 |
| [03-realtime-and-events.md](03-realtime-and-events.md) | **事件与实时**：进程内 `events.Bus`、监听器注册顺序、realtime Hub + Redis relay 三模式、daemon WS 通道、wire protocol |
| [04-agent-execution.md](04-agent-execution.md) | **Agent 执行引擎**：daemon 运行时、`pkg/agent` provider 抽象、task 生命周期、git worktree 隔离、失败分类与脱敏、cloud runtime |
| [05-services-and-scheduling.md](05-services-and-scheduling.md) | **服务层与调度**：`TaskService`、`AutopilotService`、runtime sweeper / 心跳 / autopilot 调度器 / 失败监控、DB 分布式 cron |
| [06-integrations.md](06-integrations.md) | **外部集成**：Lark/飞书（最复杂，含 WS lease 多副本协调）、GitHub、邮件、webhook、Stripe、S3/CloudFront、secretbox 加密 |
| [07-data-model.md](07-data-model.md) | **数据模型**：sqlc/pgx 技术栈、~50 张核心表与关系、migration 工具链 |
| [08-observability-and-cli.md](08-observability-and-cli.md) | **可观测性与 CLI**：Prometheus 业务指标、slog 日志、`multica` CLI 命令全景 |

---

## 核心概念速查（Glossary）

| 术语 | 含义 |
|------|------|
| **Workspace** | 多租户隔离的最高边界；几乎所有资源都挂在某个 workspace 下。`X-Workspace-ID`/`X-Workspace-Slug` 头或 task-token 绑定决定当前 workspace。 |
| **Agent** | 一个 AI 智能体实体（有名字、头像、instructions、绑定 runtime/model）。在看板上是一等公民 assignee。 |
| **Runtime** | daemon 向 server 注册的"执行能力"——某台机器上某个 agent provider（claude/codex/…）的可用实例。有 online/offline 状态，靠心跳维持。 |
| **Daemon** | 跑在用户机器上的常驻进程（`multica daemon start`），注册 runtime、轮询认领 task、拉起 agent 子进程。 |
| **Task / AgentTaskQueue** | 一次 agent 执行的工作单元。状态机：`queued → dispatched → running → completed/failed/cancelled`。可绑定 issue / chat session / autopilot run。 |
| **Session resume** | 同一 (agent, issue) 多轮对话自动恢复上一次的 agent session id + 工作目录，保留上下文。 |
| **Skill** | 可复用的 agent 能力（一份 `SKILL.md` + 附属文件），按 source URL 导入到 workspace，挂到 agent 上。 |
| **Autopilot** | 按 cron / webhook / API 触发，自动 create issue 或直接 run agent 的自动化规则。 |
| **Squad** | 由一个 leader agent 领导的多成员协作单元；任务派给 squad，leader 负责委派。 |
| **Cloud Runtime** | 可选的远端 agent fleet（`mcn_` token），当本地没有所需 CLI 或需要横向扩展时由 server 代理调度。 |

---

## 技术栈一览

- **语言/版本**：Go 1.26.1
- **HTTP**：`go-chi/chi v5` + `go-chi/cors`
- **数据库**：PostgreSQL，`jackc/pgx/v5`（pgxpool）+ **sqlc** 生成类型安全查询
- **实时**：`gorilla/websocket` + 可选 `redis/go-redis/v9`（多节点 fanout）
- **鉴权**：`golang-jwt/jwt/v5`（JWT）+ 多种自定义 token 前缀（`mul_`/`mat_`/`mdt_`/`mcn_`）
- **调度**：`robfig/cron/v3` + DB 表 `sys_cron_executions` 做分布式租约
- **CLI**：`spf13/cobra`
- **可观测**：`prometheus/client_golang` + `log/slog`（`lmittmann/tint` 彩色输出）
- **云资源**：`aws-sdk-go-v2`（S3 / SecretsManager），`resend-go`（邮件）
- **ID**：`google/uuid` + `oklog/ulid`
