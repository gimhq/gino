# 01 · 系统总览与进程模型

本文档说明 `server/` 目录里到底有哪些可执行程序、API Server 如何启动、启动后挂了哪些后台 goroutine、以及单机与多节点两种部署拓扑的差异。

---

## 1. 目录结构与可执行程序

```
server/
├── cmd/
│   ├── server/          # ★ 云端 API Server 主二进制（main.go + router.go + listeners + sweeper）
│   ├── multica/         # ★ multica CLI（内嵌 daemon）—— 24 个 cmd_*.go cobra 命令
│   ├── migrate/         # 数据库迁移工具（advisory-lock 串行化）
│   ├── backfill_task_usage_hourly/   # 历史用量回填
│   └── backfill_codex_usage_cache/   # codex 用量缓存回填
├── internal/            # 不对外导出的实现
│   ├── handler/         # ★ HTTP handler（143 文件，~76K LOC，API 主体）
│   ├── daemon/          # ★ 本地 daemon 执行器（75 文件，~40K LOC，含 execenv/ 子包）
│   ├── service/         # 业务服务层（TaskService、AutopilotService、EmailService…）
│   ├── realtime/        # WebSocket Hub + Redis relay（实时 fanout）
│   ├── daemonws/        # daemon 专用 WS Hub（server↔daemon 通道）
│   ├── events/          # 进程内同步事件总线
│   ├── integrations/    # 外部集成（lark/ 占绝大部分，~20K LOC）
│   ├── middleware/      # 鉴权 / CORS / 限流 / 日志 / CSP 中间件
│   ├── auth/            # JWT、PAT/daemon-token 缓存、cloud PAT 校验
│   ├── scheduler/       # DB 分布式 cron（sys_cron_executions）
│   ├── metrics/         # Prometheus registry（HTTP + business 指标）
│   ├── storage/         # S3 / 本地文件对象存储
│   ├── analytics/       # PostHog + Prometheus 事件埋点
│   ├── cli/             # CLI 与 server 共享的输出/解析逻辑
│   └── …（cloudruntime, agenttmpl, skill, issueguard, issueposition, taskusagebackfill, realtime…）
├── pkg/                 # 可被外部复用的库
│   ├── agent/           # ★ agent provider 抽象（62 文件，~29K LOC，13+ 种 CLI 后端）
│   ├── db/              # sqlc 生成代码 + queries（38 文件，~19K LOC）
│   ├── protocol/        # server↔client / server↔daemon 的 wire 类型
│   ├── taskfailure/     # 任务失败分类（21 个标准 reason）
│   └── redact/          # 密钥脱敏
├── migrations/          # 159 对 .up/.down SQL（最新编号 122）
└── sqlc.yaml            # sqlc 配置
```

★ = 体量最大、最核心的子系统。

---

## 2. API Server 启动序列

入口 `cmd/server/main.go:121`（`func main`）。启动是一条精心排序的流水线，顺序本身承载了正确性约束：

1. **日志与配置告警**（`main.go:122-137`）：`logger.Init()`；检查 `JWT_SECRET`、邮件后端、`MULTICA_DEV_VERIFICATION_CODE` 等关键配置缺失并 `slog.Warn`。
2. **连接 Postgres**（`main.go:144-163`）：`newDBPool` 建 pgxpool，`Ping` 校验。默认连接串 `postgres://multica:multica@localhost:5432/multica?sslmode=disable`。
3. **核心总线/Hub**（`main.go:165-169`）：
   - `bus := events.New()` —— 进程内同步事件总线
   - `hub := realtime.NewHub(); go hub.Run()` —— 面向浏览器/桌面端的 WS fanout
   - `daemonHub := daemonws.NewHub()` —— 面向 daemon 的 WS 通道
4. **Redis relay 装配**（`main.go:171-246`）：仅当设置 `REDIS_URL`。决定 `broadcaster` 与 `daemonWakeup` 的具体实现（详见 [03](03-realtime-and-events.md)）。无 Redis 时退化为单机内存 hub。
5. **注册事件监听器**（`main.go:247-259`）——**顺序关键**：
   ```
   registerListeners            (broadcaster：把 bus 事件转发到 realtime)
   registerSubscriberListeners  (自动订阅 issue 的 creator/assignee/commenter)
   registerActivityListeners    (写 activity_log)
   registerNotificationListeners(查 subscriber 表 → 生成 inbox 通知)
   ```
   注释明确：**subscriber 必须先于 notification 注册**——notification 监听器要查 subscriber 表确定收件人，而它们在同一次同步派发里执行。
6. **指标**（`main.go:261-308`）：若启用，构造独立的 sampler pgxpool（防止慢抓取饿死业务流量），建 `metrics.NewRegistry`，单开一个 metrics HTTP server（建议 loopback-only）。
7. **心跳调度器**（`main.go:314`）：`handler.NewBatchedHeartbeatScheduler` 在 router 之前构造，注入 handler。
8. **构造 router**（`main.go:316-322`）：`NewRouterWithOptions` 返回 `(chi.Router, *handler.Handler)`，后者用于优雅关停时驱动 LarkHub 等。
9. **启动后台 worker**（`main.go:330-385`，见下一节）。
10. **监听 HTTP**（`main.go:396-402`）+ 等待 `SIGINT/SIGTERM`。
11. **优雅关停**（`main.go:408-451`）——**顺序关键**：先 `autopilotCancel`，再 `srv.Shutdown`（排空在途 HTTP，让心跳 handler 跑完），**然后**才 `sweepCancel` + `heartbeatScheduler.Stop()`（flush 最后一批心跳），再 join LarkHub supervisor、关停 metrics server。

---

## 3. 后台 worker 全清单

API Server 进程内常驻的 goroutine（均在 `cmd/server/main.go` 启动）：

| Worker | 启动位置 | 周期 | 职责 |
|--------|----------|------|------|
| `hub.Run` | `main.go:167` | 常驻 | realtime WS 事件循环 |
| `runRuntimeSweeper` | `main.go:348` | 30s | 把超时未心跳的 runtime 标记 offline，fail 其孤儿 task；GC 7 天前的 offline runtime；过期 queued task |
| `heartbeatScheduler.Run` | `main.go:349` | 批量 | 合并 runtime 心跳的 `last_seen_at` 写入，降低 DB 压力 |
| `runAutopilotScheduler` | `main.go:350` | 30s | 认领到期的 schedule 触发器，`DispatchAutopilot`，推进 `next_run_at` |
| `runAutopilotFailureMonitor` | `main.go:351` | 周期 | 7 天窗口内失败率 ≥90% 且 ≥50 次的 autopilot 自动暂停并通知 |
| `runDBStatsLogger` | `main.go:352` | 周期 | 周期性记录 pgxpool 统计 |
| `LarkHub.Run` | `main.go:361` | 常驻 | Lark 入站事件 supervisor（仅当配置了 Lark master key），见 [06](06-integrations.md) |
| `schedulerMgr.Run` | `main.go:382` | 30s tick | DB 分布式 cron，跑 `rollup_task_usage_hourly`（advisory lock 4246） |
| metrics server | `main.go:388` | 常驻 | 独立 `/metrics` HTTP 端口 |

`taskSvc`（`service.NewTaskService`）与 `autopilotSvc` 在 `main.go:332-336` 构造，被 sweeper / scheduler / handler 共享。

---

## 4. 部署拓扑：单机 vs 多节点

整个后端可在两种模式运行，分水岭是 **`REDIS_URL` 是否设置**：

### 单机模式（无 Redis）
- `realtime.Hub` 是唯一的事件广播者，事件不跨进程。
- 各类共享队列（task/update/model/skill 请求）、PAT/membership 缓存、限流器、liveness store 全是**进程内内存实现**，限流退化为 no-op。
- 适合自托管单实例、本地开发。

### 多节点模式（设置 REDIS_URL）
- 事件经 Redis relay 在多个 API 节点间互相投递（`DualWriteBroadcaster`：本地立即 fanout + Redis 跨节点）。
- daemon task 唤醒经 Redis `daemon_runtime` scope 跨节点路由（`daemonws.RelayNotifier`），保证连在 A 节点的 daemon 能收到 B 节点入队的 task。
- PAT/daemon-token/membership 缓存、限流器、liveness store 切换为 **Redis 实现**，多节点共享。
- `runtimeSweeper` 与心跳写入路径必须用**同一个** liveness store（main.go 注释强调），否则在线 runtime 会被误判离线。
- realtime relay 有三种模式可选（`REALTIME_RELAY_MODE`：`sharded` 默认 / `dual` 灰度 / `legacy`），详见 [03](03-realtime-and-events.md)。

> 关键不变量：**业务正确性不依赖 Redis**——Redis 只负责跨节点 fanout 与缓存。真相永远在 Postgres，task 认领通过行级状态机 + advisory lock 保证幂等。

---

## 5. 关键环境变量

| 变量 | 作用 |
|------|------|
| `DATABASE_URL` | Postgres 连接串 |
| `REDIS_URL` | 启用多节点 fanout / 缓存 / 限流；不设则单机内存模式 |
| `PORT` | API 监听端口（默认 8080） |
| `JWT_SECRET` | JWT 签名密钥（不设则用不安全默认值并告警） |
| `REALTIME_RELAY_MODE` | `sharded`(默认)/`dual`/`legacy` |
| `REALTIME_RELAY_SHARDS` / `_STREAM_MAXLEN` / `_XREAD_COUNT` / `_XREAD_BLOCK` | sharded relay 调参 |
| `RESEND_API_KEY` / `SMTP_HOST`(及 `SMTP_*`) | 邮件后端（二选一，都不设则验证码打日志） |
| `MULTICA_DEV_VERIFICATION_CODE` | 本地开发固定验证码（`APP_ENV=production` 时忽略） |
| `S3_BUCKET` / `S3_REGION` / `AWS_*` / `CLOUDFRONT_DOMAIN` / `AWS_ENDPOINT_URL` | 对象存储（不设则本地文件） |
| `GITHUB_APP_SLUG` / `GITHUB_WEBHOOK_SECRET` | GitHub App 集成 |
| `MULTICA_LARK_SECRET_KEY`（master key） | Lark 集成；不设则 LarkHub 为 nil，零开销 |
| `RATE_LIMIT_TRUSTED_PROXIES` | 限流时信任的代理 CIDR（取 X-Forwarded-For 真实 IP） |
| `LOG_LEVEL` | debug/info/warn/error（默认 debug） |
| `APP_ENV` | `production` 时收紧若干开发后门 |

---

## 6. 这套架构的设计取舍（要点）

- **执行下沉到用户机器**：agent 进程、git 仓库、API key 都在 daemon 侧，server 只编排。信任模型 + 合规友好。
- **REST 轮询 + WS 唤醒双通道**：daemon 用 REST 认领 task（可靠、无状态、易重试），WS 仅做"有新 task 了"的低延迟唤醒（best-effort，丢了也有轮询兜底）。
- **同步事件总线 + 异步 fanout 分层**：`events.Bus` 在 HTTP handler 同步执行（保证 subscriber→notification 的因果顺序），realtime fanout 才是跨进程异步层。
- **Postgres 为唯一真相源**：分布式锁（advisory lock）、租约（`sys_cron_executions`、`lark_installation.ws_lease_token`）都建在 DB 上，Redis 只是加速层，可随时丢弃重建。
- **优雅降级**：缺哪个外部依赖就关哪个功能（Lark 未配置→不起 goroutine；S3 未配置→落本地盘；Redis 未配置→单机），而不是启动崩溃。
