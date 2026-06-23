# 04 · Agent 执行引擎：Daemon、Provider 抽象、Task 生命周期

这是 Multica 最有特色的部分：**真正拉起 agent 的代码（daemon）和云端 server 在同一个 Go module 里**，但作为不同二进制运行在用户机器上。涉及 `internal/daemon`（~40K LOC）、`pkg/agent`（~29K LOC）、`internal/cloudruntime`、`pkg/taskfailure`、`pkg/redact`。

---

## 1. Daemon 是什么

Daemon（`internal/daemon/daemon.go:142`）是跑在**用户机器**上的客户端执行运行时，通过 `multica daemon start` 启动（CLI 入口 `cmd/multica/cmd_daemon.go:31`）。一个进程做这些事：

- **注册 runtime**：向 server 上报本机有哪些 agent provider 可用（claude/codex/copilot…）+ 自定义 runtime profile，拿到持久 runtime UUID。
- **轮询认领 task**：每 ~5s（`PollInterval`）调 `ClaimTask`（REST）。
- **拉起 agent 子进程**：在隔离的 git worktree 工作目录里 spawn agent CLI。
- **流式回报**：把 agent 的输出事件经 REST/WS 实时回传 server。
- **回报用量/失败**：token 消耗（计费）、失败原因（分类）。

### 启动与主循环（`daemon.go:738` `Run`）
1. preflight：PAT 续期、workspace 同步、检测各 agent CLI 版本（`daemon.go:748-855`）
2. `workspaceSyncLoop`（`daemon.go:1610`，每 30s）：拉 workspace 列表 → 为每个 workspace 注册 runtime（`registerRuntimesForWorkspace`，`daemon.go:1067`）→ 缓存 settings/repos → 检测自定义 profile 变更并重注册
3. `heartbeatLoop`（`daemon.go:1769`）：维持 runtime online
4. 每个 runtime 一个 poller goroutine（`daemon.go:2255`）
5. 本地 health HTTP server（默认端口 19514，`/health` `/shutdown`）
6. 阻塞等 ctx 取消

### 与 server 通信（`internal/daemon/client.go:23`）
REST 客户端封装：`ListWorkspaces`、`RegisterRuntime`、`GetWorkspaceRepos`、`ClaimTask`、`ReportProgress`、`CompleteTask`、`FailTask`、`ReportTaskUsage`。另有持久 WS（`daemonws`）接收心跳 ack 与唤醒，免去纯轮询。

---

## 2. Agent Provider 抽象（`pkg/agent`）

所有 agent CLI 被统一到一个 `Backend` 接口（`pkg/agent/agent.go:15`）：

```go
type Backend interface {
    Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error)
}
```

`Execute` 返回一个 `Session`，调用方读 `Session.Messages`（流式事件）并等 `Session.Result`（最终结果，buffered 1，发一次即关）。

### 支持的 provider（`SupportedTypes` 白名单，`agent.go:144`）
13 种作为**自定义 runtime profile 合法 base**的 provider：
`claude`、`codebuddy`、`codex`、`copilot`、`opencode`、`openclaw`、`hermes`、`gemini`、`pi`、`cursor`、`kimi`、`kiro`、`antigravity`。
（`qoder` 是 `New()` 可构造的内置 provider，但**刻意不在白名单里**——不作为自定义 profile 的 base；该白名单同时是 migration 120 里 `runtime_profile.protocol_family` CHECK 约束的来源。）

各 provider 的调用方式不同，但都 spawn 子进程 + 标准 stdin/stdout 协议：

| Provider | 调用 | 协议 |
|----------|------|------|
| claude / codebuddy / gemini / cursor | `claude -p --output-format stream-json` 等 | stream-json over stdio |
| codex | `codex app-server --listen stdio://` | JSON-RPC 2.0 |
| copilot | `copilot --json`（Windows 走 PS1） | JSON mode |
| opencode / pi / openclaw | provider 专属 JSON | openclaw 支持 local/gateway 远端路由 |
| hermes / kimi / kiro | `hermes acp` 等 | ACP（Agent Communication Protocol） |

工厂：`agent.New(agentType, cfg)`（`agent.go:172`）。

### 统一事件模型（`agent.go:98`）
不论哪种 CLI，输出都被解析归一为同一个 `Message`：`Type`(text/thinking/tool-use/tool-result/status/error/log)、`Content`、`Tool`、`CallID`、`Input`、`Output`、`Status`、`SessionID`。下游（daemon 进度回报、server 事件存储）不关心是哪种 CLI 产生的。

### Claude backend 为例（`pkg/agent/claude.go:23`）
1. **spawn**（`claude.go:59`）：`exec.CommandContext` 带固定 flag（`-p --output-format stream-json --input-format stream-json --permission-mode bypassPermissions`）+ 可选 model/effort/max-turns/system-prompt/resume id + 过滤后的 custom_args + 经临时文件传入的 `--mcp-config`。
2. **stdin writer goroutine**（`claude.go:117`）：把 prompt 当 JSON 写入，避免 Claude 先打 banner 再读 stdin 造成死锁。
3. **stdout scanner**（`claude.go:148`）：逐行解析 streaming JSON：`assistant`(text/thinking/tool_use + 累计 token)、`user`(tool result)、`system`(session id/状态)、`result`(最终 output/用量/成败)、`control_request`(自动批准工具调用)。
4. **失败判定**（`claude.go:203`）：区分 `DeadlineExceeded`(超时)、`Canceled`(中止)、非零退出（附 stderr 尾部）。
5. **session resume**（`claude.go:594`）：`--resume <id>`，若返回了不同 session 则降级为全新 session。

### 模型发现与 thinking level（`pkg/agent/models.go`）
- 静态目录（claude/gemini/copilot 由厂商策展）+ 动态发现（opencode/pi/openclaw shell 出去查并缓存 60s）。
- 每个模型声明支持的 effort 级别（如 claude `low|medium|high|xhigh|max`），daemon 把用户选的级别注入命令。

---

## 3. Task 执行生命周期（daemon 侧）

```
poller 轮询 ClaimTask ──▶ 拿到 task
   │ (若 local_directory 资源) 抢路径互斥锁 → server 转 waiting_local_directory→running
   ▼
分发到 handleTask goroutine (受 task slot 信号量限流，默认并发 6)
   ▼
runTask (daemon.go:3104)
   1. 校验 workspace_id (拒绝无 workspace 的 task，防跨租户污染)
   2. 把 task 级 repo 加入 workspace allowlist
   3. 解析 provider (cfg.Agents + 自定义 profile 命令覆盖)
   4. 准备环境:
        - PriorWorkDir 有 → execenv.Reuse() 复用上次目录 (保留 agent 状态/恢复 session)
        - 否则 → execenv.Prepare() 建隔离目录 + git worktree
   5. backend.Execute(ctx, prompt, opts) → 流式读 Messages + 等 Result
   6. 合并多轮 token 用量 → reportTaskResult → CompleteTask / FailTask
```

**并发控制**：task slot 信号量（`daemon.go:2618`）带唤醒 channel，slot 释放时唤醒等待的 poller。

**取消/看门狗**：
- 取消轮询 goroutine（`daemon.go:2660`）每 5s 查 `GetTaskStatus`，server 端 task 进入终态或被删则取消 agent 的 `runCtx`，终止子进程。
- 超时：`opts.Timeout>0` 走硬墙钟超时；`==0` 走不活跃看门狗（只要 agent 还在产出事件就不杀）。
- codex 语义不活跃超时（`--codex-semantic-inactivity-timeout`）盯住"卡住但不退出"的进程。

---

## 4. 工作目录与 Git Worktree 隔离（`internal/daemon/execenv`）

每个 task 一套隔离目录，挂在 per-workspace 层级下（`ResolveWorkspacesRoot`，默认 `~/.multica/workspaces/`）：

```
~/.multica/workspaces/
├── <workspace-id>/
│   └── <task-id>/
│       ├── work/     (agent 工作目录 = git worktree)
│       ├── output/   (产物/代码变更)
│       └── logs/     (调试日志)
└── .repos/           (按 URL 哈希缓存的 bare git 仓库)
```

Git worktree（`execenv/git.go:77`）：`git fetch origin` → 从默认分支建 worktree → 分支冲突则加时间戳重试。清理（`git.go:102`，best-effort）：`git worktree remove --force` + `git branch -D`。Bare 仓库缓存在 `.repos/`（`repocache/cache.go`），task 级 repo 从缓存的 bare 仓库 checkout worktree 或 `git clone --bare`。

---

## 5. 失败分类（`pkg/taskfailure`）与脱敏（`pkg/redact`）

### 21 个标准失败 reason（`taskfailure/failure.go:54`）
分两组：
- **平台侧**（无前缀）：`queued_expired`、`runtime_offline`、`runtime_recovery`、`timeout`、`iteration_limit`、`agent_blocked`、`api_invalid_request`。
- **agent 侧**（`agent_error.*`）：`provider_auth_or_access`(401/403)、`provider_quota_limit`(402)、`provider_capacity_or_rate_limit`(429/529)、`provider_server_error`(5xx)、`provider_network`、`process_failure`、`empty_or_unparseable_output`、`agent_timeout`、`context_overflow`、`missing_config`、`model_not_found_or_unavailable`、`runtime_version_unsupported`、`runtime_missing_executable`、`unknown`(兜底)。

`Classify(rawError)`（`classify.go:43`）用子串/正则把自由错误文本映射到 reason。daemon 在 `FailTask` 时调用它（`daemon.go:2800`），结果存 `agent_task_queue.failure_reason`，server 据此决定是否自动重试（仅基础设施类失败，见 [05](05-services-and-scheduling.md)）。

### 密钥脱敏（`redact/redact.go`）
在 agent 输出落库或经 WS 广播前，`Text()`（`redact.go:85`）按规则清洗：AWS key、PEM 私钥、GitHub token(`ghp_`/`gho_`)、OpenAI/Anthropic key(`sk-`)、Slack token、GitLab PAT、JWT、Bearer、含密码的 DB 连接串、通用 `API_KEY=`/`SECRET=`/`PASSWORD=`、家目录用户名（masked 为 `****`）。工具输入经 `InputMap()` 一并脱敏。

---

## 6. Cloud Runtime（`internal/cloudruntime/client.go`）

可选的远端 agent fleet HTTP 客户端。当**本地 daemon 没有所需 CLI**、task 指定云端执行（如 GPU 负载）、或需横向扩展时，server 把 task 代理给云端 fleet 而非本地 daemon。

- 配置（`client.go:32`）：`BaseURL`(fleet 端点)、`HTTPClient`、`Recorder`(指标回调)。
- 请求带 user id + request id 头做审计；按 path/method 推断 operation（provision/terminate/status/gateway/billing…）用于打点；响应按 ok/4xx/5xx/timeout 分桶。
- fleet 收到同样的 `Task` 结构，需返回 `TaskResult`（output、session id、token 用量）——与本地 daemon 在 server 看来接口一致。

cloud 节点以 `mcn_` token 鉴权（见 [02](02-http-api.md)），并受 `RequireHumanActor` 等横向移动防护约束。
