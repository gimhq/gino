# 08 · 可观测性与 multica CLI

涵盖 `internal/metrics`（Prometheus）、`internal/logger`（slog）、以及 `cmd/multica` 的命令全景。

---

## 1. Prometheus 指标（`internal/metrics`）

### Registry（`registry.go:39`）
`NewRegistry(opts)` 组装一个 `prometheus.Registry`，挂载：
1. `GoCollector` / `ProcessCollector`（stdlib）
2. `DBCollector`（pgxpool 统计，有 pool 时）
3. `RealtimeCollector` / `DaemonWSCollector`（实时与 daemon WS 指标）
4. `BusinessSamplerCollector`（opt-in，抓取时跑 SQL）
5. `multica_build_info{version, commit}` gauge

指标通过独立的 metrics HTTP server 暴露（`main.go:298`），建议 loopback-only（非环回地址会告警）。为防慢抓取饿死业务流量，sampler 用**独立的小 pgxpool**（`main.go:266-308`）。

### HTTP 指标（`http.go`）
- `multica_http_requests_total{method,route,status}`
- `multica_http_request_duration_seconds{...}`（5ms–10s 桶）
- `multica_http_in_flight_requests`

route 标签用 `chi.RouteContext` 取匹配模式（如 `/api/issues/{id}`），未匹配为 `unmatched`，并排除健康探针路径。

### 业务指标（`business.go`）
**Task 生命周期计数**：`multica_agent_task_enqueued/dispatched/started/terminal/failed_total`（标签 source、runtime_mode、provider、terminal_status、failure_reason）。
**Task 时延直方图**（1s–7200s 桶）：`multica_agent_task_queue_wait_seconds`、`_run_seconds`、`_total_seconds`、`_iteration_count`。
**LLM 用量/成本**：`multica_llm_tokens_total{provider,model,token_type,runtime_mode,source}`、`multica_llm_cost_usd_total`（按价目表估算）、`multica_llm_unpriced_tokens_total`（未知别名）、`multica_llm_request_total`。
**租约/队列**：`multica_task_queued_expired_total`、`multica_task_lease_expired_total`。
**在途 gauge**：`multica_agent_task_in_progress{source,runtime_mode}`。

归一化维度：`source`(issue/chat/autopilot/autopilot_issue/quick_create/other)、`runtime_mode`(local/cloud/unknown)、`terminal_status`(completed/failed/cancelled)、`failure_reason`(来自 `taskfailure.AllReasons()`)、`token_type`(input/output/cache_read/cache_write)。

### Business Sampler（`business_sampler.go`）
可选 collector，在 Prometheus **抓取时**直接对 DB 跑只读查询采样业务度量（workspace/user/issue/agent 计数等），作为内存事件计数器的替代——无抓取时不持续采集。

---

## 2. 日志（`internal/logger/logger.go`）

`log/slog` + `lmittmann/tint`（TTY 感知彩色）：
- `Init()`：全局 logger，stderr 输出，按 TTY 决定是否上色（daemon.log 文件保持干净）。
- `NewLogger(component)`：CLI 二进制的命名 logger。
- 级别从 `LOG_LEVEL`（默认 debug）；时间格式 `HH:MM:SS.000`。
- `RequestAttrs(r)`：从请求抽取观测维度（request_id、user_id、client_platform/version/os）注入 handler 日志。

---

## 3. `multica` CLI（`cmd/multica`）

cobra 根命令（`main.go:24`），构建期注入 version/commit/date，支持 `MULTICA_SERVER_URL`/`MULTICA_WORKSPACE_ID`/`MULTICA_DEBUG` 环境变量与 `--server-url`/`--workspace-id`/`--profile`/`--debug` 持久 flag。命令分三组（`help.go`）：

### CORE COMMANDS
| 命令 | 文件 | 能做什么 |
|------|------|----------|
| `issue` | `cmd_issue.go`(+label/metadata) | list/get/create/update/close/reopen、comment、label、metadata。文本输入支持 inline / `--*-stdin` / `--*-file`（Windows UTF-8 旁路） |
| `project` | `cmd_project.go` | list/get/create/update |
| `label` | `cmd_label.go` | CRUD |
| `agent` | `cmd_agent.go` | list/get/create/update/archive/restore/tasks/avatar |
| `autopilot` | `cmd_autopilot.go` | list/get/create/update/pause/resume/run |
| `workspace` | `cmd_workspace.go` | list/get/create/update/invite |
| `repo` | `cmd_repo.go` | list/add/remove（GitHub/Gitea…） |
| `skill` | `cmd_skill.go` | list/get/create/update |
| `squad` | `cmd_squad.go` | list/get/create/update、member add/remove |

### RUNTIME COMMANDS
| 命令 | 文件 | 能做什么 |
|------|------|----------|
| `daemon` | `cmd_daemon.go`(+unix/windows) | **start/stop/status/logs**——拉起/管理本地执行 daemon（见 [04](04-agent-execution.md)） |
| `runtime` / `runtime-profile` | `cmd_runtime.go` / `cmd_runtime_profile.go` | list/get/register；自定义 profile CRUD（protocol_family、command_name、fixed_args） |

### ADDITIONAL COMMANDS
| 命令 | 文件 | 能做什么 |
|------|------|----------|
| `login` / `auth` / `user` | `cmd_login.go` / `cmd_auth.go` / `cmd_user.go` | OAuth/邮件登录、PAT 管理、用户资料、切换 workspace |
| `setup` / `config` | `cmd_setup.go` / `cmd_config.go` | 交互式 onboarding 向导、config show/set |
| `attachment` | `cmd_attachment.go` | upload/download |
| `update` / `version` | `cmd_update.go` | 自更新、版本信息 |

辅助：`help.go`（gh 风格分组帮助、`exactArgs` 校验、`errSilent` 抑制重复错误）、`cmd_id_resolver.go`（短 ID/名字 → UUID 模糊解析）。

---

## 4. 该后端的整体特征总结

- **多租户 SaaS + 自托管双形态**：Postgres 为唯一真相源，Redis/S3/邮件/Lark 等外部依赖均可选、缺失即优雅降级。
- **Agent 是一等公民**：schema 用 `*_type`+`*_id` 多态贯穿 assignee/actor/creator；task/runtime/session 围绕 agent 设计。
- **执行下沉 + 双通道**：云端编排、本地执行；REST 可靠认领 + WS 低延迟唤醒，真相不依赖 WS。
- **强可观测**：task 全生命周期 + LLM 用量/成本指标，失败有 21 类标准 reason 驱动重试与告警。
- **安全纵深**：5 类 token + actor 来源盖章 + 横向移动防护 + 输出脱敏 + 凭证 AES-256-GCM 静态加密。
- **分布式协调建在 DB 上**：advisory lock + 租约表（`sys_cron_executions`、`lark_installation.ws_lease_token`），无需额外协调组件。

---

*本系列文档（README + 01–08）覆盖了 `/srv/gimhq/multica/server` 的全部主要子系统。每个论断均可在对应 `file:line` 处验证；如代码演进，以源码为准。*
