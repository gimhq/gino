# 05 · 服务层、后台 Worker 与调度

涵盖 `internal/service`（业务服务）、`cmd/server` 里的后台 worker、`internal/scheduler`（DB 分布式 cron）以及若干辅助服务。

---

## 1. TaskService（`internal/service/task.go`）

整个 task 生命周期的所有者：创建、派发、状态机、session resume、daemon 唤醒。

### 入队（Enqueue）
- `EnqueueTaskForIssue`（`task.go:432`）：为 agent-assigned issue 建 queued task，广播 `EventTaskQueued`，经 `NotifyTaskEnqueued` 唤醒 daemon。
- `EnqueueTaskForMention`（`task.go:499`）/ `EnqueueTaskForSquadLeader`（`task.go:509`，带 `is_leader_task=true`）：为被 @ 的 agent / squad leader 入队。
- `EnqueueChatTask`（`task.go:710`）：chat session 任务，返回 `ErrChatTaskAgentArchived`/`ErrChatTaskAgentNoRuntime` 等可产品化的哨兵错误。
- `EnqueueQuickCreateTask`（`task.go:605`）：自然语言 prompt 任务（context JSONB），squad 有效时落到 leader。

### 认领与派发
- `ClaimTask`（`task.go:937`）：原子认领下一个 queued task，尊重 agent 的 `max_concurrent_tasks`；算等待时长指标，广播 `EventTaskDispatch`。
- `ClaimTaskForRuntime`（`task.go:1009`）：按 runtime 认领同时尊重 per-agent 并发；含空认领快路径与 stale-dispatched 回收；用版本化失效关闭"空缓存"竞态。

### 状态机
`queued → dispatched → running → completed/failed/cancelled`：
- `StartTask`（`task.go:1139`）：dispatched→running，广播 `EventTaskRunning`。
- `MarkTaskWaitingLocalDirectory`（`task.go:1163`）：等待资源锁时驻留。
- `CompleteTask`（`task.go:1190`）：标记完成；**持久化 agent session/work-dir resume 指针**（用于下次恢复）；无 output 时合成兜底 agent 评论；quick-create 通知发起人；chat 广播 assistant 消息；对账 agent 状态。
- `FailTask`（`task.go:1373`）：标记失败 + 可选 session/work-dir resume；经 `taskfailure.Classify` 分类；触发 `MaybeRetryFailedTask`。

### 重试与失败处理
- `MaybeRetryFailedTask`（`task.go:1537`）：仅对**基础设施类**失败（runtime_offline / timeout / runtime_recovery / codex_semantic_inactivity）且 `attempt < max_attempts` 且 task 绑定 issue/chat（非 autopilot）时自动重试。
- `HandleFailedTasks`（`task.go:1719`）：批量副作用——重试、指标、无活跃 task 时把 issue 重置为 `todo`。

### Session resume 与 daemon 唤醒
- `notifyTaskAvailable`（`task.go:1957`）：入队后先 bump 空认领缓存版本，**再**经 `Wakeup.NotifyTaskAvailable` 唤醒 daemon——顺序关键，bump 在前关闭竞态。
- `CompleteTask`/`FailTask` 把 session_id/runtime_id 落到 chat_session，下次认领即恢复，除非失败原因属于 resume-unsafe（如 `api_invalid_request` 会污染 session）。

`Wakeup` 是 `TaskWakeupNotifier` 接口（`task.go:47`），由 `daemonws.Hub`（单机）或 `daemonws.RelayNotifier`（多节点）实现。

---

## 2. AutopilotService（`internal/service/autopilot.go`）

Autopilot = 按 cron/webhook/API 触发自动开工的规则。

- `DispatchAutopilot`（`autopilot.go:59`）：核心入口。
  - 预入场检查 `shouldSkipDispatch`（`autopilot.go:66`）：assignee（agent 或 squad leader）离线/归档则记 `skipped` run，不入队注定失败的 task。
  - 按 `execution_mode` 分流：`create_issue` 建 issue 再走既有 listener 链入队（`autopilot.go:92`）；`run_only` 直接入队 agent task（`autopilot.go:101`）。
  - 更新 `last_run_at`，发 `EventAutopilotRunStart`。
- `dispatchCreateIssue`（`autopilot.go:145`）：事务内建 issue，模板插值标题/描述（带触发时区），事务内 fan out 默认 subscriber，再 `EnqueueTaskForIssue`/`EnqueueTaskForSquadLeader`。**squad autopilot 的 creator 是解析出的 leader**（而非配置者），让活动以正确 agent 身份渲染。
- `dispatchRunOnly`（`autopilot.go:393`）：直接入队无 issue 的 agent task，含 belt-and-braces 二次校验（leader 可能在入场与派发之间变化）。
- `SyncRunFromIssue`（`autopilot.go:461`）：issue 进入终态（done/in_review/cancelled/blocked）时把 run 更新为 completed/failed 并记指标。

---

## 3. 后台 Worker（`cmd/server`）

### Runtime Sweeper（`runtime_sweeper.go`）
`runRuntimeSweeper`（`runtime_sweeper.go:76`）每 30s tick：
- `sweepStaleRuntimes`（`:95`）：加载 `last_seen_at` 超阈值的候选，**先查 LivenessStore**（Redis 心跳还活着就跳过，即便 DB 窗口过期）；幸存者标记 offline，fail 其孤儿 task（dispatched/running），清 liveness 记录。
- `sweepStaleTasks`（`:246`）：即便 runtime 在线，dispatched>300s 或 running>9000s 的 task 也按 `timeout` fail。
- `gcRuntimes`（`:212`）：删除 7 天前、无活跃 agent 的 offline runtime。
- `sweepExpiredQueuedTasks`：>2 小时的 queued task 按 500 一批过期。

> LivenessStore（`handler.LivenessStore`）在 `main.go:342-345` 装配，Redis 有则 Redis 实现否则 Noop。**心跳写入路径与 sweeper 读取路径必须用同一个 store**，否则在线 runtime 被误判离线。

### BatchedHeartbeatScheduler（`handler/heartbeat_scheduler.go`）
合并 runtime 心跳的 `last_seen_at` 写入，降低 DB 压力。`Run`（`main.go:349`）后台跑，关停前 `Stop`（`main.go:426`）flush 最后一批——且必须在 HTTP 排空之后，否则迟到心跳无法 flush。

### Autopilot Scheduler（`autopilot_scheduler.go`）
`runAutopilotScheduler`（`:17`）每 30s：
- `recoverLostTriggers`（`:36`）：启动时恢复 `next_run_at` 为 NULL（认领后崩溃）的触发器，按 cron 重算。
- `tickScheduledAutopilots`（`:72`）：`ClaimDueScheduleTriggers` → 加载 autopilot → `DispatchAutopilot` → `AdvanceTriggerNextRun`。

### Autopilot Failure Monitor（`autopilot_failure_monitor.go`）
`runAutopilotFailureMonitor`（`:71`）：7 天窗口内 ≥50 次运行且失败率 ≥90%（均可经 env 配）的 autopilot 自动 `SystemPauseAutopilot`，发 `EventAutopilotUpdated` 并经 inbox 通知 creator。

---

## 4. DB 分布式 Cron（`internal/scheduler`）

把 `sys_cron_executions` 表当作**分布式租约 + 审计日志**，跑内部周期任务，替代运维手挂的 `pg_cron`。

### Manager（`scheduler/manager.go`）
- `Manager.Run`（`:95`）：30s tick 的事件循环，首 tick 立即执行。
- `runOnce`（`:122`）：读 DB 时间，对每个 job 取 scope，把过期 RUNNING 租约标记 FAILED（关闭弃置租约、保留审计）。
- `processPlan`（`:266`）：单个 (job, scope, plan_time) 尝试——`tryClaim`（advisory lock + INSERT ON CONFLICT 分支）→ 赢得租约或抢占 stale runner 则跑 handler → `tryTerminate` 标记终态。

### Task Usage Hourly Job（`scheduler/jobs_task_usage.go`）
`TaskUsageHourlyJob`（`:36`），在 `main.go:379` 注册：
- cadence 5m、schedule_delay 5m、catch-up = latest_only。
- handler 调 `rollup_task_usage_hourly()` SQL 函数（`:72`），该函数内部持有 **advisory lock 4246**，与遗留 pg_cron 并发安全。
- 读前后 watermark 报告 rows_affected；短运行时心跳保持 stale_after 新鲜。

---

## 5. 其它服务与 Worker

| 模块 | 文件 | 作用 |
|------|------|------|
| **issueguard** | `issueguard/duplicate.go:43` | 防重复建 issue：按 (workspace, project, parent, 归一化 title) 加 session 级 advisory lock 后查活跃重复；title 归一化（小写、单空格）。返回 `ActiveDuplicateError` 哨兵。 |
| **issueposition** | `issueposition/position.go:17` | 手动排序：`NextTopPosition` 取该状态桶 `MIN(position)-1`，新 issue 永远排最前。 |
| **taskusagebackfill** | `taskusagebackfill/backfill.go:131` | 从历史 `task_usage` 回填 `task_usage_hourly`，advisory lock 4246 下按月切片走、幂等可恢复；被 migration 103 前置 hook 与独立 `cmd/backfill_task_usage_hourly` 使用。 |
| **analytics** | `analytics/events.go` | 埋点：高频 runtime 遥测只进 Prometheus（`metrics_only_events`），用户行为进 PostHog；`CoreProperties` 带 workspace/user/agent/source/runtime_mode/provider 维度。 |

---

## 集成关系图

```
HTTP handler / autopilot scheduler
        │ Enqueue*
        ▼
   TaskService ──广播──▶ events.Bus ──▶ activity/notification/autopilot listeners
        │ NotifyTaskAvailable
        ▼
   TaskWakeupNotifier (daemonws.Hub | RelayNotifier)
        │ WS 唤醒
        ▼
      daemon ──REST 认领──▶ ClaimTaskForRuntime ──▶ running ──▶ CompleteTask/FailTask
                                                                    │
        runtimeSweeper ◀──LivenessStore(Redis)──── 心跳               ▼ MaybeRetryFailedTask
        schedulerMgr ──sys_cron_executions(advisory lock)──▶ rollup_task_usage_hourly
```
