# 03 · 事件总线、实时 fanout 与 Daemon 通道

Multica 有**三层事件架构**，理解它们的边界是读懂整个实时系统的关键：

1. **进程内同步总线**（`internal/events`）—— 在 HTTP handler 里同步执行，保证因果顺序。
2. **Realtime Hub + Redis relay**（`internal/realtime`）—— 把事件 fanout 给浏览器/桌面端 WS 客户端，可跨节点。
3. **Daemon WS 通道**（`internal/daemonws`）—— server↔daemon 的专用双向通道，做 task 唤醒。

涉及 `pkg/protocol`（wire 类型）和 `cmd/server/*_listeners.go`（监听器注册）。

---

## 1. 进程内事件总线（`internal/events/bus.go`）

一个极简的**同步** pub/sub：

- `events.New()` → `*Bus`（`bus.go:35`）。
- `Subscribe(eventType, handler)`（`bus.go:43`）：注册类型监听器，按注册顺序**同步**调用。
- `SubscribeAll(handler)`（`bus.go:51`）：全局监听器，在类型监听器之后调用。
- `Publish(e Event)`（`bus.go:61`）：先类型监听器、后全局监听器，逐个**同步**执行；单个 handler panic 被 recover，不阻断其它 handler。

`Event` 结构（`bus.go:9`）：`Type`、`WorkspaceID`（路由到正确的 Hub room）、`ActorType`/`ActorID`、`Payload`（任意 JSON），外加可选的 `TaskID`/`ChatSessionID` scope 提示（MUL-1138 phase 1，让 fanout 层路由到比 workspace 更细的 scope，无需反序列化 payload）。

> 为什么同步？因为监听器之间有**因果依赖**——通知监听器要读 subscriber 监听器刚写入的 subscriber 行。同步派发 + 注册顺序保证这一点在单次 `Publish` 内成立。

事件类型在 `pkg/protocol/events.go`：`issue:created/updated/deleted`、`comment:created/updated`、`task:queued/dispatch/completed/failed/cancelled`、`inbox:new/read/archived`、`activity:created`、`subscriber:added/removed`、`daemon:task_available`、`daemon:runtime_profiles_changed` 等。

---

## 2. 监听器注册顺序（`cmd/server/main.go:247-259, 336`）

```
1. registerListeners(bus, broadcaster)        → 把 bus 事件桥接到 realtime fanout
2. registerSubscriberListeners(bus, queries)  → 自动订阅
3. registerActivityListeners(bus, queries)    → 写 activity_log
4. registerNotificationListeners(bus, queries)→ 生成 inbox 通知
5. registerAutopilotListeners(bus, autopilotSvc) → 同步 autopilot run 状态
```

| 监听器组 | 文件 | 触发于 | 做什么 |
|----------|------|--------|--------|
| Subscriber | `subscriber_listeners.go:16` | `issue:created/updated`、`comment:created` | 把 creator/assignee/commenter/被 @ 的人自动加为 issue subscriber；发 `subscriber:added` |
| Activity | `activity_listeners.go:19` | `issue:*`、`task:completed/failed` | 写不可变 activity_log；发 `activity:created` |
| Notification | `notification_listeners.go:544` | 多类事件 | **查 subscriber 表**确定收件人，按用户偏好生成 inbox_item + 发 `inbox:new`；处理去重与父 issue 冒泡 |
| Autopilot | `autopilot_listeners.go:15` | task 事件 | 当关联 issue 进入终态时同步 autopilot run 状态 |

**顺序铁律**：subscriber 必须先于 notification 注册——否则 notification 查 subscriber 表时还没数据。

---

## 3. Realtime Hub（面向客户端，`internal/realtime`）

### Hub 设计（`hub.go:266`）
按 **scope** 组织 WS 客户端连接（"room"）。客户端连上来（`HandleWebSocket`，`hub.go:746`）后自动订阅 workspace + user 两个 scope（`hub.go:322`）。

Scope 类型（`broadcaster.go:6`）：
- `ScopeWorkspace`(`workspace`)、`ScopeUser`(`user`) —— 已启用
- `ScopeTask`(`task`)、`ScopeChat`(`chat`) —— Phase 1 预留
- `ScopeDaemonRuntime`(`daemon_runtime`) —— 内部路由到 daemon hub

核心操作：
- `BroadcastToScope(scopeType, scopeID, msg)`（`hub.go:481`）：发给某 room 全部客户端；用 `Client.markSeen()`（`hub.go:499`）做 Redis 回环去重；慢客户端被驱逐。
- `BroadcastToWorkspace` / `SendToUser` / `Broadcast`（全员，daemon 事件用）。

**授权**：Hub 持有 `ScopeAuthorizer`（`SetAuthorizer`，`hub.go:293`，由 `main.go:253` 注入 `newScopeAuthorizer(queries)`）。客户端订阅 task/chat 这类细粒度 scope 时（`handleSubscribe`，`hub.go:935`）经它做 DB 鉴权（带缓存）。

### Redis Relay：三种模式（`REALTIME_RELAY_MODE`）
在 `cmd/server/main.go:199-243` 装配。无 Redis 时 broadcaster 就是 hub（单机）。

| 模式 | 实现 | 说明 |
|------|------|------|
| **sharded**（默认） | `ShardedStreamRelay`（`sharded_stream_relay.go:69`） | 把 (scopeType,scopeID) 用 FNV-32a 哈希到固定 shard，XADD 到 `ws:relay:shard:{0..N}`；每节点跑 N 个 `XREAD BLOCK` 消费者，本地按订阅过滤。daemon 唤醒投递器就是它自己。 |
| **dual**（灰度） | `MirroredRelay`(sharded + legacy) | 同时写两套 relay，同一 event id，客户端按 id 去重，使分歧无害——用于从 legacy 平滑迁移到 sharded。 |
| **legacy** | `RedisRelay`（`redis_relay.go:131`） | 每个活跃 scope 一个 `XREADGROUP` 消费者，scope key `ws:scope:{type}:{id}:stream`。Redis fanout 禁用 daemon 唤醒。 |

sharded 调参：`REALTIME_RELAY_SHARDS`(默认 8)、`_STREAM_MAXLEN`(10万)、`_XREAD_COUNT`(128)、`_XREAD_BLOCK`(5s)。

### DualWriteBroadcaster（`redis_relay.go:498`）
多节点模式的 broadcaster 包装：
1. **立即**投递到本地 hub（亚毫秒，快路径），并 `markSeen` 防止 Redis 回环重复。
2. **同时**publish 到 Redis relay 做跨节点 fanout。
3. 跨节点收到的事件经 event id 去重。

好处：本地用户零延迟，跨节点用户经 Redis 异步收到，且不会双投。

---

## 4. Daemon WS 通道（`internal/daemonws`）

与面向客户端的 Hub 分开，是 server↔daemon 的专用通道。

### Hub（`daemonws/hub.go:139`）
按 **runtime id** 和 **workspace** 双索引维护 daemon 连接（`byRuntime`、`byWorkspace`）。

连接生命周期（`hub.go:211`）：`HandleWebSocket` 校验 runtime id → upgrade → 建 client → 注册（按每个 runtime/workspace 入索引）→ 起 read/write pump。
- **readPump**（`hub.go:502`）：4KB 消息上限，处理 pong 刷新读 deadline。
- **writePump**（`hub.go:617`）：~54s ping ticker，10s 写超时。

入站帧处理（`hub.go:527`）：解析 `protocol.Message`，记录 kind 指标，按类型路由。目前主要处理 `daemon:heartbeat`（`hub.go:552`）——校验 runtime_id 属于该 client 的授权集合，调用心跳 handler（含副作用，不设超时），回 `daemon:heartbeat_ack`。其它类型为前向兼容静默忽略。

### Task 唤醒（best-effort 提示）
- `NotifyTaskAvailable(runtimeID, taskID)`（`hub.go:248`）：给所有 watch 该 runtime 的 client 发 `task_available` 帧，带去重；记录 hit/miss 指标。
- `NotifyRuntimeProfilesChanged(workspaceID, profileID)`（`hub.go:254`）：发给该 workspace 全部连接。

唤醒只是**提示**——daemon 收到就立刻去 REST 认领，丢了也有定时轮询兜底。真相不在 WS 上。

### 跨节点唤醒：RelayNotifier（`daemonws/notifier.go:14`）
多节点模式下，daemon 可能连在 A 节点，而 task 在 B 节点入队。`RelayNotifier` 包装本地 Hub + Redis relay publisher：
- `NotifyTaskAvailable`（`notifier.go:23`）：先调本地 hub，再 publish 到 Redis `ScopeDaemonRuntime` scope（shardKey=taskID）。
- 任何节点的 sharded relay 消费者收到 `daemon_runtime` scope 事件后，投递给本地连着的对应 daemon。

`main.go` 据 relay 模式选择 `daemonWakeup`：sharded/dual 用 `RelayNotifier`，legacy 用裸 `daemonHub`。

---

## 5. Wire Protocol（`pkg/protocol`）

### 客户端 ↔ server（realtime WS）
- 客户端→server：`{type: "auth"|"ping"|"subscribe"|"unsubscribe", payload}`，订阅帧 `{scope, id}`，task/chat scope 经 ScopeAuthorizer 鉴权。
- server→客户端：事件类型字符串 + payload + actor_id/actor_type。个人事件（inbox/invite/member:added）经 `SendToUser`，工作区事件经 `BroadcastToWorkspace`，daemon 事件经 `Broadcast`。

### daemon ↔ server（daemon WS）
关键 payload（`pkg/protocol/messages.go`）：
- `DaemonRegisterPayload`：DaemonID、AgentID、Runtimes 列表
- `DaemonHeartbeatRequestPayload`：RuntimeID、SupportsBatchImport
- `DaemonHeartbeatAckPayload`：RuntimeID、Status，及可选 pending 动作（updates / model 列表 / local skills / imports）
- `TaskProgressPayload` / `TaskCompletedPayload` / `TaskMessagePayload`：进度、完成（PR URL、output）、transcript（text/tool_use/tool_result/error）

> 注意：daemon 的 task 提交主要走 **REST**（`/api/daemon/tasks/{id}/start|progress|complete|fail|usage`），WS 主要承载心跳与唤醒。两条通道职责分明：REST 可靠传输，WS 低延迟提示。

---

## 总结

```
HTTP handler ──publish──▶ events.Bus (同步, 保证因果序)
                            │  subscriber → activity → notification → autopilot
                            ▼
                       registerListeners → broadcaster
                            │
            ┌───────────────┴───────────────┐
            ▼                               ▼
   realtime.Hub (本地 WS fanout)     Redis relay (跨节点)
            │                               │ daemon_runtime scope
            ▼                               ▼
   浏览器/桌面端                      daemonws.Hub ──task_available──▶ daemon
```
三层去重统一靠 **event id**，保证跨层不重复投递。
