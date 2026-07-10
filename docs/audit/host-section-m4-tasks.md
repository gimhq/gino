# Host Audit — M4 Task Management & Dispatch (`src/tasks/`)

Scope: `src/tasks/index.ts`, `src/tasks/store.ts`, `src/tasks/orchestrator.ts`,
`src/tasks/dispatch.ts`, `src/tasks/lifecycle.ts`, audited against
`docs/host-architecture.md` (§M4, §3, §8) and `docs/protocol.md` (§4–§6).
Context read-only: `src/edge/admin/tasks.ts`, `src/protocol/outbox.ts`,
`src/protocol/inbound.ts`, `src/protocol/envelope.ts`, `src/agents/registry.ts`,
`src/db/schema.ts`, `e2e/README.md`, `docs/audit/agent-audit-2026-07.md`.

Focus-question answers with no standalone finding:

- **msg_ack cursor skip/double-advance:** there is no cursor to skip. Delivery
  acking is per-row (`outbox.acked_at` via `ackMessage`, `src/protocol/outbox.ts:83`);
  `msgId` equals the unique outbox row id, so a duplicate `msg_ack` is an
  idempotent timestamp rewrite and an unknown `msgId` is a no-op. Reconnect
  replay (`replayUnacked`) resends every unacked frame in id order; host-side
  this is per-contract at-least-once. The duplicate-execution risk on replay is
  the agent-side missing inbound dedupe (agent audit P0-3), not a host cursor
  bug. The *documented* per-task cursor, however, does not exist — see the P2
  below.
- **Retry semantics cross-check (agent audit P1-5, `max_attempts=1`):** the host
  neither carries a retry budget in any frame (none exists in protocol §5) nor
  reacts to a `failed` `thread_result` (M5 mirrors it; nothing in M4 re-sends
  the message). That is consistent with the architecture (agent owns execution,
  host never on the critical path), but it means the agent's broken retry
  budget is silent end-to-end: a retryable failure is terminal on the agent and
  invisible to M4 except as a mirrored `thread_runs.status='failed'` row.
- **Placement vs. offline race:** route handlers are synchronous over a
  single-threaded SQLite connection, so rank→pin cannot interleave with a
  status change inside one request. Placing on an `offline`/`enrolled` agent is
  by design (`onlineRank` is only a tie-breaker; the outbox replays on
  reconnect). No finding.

---

### [P0] Per-task `close` emits the runner-level exit frame, killing every other task on the agent

- Location: src/tasks/orchestrator.ts:203 (`emitClose`); src/edge/admin/tasks.ts:272 (close route)
- Description: protocol §5 defines `close` as agent-level "wind down and exit"
  and §4.1 lists it as a pure control frame that carries no correlation ids.
  `emitClose` nevertheless enqueues `close` with a `taskId` as if it were
  task-scoped, and the `/tasks/:id/close` route treats it as a per-task
  wind-down. Receivers must ignore unknown fields (§4.2), so the agent ignores
  the `taskId` and performs the only defined semantics: full daemon shutdown.
  Confirmed observed behavior in e2e KNOWN ISSUE 2 ("close received; shutting
  down" → agent process exits).
- Failure scenario: agent hosts tasks A and B; A is mid-turn. Admin calls
  `POST /admin/tasks/B/close`. The agent daemon exits: A's in-flight turn is
  killed, A's and every other task's crons stop firing, and nothing restarts
  the agent (host never auto-launches on close). One task's close destroys all
  work placed on the agent — a protocol contract break with cross-task work
  loss.
- Suggested fix direction: stop emitting the wire `close` from the per-task
  route (mark the task closed and optionally `interrupt`), or extend the
  protocol with a real task-scoped close frame the agent implements; reserve
  the existing `close` for agent decommission (e.g. remove/stop control flow).

### [P1] Cron created before the first message is silently lost — task never seeded (e2e KNOWN ISSUE 3, mandatory check — confirmed)

- Location: src/tasks/orchestrator.ts:150 (seed gate exists only in `sendTaskMessage`); src/tasks/orchestrator.ts:173-184 (`emitCronUpsert` emits without any seed check); src/edge/admin/tasks.ts:305 and src/edge/admin/tasks.ts:322 (cron create/update routes call `emitCronUpsert` directly)
- Description: `task_upsert` is seeded lazily — `hasSeededTask`
  (src/tasks/orchestrator.ts:44-51) is consulted only inside `sendTaskMessage`.
  The cron routes bypass that path entirely, so `POST /admin/tasks/:id/crons`
  on a placed-but-never-messaged task enqueues `cron_upsert` for a task the
  agent has never heard of. Agent-side the `crons` row FK-references the local
  `tasks` row; the insert fails and — per the agent audit (P2-19) — the failure
  is swallowed and the frame is marked processed, so replay never repairs it.
  The host returns 201 and mints a `cronId` that now addresses nothing.
- Failure scenario: create task → auto-dispatch places it → immediately
  `POST /tasks/:id/crons {schedule…}` → 201 with a `cronId`; the agent drops
  the `cron_upsert` on FK failure; the cron never applies, never fires, and
  never appears in the `crons` mirror. No error surfaces anywhere; the
  scheduled work silently never happens.
- Suggested fix direction: apply the same seed-once gate to every task-scoped
  command — e.g. extract the `hasSeededTask ? … : emitTaskUpsert` step from
  `sendTaskMessage` and run it in `emitCronUpsert` (and arguably
  `emitConfigUpdate`) so the first cron seeds the task exactly like the first
  message does.

### [P1] `interrupt` is not task-scoped — it cancels whatever turn is currently running, and stale interrupts replay on reconnect

- Location: src/tasks/orchestrator.ts:197-200 (`emitInterrupt`); src/edge/admin/tasks.ts:255 (interrupt route)
- Description: protocol §4.1/§5 define `interrupt` as a pure control frame with
  no correlation ids: "graceful cancel of the current turn". `emitInterrupt`
  attaches a `taskId` and its docstring claims the frame "is scoped to this
  task", but by its own admission the agent "ignores ids it does not need" —
  i.e. the scoping is fictional. The agent executes turns serially across all
  tasks, so the frame cancels whichever turn happens to be running.
  Additionally, because `interrupt` rides the durable outbox, an interrupt
  enqueued while the agent is offline is replayed on the next reconnect and
  cancels an arbitrary future turn.
- Failure scenario: task A is mid-turn; admin interrupts idle task B → A's turn
  is cancelled instead. Or: admin interrupts while the agent is offline; hours
  later the agent reconnects, the replayed `interrupt` fires and kills an
  unrelated cron turn.
- Suggested fix direction: make interrupt genuinely task-scoped in the protocol
  (agent honors `taskId`/`threadId`), and treat it as a live-only signal — skip
  or expire it on outbox replay rather than replaying a stale cancel.

### [P1] Thread-identity contract inverted: host mints thread ids and sends "continue" for threads the agent has never seen; agent-minted ids are never recorded

- Location: src/tasks/orchestrator.ts:117-121 (`emitMessage` mints `threadId` and always sends it); src/tasks/store.ts:154-162 (`setCurrentThread` — the only writer of `current_thread_id`)
- Description: protocol §5 (`message.threadId`) says: present → CONTINUE that
  existing thread (resume its stored `engine_session_id`); absent → the AGENT
  mints the new thread id and returns it so the host records it as
  `current_thread_id`. The orchestrator does the opposite: for a first message
  or `newThread` it host-mints a fresh ULID and sends it as `threadId` —
  wire-semantically a request to continue a thread that does not exist
  agent-side. Symmetrically, the host never records agent-minted thread ids
  (`setCurrentThread` is called only from `emitMessage`), so the §5 "host
  records the returned id" half is also unimplemented. Today this works only
  because the agent leniently auto-creates unknown thread ids — behavior the
  contract does not specify — and it breaks the §8 invariant that both sides
  test against the published schema "so neither side drifts".
- Failure scenario: any strictly-conforming agent (or a future agent version
  that validates "continue" against known threads) rejects or misroutes the
  first message of every task — e.g. fails it as an unknown-thread resume — and
  the conversation never starts. Until then the deviation is latent, which is
  why this is graded P1 rather than P0.
- Suggested fix direction: pick one side and align code + spec: either amend
  protocol §5 to say the host mints thread ids (and require the agent to
  create-on-first-sight), or implement the spec as written (omit `threadId` on
  new threads and record the id returned on `thread_result`/`thread_message`).

### [P1] Orphaned task can be re-placed on a new agent, but is never re-seeded and drags a dead `current_thread_id` along

- Location: src/tasks/dispatch.ts:101-118 (`placeTask` re-places any `agentId == null` task); src/tasks/orchestrator.ts:44-51 (`hasSeededTask` keyed by taskId only, not placement); src/agents/registry.ts:246-252 (context: `removeAgent` clears `tasks.agent_id`)
- Description: `removeAgent` orphans tasks by nulling `tasks.agent_id`. The
  architecture (§3, §8, and lifecycle.ts's own header) says continuing such
  work must be a brand-new task — but nothing enforces that: `placeTask` /
  `assignTaskToAgent` see `agentId == null` and happily pin the SAME task to a
  different agent, silently performing the forbidden migration. The task's
  state then betrays the new agent twice: (1) `hasSeededTask` still finds the
  old `task_upsert` outbox row (it filters only on `taskId` + type), so
  `sendTaskMessage` never emits `task_upsert` to the new agent — the first
  `message` arrives for a task the agent has never seen; (2) `current_thread_id`
  still points at a thread whose engine session lives only on the removed
  agent, so the message asks the new agent to "continue" continuity that the
  no-migration rule says must not be carried. `emitTaskUpsert`'s docstring
  ("re-emit … so a re-placed task re-materializes its desired set") describes a
  re-emit path that no code implements.
- Failure scenario: agent X is removed (its tasks orphaned) → admin runs
  `POST /tasks/:id/assign` (with or without `agent_id`) → task pins to agent Y,
  status `active` → user sends a message → Y receives a `message` frame for an
  unknown task/thread with no preceding `task_upsert`; the turn fails or lands
  in an unseeded context, and the old conversation state is silently presumed.
- Suggested fix direction: make orphaning explicit rather than a reusable NULL —
  e.g. a distinct orphaned marker/status that `placeTask`/`assignTaskToAgent`
  refuse to re-place (forcing the documented "new task" path). If re-placement
  is ever allowed, key `hasSeededTask` on (task, current placement) or reset
  the seed and `current_thread_id` when placement changes.

### [P2] `assignTaskToAgent` is not idempotent on status: re-assigning the same agent resurrects a closed task to `active`

- Location: src/tasks/dispatch.ts:140-142; src/tasks/store.ts:118-136 (`setPlacement` same-agent no-op)
- Description: `placeTask` returns early for an already-placed task
  (src/tasks/dispatch.ts:104), but `assignTaskToAgent` does not: for an
  already-placed task with the same `agent_id`, `setPlacement` is a documented
  idempotent no-op, yet `setStatus(…, "active")` still runs unconditionally.
  The pin is set-once but the status flip is not, so a repeated/retried assign
  call mutates lifecycle state.
- Failure scenario: task is closed via `POST /tasks/:id/close` (status
  `closed`, `close` already sent to the agent) → a client retries or replays
  `POST /tasks/:id/assign {agent_id: same}` → 200 `reason: "placed"` and the
  task flips back to `active`, even though the agent wound it down. The admin
  surface now shows a live task nothing is executing.
- Suggested fix direction: mirror `placeTask`'s early return in
  `assignTaskToAgent` (already-placed → report `already-placed`, do not touch
  status), or only transition to `active` from `pending-assignment`.

### [P2] The documented per-task delivery cursor does not exist, and the `message` `msgId` is assigned non-atomically

- Location: src/tasks/orchestrator.ts:110-112 (docstring: "the host advances its delivery cursor only once the agent msg_acks"); src/protocol/outbox.ts:83-89 (context: `ackMessage` per-row ack with `TODO(M4/B3)` cursor); src/protocol/outbox.ts:47-65 (context: insert-then-update `msgId`)
- Description: host-architecture (§M2: "`msg_ack` (→M4 cursor)") and protocol
  §4.2 ("the host advances its cursor") describe an M4 per-task delivery
  cursor; the orchestrator's comments repeat the claim. In code no cursor
  exists — `ackMessage` just stamps `acked_at` on the single row and carries a
  TODO for the M4 advance. Per-row acking is functionally sound (no skip /
  double-advance is possible), so this is doc/code drift plus a misleading M4
  contract comment rather than message loss. Separately, on the same message
  path `enqueue` assigns `msgId` in a second UPDATE after the insert; a crash
  between the two statements leaves a durable `message` row with `msgId NULL`,
  which `encodeOutbox` then emits with NO delivery id — the agent has no
  `msgId` to `msg_ack`, so the row can never be acked and is replayed on every
  reconnect forever.
- Failure scenario: host crashes between the outbox insert and the `msgId`
  update → after restart, every reconnect replays a `message` frame the agent
  cannot ack; with agent-side dedupe also missing (agent audit P0-3), each
  replay is a fresh duplicate execution of that user prompt.
- Suggested fix direction: set `msgId` in the insert itself (or wrap
  insert+update in one transaction); align the docs/comments with the per-row
  ack design or implement the promised cursor — one of the two, explicitly.

### [P2] No garbage collection for the host outbox; rows for removed agents are unackable forever

- Location: src/tasks/orchestrator.ts:90 and every other `enqueue` producer in the module; src/protocol/outbox.ts:96-102 (context: replay full-scans `acked_at IS NULL` with no index)
- Description: every M4 command (task_upsert, message, cron_upsert/delete,
  interrupt, close, config_update) persists an `outbox` row, and no DELETE on
  `outbox` exists anywhere in `src/` (verified repo-wide). Acked rows
  accumulate indefinitely; unacked rows addressed to a removed agent (which
  can never reconnect — its connect token is revoked) stay `acked_at IS NULL`
  forever. This mirrors agent audit P1-9 on the host side. Note the coupling:
  `hasSeededTask` (src/tasks/orchestrator.ts:44-51) infers "task was seeded"
  from outbox row *presence*, so any future retention job that deletes old
  acked rows silently flips first-message semantics — the seam makes GC
  hazardous to add later.
- Failure scenario: long-lived host with chatty channels → `outbox` grows
  unboundedly; reconnect replay and the `hasSeededTask` lookup degrade; rows
  for every removed agent linger as permanent unacked debris.
- Suggested fix direction: add a retention sweep (delete acked rows past a
  window; delete/void rows for `removed` agents) plus a partial index on
  `acked_at IS NULL`; first migrate `hasSeededTask` to state that survives GC
  (e.g. a `seeded_at`/`seeded_agent_id` column on `tasks`).

### [P2] A never-placed task can never be closed — `close`/`interrupt` 409 before the status write, and no other status route exists

- Location: src/tasks/orchestrator.ts:203-205 (`emitClose` → `requirePlacement` throws for unplaced tasks); src/edge/admin/tasks.ts:272-273 (emit precedes `closeTask`); src/tasks/lifecycle.ts:42-49 (`transitionTaskStatus` exported but not routed)
- Description: the only status-writing route is `POST /tasks/:id/close`, which
  calls `emitClose` before `closeTask`; for a `pending-assignment` task
  `requirePlacement` throws 409 first, so `closeTask` is unreachable.
  `transitionTaskStatus` — described as "the ONLY status writer the admin
  surface uses" — has no admin route at all (dead export), so there is no way
  to retire a task that never found an agent.
- Failure scenario: task created for an owner with no permitted agent →
  `pending-assignment` → owner abandons it → admin tries `POST /tasks/:id/close`
  → 409 `TASK_NOT_PLACED`. The task sits in the pending queue forever and will
  be silently activated if an agent is ever granted and someone runs a blanket
  `/assign`.
- Suggested fix direction: in the close route, skip the wire `close` (nothing
  to command) and just mark the task closed when it is unplaced — or expose
  `transitionTaskStatus` as a status route; also make `placeTask` skip tasks
  already in a terminal status.

### [P3] Frame-catalog drift: `task_upsert` / `cron_upsert` payloads sent by M4 are not what protocol §5 documents

- Location: src/tasks/orchestrator.ts:69-89 (`task_upsert` data: engine/model/workdir/meta/entries); src/tasks/orchestrator.ts:158 and src/tasks/orchestrator.ts:176-182 (`cron_upsert` data carries `prompt`)
- Description: protocol §5 documents `task_upsert.data` as `{name?, kind?,
  status?}` and `cron_upsert.data` without `prompt`; the orchestrator sends the
  richer OD15/M8 payload (engine, model, workdir, meta, entries) and the cron
  `prompt`. Receivers-ignore-unknown-fields (§4.2) keeps it working, and the
  agent clearly consumes the extra fields — i.e. the published contract
  under-documents the real one, weakening the §8 "neither side drifts" claim
  and the generated JSON-schema tests.
- Failure scenario: a conforming third-party or regenerated agent implements
  §5 as written and ignores `engine`/`prompt` etc. — tasks seed without an
  engine and crons apply without work to run.
- Suggested fix direction: update protocol §5 (and the generated §8 schema)
  to document the actual `task_upsert`/`cron_upsert` payloads.

### [P3] Cron update/delete accept arbitrary `cronId`s with no existence check — a typo'd PUT silently creates a second cron

- Location: src/tasks/orchestrator.ts:175 (`input.cronId ?? ulid()` — caller id accepted verbatim); src/edge/admin/tasks.ts:310-325 (PUT route passes the path param straight through)
- Description: `PUT /tasks/:id/crons/:cronId` never checks the cron mirror (or
  anything else) for the id; because `cron_upsert` is an upsert on the agent,
  an unknown id creates a new cron rather than failing. The update endpoint
  therefore doubles as create-with-arbitrary-id, and a mistyped/stale id
  silently forks a duplicate schedule instead of returning 404. DELETE has the
  same shape (harmless no-op agent-side, but also no 404).
- Failure scenario: client updates a cron using a stale id (e.g. one lost to
  the P1 seeding bug above, or a copy/paste error) → 200 OK → the old cron
  keeps firing unchanged and a second cron with the typo'd id starts firing
  too.
- Suggested fix direction: on PUT/DELETE, require the `cronId` to exist in the
  `crons` mirror (404 otherwise), accepting the mirror's eventual-consistency
  lag as the documented caveat.

Section summary: P0=1 P1=4 P2=4 P3=2
