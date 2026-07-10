# Host Audit — M1 Edge + M2 Protocol Adapter (2026-07-10)

Scope: `src/protocol/**` (envelope, frames, inbound, outbox, version), `src/edge/**`
(agent data plane, admin control API, messages, llm, stub), `src/routes/**`,
`src/modules/docs/**`, `src/modules/system/**`. Ground truth: `docs/protocol.md`
§4–§6 and `docs/host-architecture.md` §M1/M2/8. Cross-module claims (sessions,
msgstore writers, orchestrator emission, registry, liveness) were verified against
those files read-only. Method: pma-cr repository-audit policy, per-file line review
with correctness / security / concurrency / idempotency / lifecycle / error-handling
lenses.

Verified-conformant (no finding): outbox persist-before-send and unacked replay in
id order on reconnect (`outbox.ts:46-71,96-111`, wired at `agent.ts:250-255`);
`messages` idempotent insert on `(thread_id, seq)` with ON CONFLICT DO NOTHING and
`ORDER BY seq` reads; `thread_runs`/`crons` idempotent UPSERT mirrors; envelope-only
routing (decode never parses `data`); strict register version gate; explicit prefix
routing with clean 404 (no fall-through into the `/llm` proxy); per-agent connect
token resolved from the token (hash lookup, constant-time verify, removed fails
closed) with cross-agent path check; store-write frames authorized against the
token-resolved agent (`authorizeWrite`).

---

### [P0] `ack` / `msg_ack` are not scoped to the sending agent — any agent can suppress another agent's undelivered frames

- Location: src/protocol/inbound.ts:155-166; src/protocol/outbox.ts:74-89
- Description: `applyFrame` handles `ack {id}` via `ackOutbox(db, id)` and
  `msg_ack {msgId}` via `ackMessage(db, msgId)`. Neither filters on
  `outbox.agent_id = deps.agentId`, even though the authenticated agent identity is
  in scope (`deps.agentId`) and store-write frames are already gated by
  `authorizeWrite`. `outbox.id` is a global autoincrement integer shared by all
  agents, so foreign delivery ids are trivially enumerable. Additionally
  `ackOutbox` has no `type` filter, so a plain `ack` can mark a `message` row
  delivered without the durable-storage confirmation `msg_ack` is defined to carry
  (protocol §4.2).
- Failure scenario: agent B (or anyone holding any valid connect token, including a
  leaked one) sends `{"type":"ack","id":N}` for N = 1..max over its own WS or
  `POST /agent/output`. Rows belonging to agent A — including queued `message`,
  `cron_upsert`, `task_upsert`, `close` frames not yet delivered because A is
  offline — get `acked_at` set. When A reconnects, `replayUnacked` finds nothing:
  the work is silently and permanently lost. This breaks the at-least-once
  delivery contract (§4.2) across a tenant boundary.
- Suggested fix direction: add `eq(outbox.agentId, agentId)` to both ack UPDATEs
  (thread the agent id into `ackOutbox`/`ackMessage`), and restrict `ackOutbox` to
  non-`message` rows so only `msg_ack` can complete a message delivery.

### [P1] Host mints thread ids and never exercises the documented fresh-thread handshake (absent `threadId` → agent-minted id)

- Location: src/tasks/orchestrator.ts:117-121 (emission), surfaced via src/edge/admin/tasks.ts:219-242
- Description: protocol §5 defines `message.threadId` as: present → CONTINUE that
  existing thread (resume its stored `engine_session_id`); absent → the AGENT mints
  a new unique thread id and returns it on the resulting frames "so the host records
  it as the task's `current_thread_id`". The host instead mints a ULID itself for
  the first message (and `new_thread`) and sends it as if continuing; the
  absent-`threadId` path is never used, and no inbound code path records an
  agent-minted thread id into `tasks.current_thread_id` (grep: `setCurrentThread`
  is called only from `emitMessage`). The contract defines no "create a thread with
  a host-supplied id" semantics, so the host depends on undocumented
  create-on-unknown-id behavior in the peer.
- Failure scenario: a strictly protocol-conforming agent receives
  `message {taskId, threadId: <never-seen ULID>}`, looks up the thread to resume
  its `engine_session_id`, finds nothing, and fails or rejects the turn — the
  conversation never starts. Conversely, if a future agent build honors §5 and
  mints its own id for any recovery path, the host has no code to record it and the
  task's continuity silently forks.
- Suggested fix direction: either implement the documented handshake (send the
  first message with no `threadId`, record the returned id from
  `thread_message`/`thread_result` into `current_thread_id`), or amend
  `docs/protocol.md` §5 to specify host-minted thread ids and the agent's
  create-on-first-sight obligation. Pick one; today code and contract disagree.

### [P1] `/admin/tasks/:id/interrupt` and `/close` present agent-global control frames as task-scoped operations

- Location: src/edge/admin/tasks.ts:244-276; src/tasks/orchestrator.ts:197-206
- Description: protocol §5 defines `interrupt` as "graceful cancel of the current
  turn" and `close` as "wind down and exit", both pure control frames that "carry
  no correlation ids" (§4.1). The admin routes are per-task and the orchestrator
  attaches a `taskId` the contract tells the receiver to ignore as an unknown
  field. An agent hosts N tasks (host-architecture §0 law 5), so the frame's real
  effect is agent-wide, not task-wide.
- Failure scenario: tasks A and B are placed on one agent; B's thread is mid-turn.
  Admin calls `POST /admin/tasks/A/interrupt` — the agent cancels the *current*
  turn, i.e. B's work. Worse: `POST /admin/tasks/A/close` marks task A closed on
  the host and tells the whole agent daemon to exit, killing B's execution and all
  scheduled crons on that agent.
- Suggested fix direction: either add task-scoped `interrupt`/`close` semantics to
  the protocol (make `taskId` normative for these frames and bump/extend the
  contract), or constrain the admin surface (agent-level endpoints for
  interrupt/close, or refuse when the agent hosts other active tasks).

### [P1] Permanently unprocessable inbound frames are never acked — poison frames replay forever

- Location: src/protocol/inbound.ts:117,129,143 (throwing `.parse`), 180-198 (drop without ack); src/protocol/inbound.ts:81-96 (authorize deny, no ack)
- Description: a known-type frame whose `data` fails the zod schema throws, is
  caught in `handleInboundFrame`, and is dropped without an ack; `authorizeWrite`
  denials likewise drop without ack. Not acking is correct for *transient* storage
  failures (the agent should replay), but the host makes no distinction for
  *permanent* failures: per §4.2 the agent must keep the frame in its outbox and
  retransmit it on every reconnect/flush, forever. There is no poison policy,
  counter, or dead-letter on either side of this loop. Note `ThreadMessageData.kind`
  is a closed `z.enum` — an agent build that emits any new `kind` value produces
  frames the host will refuse to ack for eternity, an entirely plausible
  version-drift event for an additive-by-design protocol (§1).
- Failure scenario: agent 1.1 adds `kind: "usage"` transcript events. Every such
  frame fails `parse`, is never acked, and accumulates unacked in the agent's
  outbox; the agent (which retransmits its whole unacked backlog per flush — see
  agent audit P2-16) burns bandwidth and log noise forever, and the frames' seq
  numbers become permanent transcript holes.
- Suggested fix direction: separate malformed/denied (permanent → log + ack-and-drop
  or persist to a dead-letter table, so the agent stops replaying) from storage
  errors (transient → no ack, replay). Consider loosening enum-valued business
  fields to `z.string()` with a recognized-set check at the writer.

### [P1] Superseded-socket close marks a live reconnected agent `offline`, and nothing but a reconnect ever restores `online`

- Location: src/edge/agent.ts:267-273 (onClose); src/edge/agent.ts:250-255 (onOpen); src/agents/liveness.ts:51-74; src/agents/registry.ts:133-142
- Description: `SessionRegistry.attach` closes any stale socket on reconnect, and
  `detach(agentId, ws)` correctly ignores a close from a superseded socket — but
  the route's `onClose` also runs `setAgentStatus(db, agentId, "offline")`
  unconditionally. On every dirty reconnect (agent redials while the host still
  holds the dead socket) the ordering is: new socket attaches + status `online` →
  host closes old socket → old socket's `onClose` fires → status flipped `offline`
  while the new socket is live. The flip is sticky: `touchLiveness` (heartbeat)
  updates only `last_seen`/`active_threads`, never status, and the liveness sweep
  only ever sets `offline`. Related same-shape gap: `onOpen` does not bump
  `last_seen`, so an agent reconnecting after a long outage can be swept `offline`
  by `sweepLiveness` (stale `last_seen` older than grace) before its first
  heartbeat lands — equally sticky.
- Failure scenario: agent's network blips; it redials before TCP notices the old
  socket died. DB now says `offline` although frames flow normally. Dispatch,
  capacity views, and `/admin/agents` all report a healthy agent as offline until
  the next full reconnect.
- Suggested fix direction: only write `offline` when the closing socket is still
  the current one (have `detach` return whether it removed the session, or consult
  `sessions.get`); bump `last_seen` in `onOpen`; consider restoring `online` on
  heartbeat for an agent whose status is `offline` but whose socket is attached.

### [P1] Every `thread_result` is recorded with `source: "chat"` and no `cron_id` — cron runs are indistinguishable in the read model

- Location: src/protocol/inbound.ts:130-133
- Description: the inbound handler is the only production caller of
  `writeThreadResult` and hardcodes `source: "chat"` with no `cronId` (the in-code
  comment defers this to "M4/B3", which never landed). Host-architecture M5 states
  the host "derives *dispatched* counts from `thread_runs` by `cron_id`" and §6
  models `thread_runs.cron_id → crons`; both are permanently NULL/wrong for every
  cron-fired thread.
- Failure scenario: a task has an active cron firing hourly. Every resulting
  `thread_result` lands as `source='chat', cron_id=NULL`; any operator query or
  future UI that counts dispatches per cron, or separates scheduled from
  interactive work, returns zero cron activity while the cron mirror shows
  `round_count` climbing.
- Suggested fix direction: derive the cron context at write time — e.g. match the
  agent's documented isolated-fire thread-id shape (`cronId@epoch`, protocol §1
  example / agent-architecture §12) or carry dispatch context via a host-side
  thread registry keyed by the ids the host mints/receives; alternatively add an
  optional additive envelope/cron field to `thread_result` in the contract.

### [P2] `message` enqueue is non-atomic: a crash between INSERT and the msgId UPDATE yields a frame with no delivery id

- Location: src/protocol/outbox.ts:46-71; src/protocol/envelope.ts:86-93
- Description: `enqueue` inserts the outbox row with `msgId: null`, then sets
  `msgId = id` in a second, non-transactional statement. If the process dies
  between the two, the durable row survives with `msgId` NULL; `encodeOutbox` then
  emits a `message` frame with *no* `msgId` and no `outboxId` on every replay. The
  agent can neither `msg_ack` it (nothing to reference) nor dedupe it by id
  (§4.2), so the row replays on every reconnect forever — and an agent that
  processes it anyway re-executes the same user prompt each time.
- Failure scenario: host crashes mid-`enqueue` for a `message`. From then on, each
  agent reconnect receives an unackable duplicate of that message; the row can
  never be marked acked (the `msg_ack` matcher keys on `outbox.msgId`).
- Suggested fix direction: make the frame's delivery id derivable in one step —
  either wrap insert+update in a transaction, or drop the `msgId` column write and
  have `encodeOutbox` emit `msgId: row.id` for `message` rows (with `ackMessage`
  matching on `id` + `type='message'`).

### [P2] Outbox has no retention/GC and no supporting indexes

- Location: src/db/schema.ts:146-157; src/protocol/outbox.ts:96-111; src/tasks/orchestrator.ts:44-51
- Description: no code path ever deletes `outbox` rows (repo-wide grep: no
  `delete(outbox)`), so acked frames — one per transcript-triggering command,
  message, and control frame — accumulate for the life of the database. The
  replay query (`agent_id = ? AND acked_at IS NULL`) and `hasSeededTask`
  (`task_id = ? AND type = 'task_upsert'`) both run over unindexed columns, so
  every reconnect and every first-message check degrade linearly with total
  history. This mirrors agent-side finding P1-9 on the host.
- Failure scenario: a long-lived deployment with chatty tasks reaches millions of
  outbox rows; each agent reconnect full-scans the table, and admin `message`
  sends slow down proportionally.
- Suggested fix direction: retention-window DELETE for acked rows (noting
  `hasSeededTask` must then use a durable flag on `tasks` instead of outbox
  history) plus a partial/composite index on `(agent_id) WHERE acked_at IS NULL`.

### [P2] `/agent/output` maps transient storage failures to 400, telling the agent a retryable frame is permanently bad

- Location: src/edge/agent.ts:308-324; src/protocol/inbound.ts:200-213
- Description: `ingestHttpFrame` throws for both a malformed frame and a failed
  M5 write; the route catches every throw and returns
  `400 BAD_FRAME "malformed or unprocessable frame"`. The inbound module's own
  doc-comment says a storage failure should surface non-2xx *so the agent
  retries*, but 400 is the canonical "do not retry" class — a conforming client
  drops the frame. (Denied/ignored frames also return `{ok:true}`, indistinguishable
  from a stored ack, which is acceptable ignore-policy but worth noting.)
- Failure scenario: agent posts a `thread_result` via the pre-WS fallback while
  the host DB hits a transient `SQLITE_BUSY`; the agent receives 400, treats the
  frame as poison, and the terminal result is lost even though a retry one second
  later would have succeeded.
- Suggested fix direction: distinguish decode failure (400) from storage failure
  (rethrow → 500, or 503) in `ingestHttpFrame` (e.g. tag the malformed-frame
  error and only map that one to 400).

### [P2] No rate limiting anywhere on the edge; the rate-limit middleware is orphaned dead code

- Location: src/shared/middleware/rate-limit.ts:94 (zero importers); src/edge/agent.ts:184-224
- Description: protocol §3 defines a `rate_limited` register_error and
  host-architecture M1 says the edge "owns transport, auth, rate-limit" — but
  `rateLimit()` is imported nowhere, and no route (notably `POST /agent/register`,
  authenticated by a *shared long-lived* token) is throttled. Register also
  creates a fresh `agents` row + token hash on every call.
- Failure scenario: (a) an attacker brute-forces the shared register token at full
  connection speed — the constant-time compare protects against timing, not
  volume; (b) a misconfigured agent stuck in a register-retry loop mints thousands
  of `enrolled` agent rows (each with a hashed token), polluting the registry and
  admin views.
- Suggested fix direction: mount the existing `rateLimit` middleware on
  `/agent/register` (returning the documented `register_error{rate_limited}`
  envelope) and consider a coarse limit on the other token-guarded write surfaces;
  otherwise delete the middleware so a dead trust-boundary component doesn't imply
  protection that isn't there.

### [P2] Byte-ingest endpoints buffer unbounded request bodies in memory

- Location: src/edge/agent.ts:397; src/edge/admin/config.ts:60
- Description: `POST /agent/attachments` and `POST /admin/config/artifacts` read
  the entire body with `await c.req.arrayBuffer()` before any size check —
  no Content-Length precheck, no streaming cap, no configured limit. Any
  connect-token or channel-token holder can post an arbitrarily large body; the
  full payload (plus a `Uint8Array` copy and a sha256 pass) sits in host memory.
  This mirrors agent-side finding P1-10(a) on the host.
- Failure scenario: a compromised or buggy agent uploads a multi-GB "attachment";
  the host process balloons and is OOM-killed, taking down the control plane for
  every other agent.
- Suggested fix direction: enforce a configurable max upload size — reject on
  declared Content-Length, and stream-with-cap (or at minimum check
  `arrayBuffer().byteLength` against a limit before hashing/storing).

### [P2] Attachment descriptors accepted as `z.unknown()` and forwarded onto the wire unvalidated

- Location: src/edge/messages.ts:29; src/edge/admin/tasks.ts:94
- Description: both message-bearing edges validate `attachments` as
  `z.array(z.unknown())` and pass the items through
  `routeInbound`/`sendTaskMessage` into `message.data.attachments`. The frame
  catalog (`MessageData` → `AttachmentDescriptor`) requires
  `{id,name,content_type,size,sha256,ref}`; the host — the *sender* — can thus
  emit frames that violate its own published contract (protocol.md §8 schema).
- Failure scenario: a channel client sends `attachments: [42]`; the host persists
  and delivers a `message` frame whose data fails the agent's typed decode — at
  best the attachments vanish silently, at worst the agent rejects the whole
  frame and (given at-least-once) the host replays the poison message forever.
- Suggested fix direction: validate the request field against the
  `AttachmentDescriptor` zod schema (imported from `protocol/frames`) at both
  edges and 422 on mismatch.

### [P3] `final_seq` completeness verification is never performed

- Location: src/protocol/inbound.ts:129-133; src/msgstore/thread-runs.ts:52
- Description: protocol §6 and host-architecture M5 describe transcript
  completeness checking via `thread_result.final_seq` ("the host verifies it
  received 1..=final_seq"). The value is stored on `thread_runs` and never
  compared against the `messages` rows, so a permanently lost transcript event
  (e.g. one dropped by the P1 poison path or an authorize denial) leaves a silent
  hole no one detects.
- Failure scenario: thread finishes with `final_seq=42` but seq 17 was dropped;
  the transcript reads clean, egress delivers 41 messages, and nothing flags the
  gap.
- Suggested fix direction: on `thread_result` ingest (or a periodic sweep),
  compare `COUNT(*)`/`MAX(seq)` for the thread against `final_seq` and log/flag a
  gap metric for operator attention.

### [P3] Register failure paths do not use the documented `register_error` envelope; documented reasons unreachable

- Location: src/edge/agent.ts:193-198; src/shared/middleware/auth.ts:45-56
- Description: protocol §3 enumerates `invalid_register_token | rate_limited |
  invalid_body | registration_disabled | unsupported_protocol` as
  `register_error` reasons. Only `unsupported_protocol` is ever returned. A bad or
  missing token, or an unparseable body, is rejected by `registerTokenAuth` with
  the scaffold's generic 401 envelope (`UnauthorizedError`), not
  `register_error{invalid_register_token|invalid_body}`; `rate_limited` and
  `registration_disabled` have no implementation at all.
- Failure scenario: an agent implementing §3 parses the register failure body for
  `register_error.reason` to decide between "exit permanently" vs "retry"; it
  receives a different JSON shape and falls into its unknown-error handling.
- Suggested fix direction: return the `register_error` envelope from the register
  route for all §3 reasons (move token validation into the handler or map the
  401 in an error hook scoped to `/agent/register`).

### [P3] Orphaned skeleton files: `edge/stub.ts` and the stale `protocol/index.ts` placeholder

- Location: src/edge/stub.ts:1-30; src/protocol/index.ts:1-11
- Description: `stub.ts` (`notImplemented`/`stubRoute`, the "501 until B1–B6"
  helpers) has zero importers — the build-out completed and left it behind.
  `protocol/index.ts` still reads "SKELETON PLACEHOLDER — no logic yet
  (workstreams C/D)" and exports only an unused `MODULE` constant, while the
  module beside it is fully implemented; consumers import from
  `@/protocol/<file>` directly. Dead scaffolding that misstates the state of the
  only wire-coupled module.
- Failure scenario: a reader (or tool) trusts the placeholder comment and
  concludes M2 is unimplemented; stub helpers get resurrected for a new route and
  ship a 501 surface that skips the auth-documentation conventions used
  elsewhere.
- Suggested fix direction: delete `edge/stub.ts`; replace `protocol/index.ts`
  with a real barrel (or accurate header) for the M2 module.

### [P3] `GET /admin/tasks/:id` returns every message of every thread with no bound

- Location: src/edge/admin/tasks.ts:186-196
- Description: the detail route loads `getTaskThreads` and then the *full*
  transcript of each thread into one JSON response — no limit, no pagination,
  no per-thread cap. A long-running conversational task with cron history can
  hold tens of thousands of `messages` rows (tool inputs/outputs included).
- Failure scenario: an operator opens a months-old task in the admin UI; the host
  serializes a multi-hundred-MB response, blocking the event loop and possibly
  exhausting memory.
- Suggested fix direction: return thread summaries (run + counts + last seq) by
  default and page transcript reads behind a separate `?thread=&after=&limit=`
  query, reusing the existing cursor-read shape.

### [P3] `GET /:channel/poll` numeric query params are coerced with `Number()` and never validated

- Location: src/edge/messages.ts:146-160
- Description: `after`, `limit`, and `wait` are passed through `Number()` with no
  guard: garbage yields `NaN`, which flows into the cursor comparison
  (`after: NaN`) and the long-poll deadline (`Date.now() + NaN` → `NaN`, so the
  wait silently degrades to a single poll). The POST variant validates the same
  fields with zod (`nonnegative`, `positive`), so the two entry points accept
  different contracts for the same operation.
- Failure scenario: a client sends `?after=latest&wait=5000x`; instead of a 422 it
  gets an immediate, possibly cursor-ignoring response and misinterprets the
  stream position.
- Suggested fix direction: validate the GET query with the same zod schema as the
  POST body (Hono `validator("query", ...)`), rejecting non-numeric input.

Section summary: P0=1 P1=5 P2=6 P3=5
