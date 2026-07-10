# Gino Host Daemon — Source Audit (2026-07)

**Date:** 2026-07
**Scope:** `src/**` of the Gino host daemon at the audited commit, cross-checked
against `docs/host-architecture.md` (module map, §3–§8 invariants) and
`docs/protocol.md` (§3–§6 wire contract). Read-only cross-references to
`docs/audit/agent-audit-2026-07.md` where the same pattern appears on both sides.

**Method:** each module (M1/M2 edge + protocol, M3 agent management, M4 task
management & dispatch, M5/M6 message store & channels, M7 credential proxy, M8
config store, M9 platform/db/shared/bootstrap) was audited line-by-line under the
pma-cr repository-audit policy with correctness / security / concurrency /
idempotency / lifecycle / error-handling lenses. Three cross-cutting lenses (UTC
discipline, auth-surface inventory, at-least-once / idempotency end-to-end trace)
were run over all of `src/**`. Findings are graded P0 (critical) → P3 (minor).
Each finding has a stable id `HOST-<module>-<nn>` (`HOST-XC-<nn>` for cross-cutting).
Where a cross-cutting lens re-reports an issue already owned by a module chapter,
the module entry is canonical and the cross-cutting entry is a cross-reference
(not double-counted); those dedupes are noted inline.

---

## Executive Summary

### Totals by grade

| Grade | Count |
|---|---|
| P0 | 5 |
| P1 | 18 |
| P2 | 41 |
| P3 | 27 |
| **Total** | **91** |

Counts reflect the deduplicated finding set: 89 module findings plus 2 unique
cross-cutting findings (both P2). The other 9 cross-cutting entries duplicate
module findings and are cross-referenced, not counted again.

### Findings per module

| Module | P0 | P1 | P2 | P3 |
|---|---|---|---|---|
| M1/M2 — Edge + Protocol Adapter | 1 | 5 | 6 | 5 |
| M3 — Agent Management | 1 | 3 | 8 | 3 |
| M4 — Task Management & Dispatch | 1 | 4 | 4 | 2 |
| M5/M6 — Message Store & Channels | 0 | 2 | 6 | 3 |
| M7 — Credential Proxy + Lode | 1 | 1 | 2 | 3 |
| M8 — Config Store | 1 | 2 | 7 | 4 |
| M9 — Platform / DB / Bootstrap | 0 | 1 | 6 | 7 |
| XC — Cross-cutting (unique) | 0 | 0 | 2 | 0 |
| **Total** | **5** | **18** | **41** | **27** |

### Top P0 findings

| id | title | location |
|---|---|---|
| HOST-M1M2-01 | `ack`/`msg_ack` not scoped to sending agent — any agent can suppress another's undelivered frames | `src/protocol/inbound.ts:155` |
| HOST-M3-01 | `removed` is not terminal — a late WS close resurrects a removed agent as `offline` | `src/agents/registry.ts:124` |
| HOST-M4-01 | Per-task `close` emits the runner-level exit frame, killing every other task on the agent | `src/tasks/orchestrator.ts:203` |
| HOST-M7-01 | Forged `sk-ant-` key bypasses proxy auth and spends the host's OpenAI/Codex credentials | `src/proxy/auth.ts:31` |
| HOST-M8-01 | Global config carries no `version`; the reconcile gate is broken, so global desired-set changes are never applied | `src/configstore/entries.ts:164` |

### Top P1 findings

| id | title | location |
|---|---|---|
| HOST-M1M2-02 | Host mints thread ids and never exercises the documented fresh-thread handshake | `src/tasks/orchestrator.ts:117` |
| HOST-M1M2-03 | `interrupt`/`close` presented as task-scoped but are agent-global control frames | `src/edge/admin/tasks.ts:244` |
| HOST-M1M2-04 | Permanently unprocessable inbound frames are never acked — poison frames replay forever | `src/protocol/inbound.ts:117` |
| HOST-M1M2-05 | Superseded-socket close marks a live reconnected agent `offline` | `src/edge/agent.ts:267` |
| HOST-M1M2-06 | Every `thread_result` recorded with `source:"chat"` and no `cron_id` | `src/protocol/inbound.ts:130` |
| HOST-M3-02 | Reconnect supersede marks a live agent `offline` | `src/agents/sessions.ts:46` |
| HOST-M3-03 | Sweep-marked `offline` is one-way while the socket stays connected | `src/agents/liveness.ts:51` |
| HOST-M3-04 | `removeAgentControl` aborts durable revocation when the launcher op fails | `src/agents/control.ts:127` |
| HOST-M4-02 | Cron created before the first message is silently lost — task never seeded | `src/tasks/orchestrator.ts:150` |
| HOST-M4-03 | `interrupt` is not task-scoped and stale interrupts replay on reconnect | `src/tasks/orchestrator.ts:197` |
| HOST-M4-04 | Thread-identity contract inverted; agent-minted ids never recorded | `src/tasks/orchestrator.ts:117` |
| HOST-M4-05 | Orphaned task re-placed on a new agent, never re-seeded, drags a dead `current_thread_id` | `src/tasks/dispatch.ts:101` |
| HOST-M5M6-01 | Poll egress advances the cursor on read, before the client receives the bytes | `src/channels/egress.ts:215` |
| HOST-M5M6-02 | Webhook SSRF guard bypassable (IPv4-mapped IPv6, `0.0.0.0`, alternate encodings) | `src/channels/webhooks.ts:88` |
| HOST-M7-02 | Streaming Claude→OpenAI bridge never tears down the upstream WS on client abort | `src/proxy/bridge.ts:78` |
| HOST-M8-02 | `setEntries` delete-then-insert is not transactional — a scope's desired set can be wiped | `src/configstore/entries.ts:113` |
| HOST-M8-03 | Global entries filtered to the agent's primary engine only — multi-engine agents lose config | `src/configstore/entries.ts:164` |
| HOST-M9-01 | Graceful shutdown is inverted: `server.stop(true)` runs first, killing all in-flight work | `src/index.ts:57` |

---

## Chapter M1/M2 — Edge + Protocol Adapter

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

### HOST-M1M2-01 · [P0] `ack` / `msg_ack` are not scoped to the sending agent — any agent can suppress another agent's undelivered frames

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

### HOST-M1M2-02 · [P1] Host mints thread ids and never exercises the documented fresh-thread handshake (absent `threadId` → agent-minted id)

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
- Related: HOST-M4-04 (M4 chapter) reports the same thread-identity inversion from the orchestrator/store side.

### HOST-M1M2-03 · [P1] `/admin/tasks/:id/interrupt` and `/close` present agent-global control frames as task-scoped operations

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
- Related: HOST-M4-01 (per-task `close`, graded P0 from the orchestrator side) and HOST-M4-03 (per-task `interrupt`).

### HOST-M1M2-04 · [P1] Permanently unprocessable inbound frames are never acked — poison frames replay forever

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

### HOST-M1M2-05 · [P1] Superseded-socket close marks a live reconnected agent `offline`, and nothing but a reconnect ever restores `online`

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
- Related: HOST-M3-02 (same unguarded-status root cause, agent-management chapter) and HOST-M3-03 (sweep one-way flip).

### HOST-M1M2-06 · [P1] Every `thread_result` is recorded with `source: "chat"` and no `cron_id` — cron runs are indistinguishable in the read model

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
- Related: HOST-M5M6-06 (same `thread_runs.cron_id` gap from the msgstore side).

### HOST-M1M2-07 · [P2] `message` enqueue is non-atomic: a crash between INSERT and the msgId UPDATE yields a frame with no delivery id

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
- Related: HOST-M4-07 (same non-atomic msgId assignment, task chapter); the cross-cutting trace re-reports it as a dedupe (see HOST-XC, Lens C).

### HOST-M1M2-08 · [P2] Outbox has no retention/GC and no supporting indexes

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
- Related: HOST-M4-08 and HOST-M9-07 report the same host-outbox GC/index gap from the task and platform chapters.

### HOST-M1M2-09 · [P2] `/agent/output` maps transient storage failures to 400, telling the agent a retryable frame is permanently bad

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

### HOST-M1M2-10 · [P2] No rate limiting anywhere on the edge; the rate-limit middleware is orphaned dead code

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
- Related: HOST-M9-02 (same dead rate-limit middleware from the platform chapter); the cross-cutting auth lens dedupes to these.

### HOST-M1M2-11 · [P2] Byte-ingest endpoints buffer unbounded request bodies in memory

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

### HOST-M1M2-12 · [P2] Attachment descriptors accepted as `z.unknown()` and forwarded onto the wire unvalidated

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

### HOST-M1M2-13 · [P3] `final_seq` completeness verification is never performed

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
- Related: the cross-cutting idempotency lens re-reports this (see HOST-XC, Lens C); this module entry is canonical.

### HOST-M1M2-14 · [P3] Register failure paths do not use the documented `register_error` envelope; documented reasons unreachable

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
- Related: the cross-cutting auth lens re-reports this (see HOST-XC, Lens B); this module entry is canonical.

### HOST-M1M2-15 · [P3] Orphaned skeleton files: `edge/stub.ts` and the stale `protocol/index.ts` placeholder

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

### HOST-M1M2-16 · [P3] `GET /admin/tasks/:id` returns every message of every thread with no bound

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

### HOST-M1M2-17 · [P3] `GET /:channel/poll` numeric query params are coerced with `Number()` and never validated

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

*Chapter summary: P0=1 P1=5 P2=6 P3=5 (17 findings).*

---

## Chapter M3 — Agent Management (`src/agents/**`)

Scope: `src/agents/index.ts`, `registry.ts`, `sessions.ts`, `liveness.ts`, `control.ts`,
`launchers/{index,types,manual,docker}.ts`, read against `docs/host-architecture.md` (§M3, §4, §8),
`docs/protocol.md` (§3, §4-6), `src/db/schema.ts`, and the wiring context in `src/edge/agent.ts`,
`src/protocol/{inbound,outbox}.ts`, `src/tasks/dispatch.ts` (context read-only; all findings anchor
in the audited module). Cross-checked against `docs/audit/agent-audit-2026-07.md` where noted.

What is sound: connect tokens are high-entropy (32 random bytes), stored only as SHA-256 hashes,
verified constant-time (`platform/crypto.ts` `timingSafeEqual`), revoked on remove by hash rotation
plus a `removed`-status fail-closed check; the token→identity resolver never trusts a client-supplied
agent id; launchers are cleanly isolated behind a capability-gated seam with a safe `manual` default;
the docker launcher builds argv arrays (no shell interpolation) and spreads host-authoritative env
keys last so callers cannot clobber them. The findings below are the deviations.

### HOST-M3-01 · [P0] `removed` is not terminal — a late WS close resurrects a removed agent as `offline`

- Location: src/agents/registry.ts:124 (`setAgentStatus`), src/agents/control.ts:122-139, wiring src/edge/agent.ts:267-273
- Description: `setAgentStatus` is an unconditional last-write-wins UPDATE with no state guard, and
  the WS `onClose` handler calls it with `"offline"` for every socket close. `removeAgentControl`
  removes an online agent by sending a `close` (or `docker rm`), then runs `removeAgent` which sets
  `status = "removed"`. The socket close that the remove itself provoked arrives afterwards and
  overwrites `removed` with `offline`. Both fail-closed gates (`isOwnerPermitted`,
  `dispatch.ts:65/130`) check only `status === "removed"`, so the resurrected agent is again a valid
  placement target — while its connect token is revoked, so it can never connect again.
- Failure scenario: Admin removes an online `public` manual agent. `close` frame is pushed, agent
  closes the socket, `onClose` fires after `removeAgent` committed → row ends `offline`, token
  revoked, tasks orphaned. The next new task for any owner ranks this agent as a candidate
  (`dispatch` only excludes `removed`; `offline` merely ranks lower) and, if it is the only permitted
  agent, pins the task to it set-once — the task is permanently stranded on an agent that can never
  reconnect (no migration, DP4). Silent task loss in a routine admin flow.
- Suggested fix direction: Make lifecycle transitions guarded CAS updates (`SET status='offline'
  WHERE id=? AND status='online'`, `removed` only from non-removed, never out of `removed`), or have
  `onClose` skip the status write when the row is already `removed`.

### HOST-M3-02 · [P1] Reconnect supersede marks a live agent `offline` — the old socket's close clobbers the new session's status

- Location: src/agents/sessions.ts:46-57 (`attach` closes the superseded socket), wiring src/edge/agent.ts:267-273
- Description: On reconnect, `attach` closes the stale socket (1012). The stale socket's `onClose`
  fires after the new connection's `onOpen` already set `online`. `detach(agentId, ws)` correctly
  no-ops (guarded on socket identity), but the adjacent `setAgentStatus(db, agentId, "offline")` is
  unconditional and not tied to whether this close evicted the current session — the freshly attached
  agent is recorded `offline`. Nothing restores `online` except a future `onOpen` (see next finding),
  so the DB state stays wrong for the life of the connection.
- Failure scenario: Agent's network flaps; it redials while the host still holds the half-open old
  socket. New WS attaches, replays outbox, streams normally — yet `agents.status = "offline"`.
  Dispatch deprioritizes it (`onlineRank`), admin UI shows it offline, and any status-driven logic
  misroutes, while `sessions.isOnline` says the opposite.
- Suggested fix direction: Only write `offline` when the detach actually removed the current session
  (have `detach` return whether it evicted, or check `sessions.get(agentId)` before the status
  write). Same root cause as the P0: unguarded status transitions.
- Related: HOST-M1M2-05 reports the same root cause from the edge chapter.

### HOST-M3-03 · [P1] Sweep-marked `offline` is one-way while the socket stays connected — heartbeats never restore `online`

- Location: src/agents/liveness.ts:51-74 (`sweepLiveness`), src/agents/registry.ts:133-142 (`touchLiveness`)
- Description: The sweep flips a silent `online` agent to `offline`, and the module comment says the
  WS `onOpen` flips it back. But an agent can go silent past the grace (3 × 30 s) while its socket
  stays healthy — heartbeat starvation in the agent (long engine run, event-loop stall; cf. agent
  audit P2-16 "heartbeat is write-only"), a host stall, or clock skew. When heartbeats resume,
  `touchLiveness` bumps `last_seen`/`active_threads` but never touches `status`, and the agent has no
  reason to reconnect — so the row is stuck `offline` indefinitely on a live connection.
- Failure scenario: Agent stops heartbeating for 95 s during a heavy run, then resumes. Sweep marked
  it `offline`; every subsequent heartbeat updates `last_seen` only. The agent remains `offline` in
  the registry for hours while streaming results, degrading dispatch ranking and admin observability;
  the flap never self-heals.
- Suggested fix direction: In the heartbeat path (or `touchLiveness`), restore `status = 'online'`
  when the row is `offline` (guarded to never touch `removed`); alternatively have the sweep consult
  the in-memory `SessionRegistry` and skip agents with an attached socket.

### HOST-M3-04 · [P1] `removeAgentControl` aborts durable revocation when the launcher op fails — remove is not idempotent and can be permanently blocked

- Location: src/agents/control.ts:127-136
- Description: The launcher teardown is awaited before the durable bookkeeping; any launcher error
  propagates and `removeAgent` (token revocation + task orphaning) never runs. For a docker agent
  whose container is already gone (crashed and pruned, or manually deleted), `docker rm -f <ref>`
  exits non-zero, `run()` throws, and remove fails — on every retry, forever. There is no other
  revocation path in the API, so OD12 revocation is gated on runtime success.
- Failure scenario: A docker agent's container is deleted out-of-band. Admin calls
  `POST /admin/agents/:id/remove` → 500 from `docker rm`. The connect token stays valid at rest and
  the agent stays a permitted dispatch target; an attacker holding the (never-revocable) token keeps
  a working credential. Same shape for `stop`/`restart`: a gone container makes them hard-fail
  instead of degrading to the WS-close fallback.
- Suggested fix direction: Run the durable bookkeeping unconditionally (before, or in a `finally`
  with the launcher error reported in the result), and/or treat "no such container" as success.
  Consider falling back to the WS-close path when the launcher op fails.

### HOST-M3-05 · [P2] Pending launches live only in memory with no expiry — orphaned containers holding the register token, unreachable by OOB control

- Location: src/agents/registry.ts:274-305 (`pendingLaunches`), src/agents/control.ts:229, src/agents/launchers/docker.ts:93-119
- Description: `launchAgent` starts a container, then records `{launcherKind, launchRef}` in a
  process-local `Map` keyed by nonce, consumed only by a successful registration. There is no TTL,
  no persistence, and no reaper. If the launched agent never registers (bad image config, crash-loop,
  network fault) the container keeps running with `GINO_REGISTER_TOKEN` in its env while the host
  holds its only handle in memory; a host restart erases the map entirely, after which even a
  successful late registration cannot correlate (`correlateLaunch` returns null) and no `agents` row
  ever carries the `launch_ref`.
- Failure scenario: Host launches a container, then restarts (deploy). The container registers fine
  but binds no `launcher_kind`/`launch_ref` → `stop`/`remove` degrade to WS-close and `docker rm` is
  never issued for it; or the agent inside never comes up and the container runs forever, invisible
  to `GET /admin/agents`. Repeated failed launches accumulate unbounded map entries and containers.
- Suggested fix direction: Give pending launches a TTL with a reaper that tears down the runtime
  (`launcher.remove(ref)`) on expiry, and/or persist them (a small `pending_launches` table or a
  docker label `gino.host=<id>` enabling reconciliation by `docker ps --filter label=...`).

### HOST-M3-06 · [P2] Enrollment is not idempotent — a register retry mints a duplicate agent and the launch correlation is consumed by the ghost row

- Location: src/agents/registry.ts:54-75 (`enrollAgent`), src/agents/registry.ts:288-300 (`correlateLaunch`)
- Description: `enrollAgent` unconditionally INSERTs a new agent row per `POST /agent/register`, and
  `correlateLaunch` consumes the one-shot nonce on the first request. Registration rides HTTP with a
  retrying client (protocol §3 says the agent persists the token from the response); if the response
  is lost (timeout — the agent audit notes its HTTP client has no timeouts), the agent retries and a
  second, unrelated `agentId` is created. The launch binding (`launcher_kind`/`launch_ref`) landed on
  the first, ghost row whose connect token nobody holds.
- Failure scenario: Launched container registers; response lost in transit; agent retries. Result:
  agent row A (`enrolled` forever, holds the container's `launch_ref`, token unknown to anyone) and
  row B (the live agent, no launcher binding). OOB stop/remove of B falls back to WS-close; the
  container itself is only reachable through the dead row A. Ghost `enrolled` rows accumulate.
- Suggested fix direction: Make register idempotent per launch — key enrollment on the launch nonce
  (nonce already bound → return the existing agent/token rather than inserting), and/or accept an
  agent-supplied idempotency key. A reaper for never-connected `enrolled` rows would bound the ghosts.

### HOST-M3-07 · [P2] Remove never severs the live session — revocation is not enforced on an attached socket

- Location: src/agents/control.ts:122-139, src/agents/sessions.ts:37-104 (no force-close surface used on remove)
- Description: The connect token is verified only at WS upgrade. `removeAgentControl`'s WS-close path
  merely enqueues a `close` frame and relies on the agent to hang up voluntarily; nothing calls
  `ws.close()` on the host side or detaches the session. A non-cooperating (compromised) agent keeps
  its authenticated push/stream pipe indefinitely after revocation: it stays `sessions.isOnline`,
  keeps receiving any outbox frames still addressed to it, and its heartbeats keep updating the
  `removed` row via `touchLiveness` (which is also status-blind). Damage is bounded by the inbound
  write gate (tasks are orphaned, so store writes deny), but OD12 "revoked on remove" only holds for
  *future* connections.
- Failure scenario: Admin removes a suspected-compromised manual agent. The agent ignores the `close`
  frame. Hours later it is still connected, observing every frame the host pushes at it and keeping
  the session slot; only a network drop ends it (and the revoked token then blocks re-auth).
- Suggested fix direction: On remove, force-close from the host: `sessions.get(agentId)?.ws.close()`
  + `detach` after the bookkeeping. Optionally guard `touchLiveness` to skip `removed` rows.

### HOST-M3-08 · [P2] An `online` agent that never sent a frame is never swept offline

- Location: src/agents/liveness.ts:59-63 (`isNotNull(agents.lastSeen)` guard), wiring src/edge/agent.ts:250-256 (onOpen stamps no `last_seen`)
- Description: `onOpen` sets `status = 'online'` but does not stamp `last_seen`; only inbound frames
  do (`touchLiveness`). The sweep deliberately skips rows with `last_seen IS NULL` (meant for
  never-connected `enrolled` agents), so an agent that connects and then dies half-open before its
  first heartbeat has `status='online'`, `last_seen=NULL` — permanently outside the sweep's WHERE.
- Failure scenario: Fresh agent registers, opens the WS, and the process is SIGKILLed (or the network
  half-opens) before the first heartbeat. No clean close ever arrives. The agent shows `online`
  forever, ranks top for dispatch (`onlineRank=0`), and tasks pinned to it wait indefinitely.
- Suggested fix direction: Stamp liveness at attach time (call `touchLiveness` in `onOpen`, or have
  `setAgentStatus(...,'online')` set `last_seen = now`), which makes the `isNotNull` guard safe.

### HOST-M3-09 · [P2] Register-token holder self-declares its access policy — `access: "public"` is client-controlled at enroll

- Location: src/agents/registry.ts:64 (`access: input.access ?? "dedicated"`), wiring src/edge/agent.ts:204
- Description: The register edge forwards the request body's `access` straight into `enrollAgent`.
  The register token is a shared, long-lived bootstrap secret (OD12) — and the agent side stores and
  handles it weakly (agent audit: token on argv, cleartext `http://` accepted). Whoever presents it
  can enroll an agent that declares itself `public`, which `isOwnerPermitted` then allows for *every*
  owner with no admin grant step. OD13 access policy is thereby set by the least-trusted party in the
  exchange.
- Failure scenario: The shared register token leaks. The attacker enrolls a rogue agent with
  `access: "public"` and honest capabilities. Dispatch places any owner's next unassigned task on it
  (it is permitted and has room); the rogue agent receives the task's prompts, config entries, and
  message stream — silent interception without touching any existing agent.
- Suggested fix direction: Default enrollment to `dedicated` and ignore (or reject) a client-supplied
  `access`; make `public` an admin-only mutation (like grant/revoke), or an allowlisted property of
  the launch config for host-launched agents.

### HOST-M3-10 · [P2] Docker shell-outs have no timeout — a wedged docker daemon hangs admin control ops indefinitely

- Location: src/agents/launchers/docker.ts:54-62 (`bunSpawn`), 77-84 (`run`)
- Description: `bunSpawn` awaits `proc.exited` with no timeout or kill path, and `run` layers no
  deadline on top. Docker CLI calls block arbitrarily when the daemon is unresponsive (a common
  failure mode). Every control op (`launch`/`stop`/`restart`/`remove`/`inspect`) is awaited inside an
  admin HTTP handler, so the request — and the admin's remove/stop intent — hangs with it; `inspect`
  runs inside `GET /admin/agents/:id`, wedging plain observability reads too.
- Failure scenario: Docker daemon deadlocks. `GET /admin/agents/:id` and every
  `POST /admin/agents/:id/stop|remove` hang until the client gives up; spawned `docker` processes
  accumulate; no fallback to WS-close ever triggers because the call neither fails nor returns.
- Suggested fix direction: Enforce a per-op deadline in the spawn seam (kill the child on timeout and
  surface a distinct error); on timeout, let control degrade to the WS-close fallback.

### HOST-M3-11 · [P2] The shared register token rides the docker CLI argv and persists in the container's inspectable env

- Location: src/agents/launchers/docker.ts:104-113 (`-e GINO_REGISTER_TOKEN=<plaintext>` argv)
- Description: The launch env — including the long-lived shared `GINO_REGISTER_TOKEN` — is passed as
  `-e KEY=VALUE` command-line arguments to the docker CLI. Argv is world-readable via
  `/proc/<pid>/cmdline` for the CLI's lifetime, and docker persists the env in the container config,
  readable via `docker inspect` by anyone in the docker group for the container's entire life
  (mirrors agent audit P3 "register token as CLI flag", host side; the agent audit also flags what a
  leaked register token buys — compounded by the `access` finding above).
- Failure scenario: Any unprivileged local user polls `/proc/*/cmdline` during launches, or any
  docker-group user runs `docker inspect` on a long-lived agent container, and reads the shared
  bootstrap secret — then enrolls rogue agents at will.
- Suggested fix direction: Pass secret env without argv exposure: `--env-file` on a 0600 tempfile, or
  `-e GINO_REGISTER_TOKEN` (value-less form inherits from the CLI process env set via `Bun.spawn`'s
  `env` option). The `docker inspect` persistence argues for per-launch short-lived register tokens
  eventually.

### HOST-M3-12 · [P2] Removing an agent leaves its unacked outbox queue permanently undeliverable — and control ops keep feeding it

- Location: src/agents/control.ts:122-139 (remove does no queue cleanup), src/agents/control.ts:68-70/89/109 (closeViaWs enqueues for any agent, including `removed`)
- Description: After remove, the agent can never reconnect (token revoked), so its unacked `outbox`
  rows can never replay or ack — yet remove leaves them in place and even enqueues one more durable
  `close`. `requireAgent` in stop/restart/remove only checks existence, not `removed`, so repeated
  admin ops on a removed agent keep appending undeliverable frames (and `grantAgentOwner` silently
  accepts grants on removed agents). No GC exists for the host `outbox` (the agent-side audit found
  the same missing-GC pattern, P1-9); these rows are immortal by construction, and every reconnect
  replay for *other* agents scans a table that only grows.
- Failure scenario: Long-running deployment cycles many docker agents. Each removal strands that
  agent's queued frames plus a `close`; occasional retried stop/remove calls on removed agents add
  more. The outbox grows without bound with rows that structurally can never be acked.
- Suggested fix direction: On remove, delete (or mark dead) the agent's unacked outbox rows in the
  same transaction as the token revocation; reject or no-op control/grant ops on `removed` agents.
  A general acked/dead-row GC for `outbox` belongs with M2.

### HOST-M3-13 · [P3] Durably queued `close` frames replay regardless of staleness — a stop issued while offline kills a later deliberate start

- Location: src/agents/control.ts:68-70 (`closeViaWs` via the durable outbox)
- Description: Stop/restart fallbacks are ordinary outbox frames: persisted, replayed on reconnect,
  with no TTL or supersede. A `close {reason:"stop"}` enqueued while the agent is offline waits
  indefinitely and fires on the next connect — which may be weeks later, when the operator has
  deliberately started the agent and no longer wants it stopped. A restart-while-offline similarly
  bounces the agent one extra time on its next connect.
- Failure scenario: Admin stops an offline manual agent (queued close). A month later the operator
  intentionally starts it; it connects, replays the stale close, and immediately shuts down —
  looking like a crash loop.
- Suggested fix direction: Give control frames a TTL (skip on replay past it) or supersede semantics
  (a newer start/restart intent cancels queued closes); at minimum log loudly when a stale control
  frame replays.

### HOST-M3-14 · [P3] Sweep leaves the zombie socket attached — `sessions.isOnline` and `agents.status` diverge

- Location: src/agents/liveness.ts:51-74 (sweep updates the DB only)
- Description: When the sweep flips a half-open agent to `offline`, the dead socket stays in the
  `SessionRegistry`: `isOnline` keeps answering true, the admin list renders both fields
  contradictorily, and `enqueue` keeps "successfully" pushing frames into a dead TCP buffer (safe for
  delivery — rows stay unacked and replay on reconnect — but misleading). The zombie is only evicted
  by the agent's own reconnect superseding it.
- Failure scenario: Agent dies half-open; sweep marks it `offline`; `GET /admin/agents` shows
  `status: offline, online: true` for hours, and pushed frames appear sent while nothing is
  listening.
- Suggested fix direction: Let the sweep (or its caller) close+detach the swept agent's session
  (`sessions.get(id)?.ws.close()`), converging the two views; requires handing the sweep the session
  registry, which its deps seam already accommodates.

### HOST-M3-15 · [P3] Dead/unwired registry surface: `Session.connectTokenHash` never read, `hasPendingLaunch` uncalled, `registerTokenRef` never populated

- Location: src/agents/sessions.ts:27 (stored at attach, read nowhere), src/agents/registry.ts:303 (`hasPendingLaunch`), src/agents/registry.ts:66 (`registerTokenRef`)
- Description: Three vestigial pieces. (1) Sessions store `connectTokenHash` per the architecture's
  session shape but nothing consumes it — it was the natural vehicle for enforcing revocation on live
  sessions (see the P2 above). (2) `hasPendingLaunch` has no production caller in `src/`. (3)
  `enrollAgent` accepts `registerTokenRef` ("which shared register token enrolled it — audit"), but
  the register edge never passes it, so the audit column is always NULL and the documented
  provenance trail does not exist.
- Failure scenario: Not a runtime fault; the cost is a misleading contract — an operator rotating
  register tokens cannot attribute enrollments to a token generation, and readers assume live-session
  token checks that do not happen.
- Suggested fix direction: Wire `registerTokenRef` from the register edge (or drop the parameter);
  either use `connectTokenHash` for live revocation checks or remove the field; delete or test-scope
  `hasPendingLaunch`.

*Chapter summary: P0=1 P1=3 P2=8 P3=3 (15 findings).*

---
