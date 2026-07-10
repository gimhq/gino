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

## Chapter M4 — Task Management & Dispatch (`src/tasks/`)

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
  bug. The *documented* per-task cursor, however, does not exist — see HOST-M4-07.
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

### HOST-M4-01 · [P0] Per-task `close` emits the runner-level exit frame, killing every other task on the agent

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
- Related: HOST-M1M2-03 reports the same `close`/`interrupt` scoping break from the edge chapter.

### HOST-M4-02 · [P1] Cron created before the first message is silently lost — task never seeded (e2e KNOWN ISSUE 3, mandatory check — confirmed)

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

### HOST-M4-03 · [P1] `interrupt` is not task-scoped — it cancels whatever turn is currently running, and stale interrupts replay on reconnect

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
- Related: HOST-M1M2-03 (edge-side view of the same scoping break).

### HOST-M4-04 · [P1] Thread-identity contract inverted: host mints thread ids and sends "continue" for threads the agent has never seen; agent-minted ids are never recorded

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
- Related: HOST-M1M2-02 reports the same inversion from the edge/emission side.

### HOST-M4-05 · [P1] Orphaned task can be re-placed on a new agent, but is never re-seeded and drags a dead `current_thread_id` along

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

### HOST-M4-06 · [P2] `assignTaskToAgent` is not idempotent on status: re-assigning the same agent resurrects a closed task to `active`

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

### HOST-M4-07 · [P2] The documented per-task delivery cursor does not exist, and the `message` `msgId` is assigned non-atomically

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
- Related: HOST-M1M2-07 (same non-atomic msgId, edge chapter). The cross-cutting Lens C re-reports both the cursor TODO and the crash window as dedupes.

### HOST-M4-08 · [P2] No garbage collection for the host outbox; rows for removed agents are unackable forever

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
- Related: HOST-M1M2-08 and HOST-M9-07 report the same gap; cross-cutting Lens C dedupes here.

### HOST-M4-09 · [P2] A never-placed task can never be closed — `close`/`interrupt` 409 before the status write, and no other status route exists

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

### HOST-M4-10 · [P3] Frame-catalog drift: `task_upsert` / `cron_upsert` payloads sent by M4 are not what protocol §5 documents

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

### HOST-M4-11 · [P3] Cron update/delete accept arbitrary `cronId`s with no existence check — a typo'd PUT silently creates a second cron

- Location: src/tasks/orchestrator.ts:175 (`input.cronId ?? ulid()` — caller id accepted verbatim); src/edge/admin/tasks.ts:310-325 (PUT route passes the path param straight through)
- Description: `PUT /tasks/:id/crons/:cronId` never checks the cron mirror (or
  anything else) for the id; because `cron_upsert` is an upsert on the agent,
  an unknown id creates a new cron rather than failing. The update endpoint
  therefore doubles as create-with-arbitrary-id, and a mistyped/stale id
  silently forks a duplicate schedule instead of returning 404. DELETE has the
  same shape (harmless no-op agent-side, but also no 404).
- Failure scenario: client updates a cron using a stale id (e.g. one lost to
  HOST-M4-02, or a copy/paste error) → 200 OK → the old cron
  keeps firing unchanged and a second cron with the typo'd id starts firing
  too.
- Suggested fix direction: on PUT/DELETE, require the `cronId` to exist in the
  `crons` mirror (404 otherwise), accepting the mirror's eventual-consistency
  lag as the documented caveat.

*Chapter summary: P0=1 P1=4 P2=4 P3=2 (11 findings).*

---

## Chapter M5/M6 — Message Store & Channels (`src/msgstore/**`, `src/channels/**`)

Scope: every line of `src/msgstore/{index,messages,reads,thread-runs,crons,attachments}.ts`
and `src/channels/{index,registry,ingress,egress,webhooks}.ts`, read against
`docs/host-architecture.md` (M5/M6/§8) and `docs/protocol.md` (§4–6). Callers were read for
context (`src/protocol/inbound.ts`, `src/edge/messages.ts`, `src/tasks/orchestrator.ts`,
`src/proxy/provider.ts`) but not themselves audited.

Ground-truth checks that PASSED (no finding, recorded so a re-audit need not re-derive them):

- **`(thread_id, seq)` dedup is race-free.** `writeThreadMessage` is a single
  `INSERT … ON CONFLICT (thread_id, seq) DO NOTHING` on the composite primary key
  (`messages.ts:27-42`) — no check-then-insert window; at-least-once replay is a true
  first-writer-wins no-op. Same pattern is correct for `writeAttachmentDescriptors` (id),
  `writeThreadResult` (UPSERT on `thread_id`), `writeCronState` (UPSERT on cron id).
- **Cross-task write authorization** (`inbound.ts:67-97`) correctly blocks a placed agent
  from poisoning another task's transcript / cron mirror via a forged `(thread_id, seq)` or
  cron id.
- **Egress IDOR guard** (`egress.ts:35-46`, `tailTask`/`pollChannel`) refuses a caller-supplied
  `taskId` not bound to the channel and not owned by the channel owner.

### HOST-M5M6-01 · [P1] Poll egress advances the cursor on read, before the client receives the bytes (at-most-once → message loss)

- Location: `src/channels/egress.ts:215` (`pollChannel` advance) + `src/edge/messages.ts:157,181` (default `peek=false`)
- Description: `pollChannel` calls `advanceCursor(...)` as soon as it has *read* a batch
  (non-`peek`), and the DB cursor commit happens before the HTTP response is serialized and
  flushed. The `GET /:channel/poll` handler defaults `peek` to `false`, so every ordinary poll
  advances the stored `channel_cursors.last_seq` regardless of whether the caller ever received
  the payload. This directly contradicts the M6 invariant (`host-architecture.md` §M6/§8: "a
  delivery failure or host restart just re-reads from the store; nothing is lost in flight").
- Failure scenario: a channel client `GET`s `/poll`; `pollChannel` reads rows up to rowid 100
  and commits the cursor to 100; the socket write then fails (client disconnect, proxy timeout,
  process crash mid-flush). The client never received messages 1..100, but the next poll reads
  `rowid > 100` and those messages are gone forever from the channel's view. The webhook path
  (`pushChannelWebhooks`) correctly advances only after a 2xx; the poll path does not.
- Suggested fix direction: make the default poll non-destructive (peek + explicit
  client-acknowledged `after`), or advance the cursor only on a confirmed-received signal;
  align the poll path with the webhook path's "advance after delivery" contract.

### HOST-M5M6-02 · [P1] Webhook SSRF guard is bypassable (IPv4-mapped IPv6, `0.0.0.0`, alternate literal encodings)

- Location: `src/channels/webhooks.ts:88,104-118` (relies on `isPrivateUpstreamHost`, `src/proxy/provider.ts:133-160`)
- Description: the webhook SSRF guard (mandatory area 3) delegates the private-range decision to
  `isPrivateUpstreamHost`, which only recognizes dotted-decimal IPv4 and a narrow IPv6 set
  (`::1`, `fc/fd`, `fe8-feb`). It does not classify **IPv4-mapped IPv6** (`::ffff:127.0.0.1`,
  `::ffff:169.254.169.254`), the IPv4 unspecified/`0.0.0.0/8`, `100.64.0.0/10` (CGNAT), the IPv6
  unspecified `::`, nor decimal/octal/hex-encoded IPv4. For an IPv6-literal host, `deliverWebhook`
  additionally *skips DNS resolution* (`webhooks.ts:107-109`), so the delivery-time guard adds
  nothing over the incomplete static check.
- Failure scenario: a webhook registered with `http://[::ffff:169.254.169.254]/` (cloud metadata)
  or `http://0.0.0.0:<port>/` passes `assertSafeWebhookUrl` at registration and
  `assertSafeWebhookUrlResolved` at delivery — `isIP` returns 6/4, `isPrivateUpstreamHost` returns
  `false` — and the request is issued to a loopback/link-local/metadata endpoint. The security
  control that exists specifically to stop this is defeated.
- Suggested fix direction: normalize to a canonical address before classifying (unwrap
  `::ffff:` mapped form to its IPv4, reject `0.0.0.0/8`, `100.64.0.0/10`, `::`, and reserved
  ranges); prefer a vetted IP-range library and fail closed on anything not provably public.
  (Note: currently latent — see HOST-M5M6-04 "webhook egress unwired" — but the guard is the
  security boundary and is broken today.)

### HOST-M5M6-03 · [P2] Egress cursor persists the SQLite `rowid`, which is not stable across `VACUUM`

- Location: `src/channels/egress.ts:122-126,139` (cursor = `rowid`) + `channel_cursors` schema
- Description: the per-`(channel, task)` cursor stores `messages.rowid` as its high-water mark
  and tails `WHERE rowid > last_seq`. `messages` has a composite primary key
  (`PRIMARY KEY (thread_id, seq)`) and is **not** declared `WITHOUT ROWID`, so `rowid` is an
  implicit, non-aliased column. Per SQLite semantics, `VACUUM` (and `auto_vacuum` compaction) may
  renumber rowids of any table lacking an `INTEGER PRIMARY KEY` alias. The stored cursors then
  point at the wrong positions.
- Failure scenario: an operator (or auto-vacuum) VACUUMs the DB. Every `channel_cursors.last_seq`
  now refers to a different logical row; depending on renumbering, channels either re-deliver a
  large historical backlog (duplicate storm) or skip messages permanently (loss). The whole
  distribution layer's correctness silently depends on rowids never being renumbered.
- Suggested fix direction: do not tie a durable cursor to `rowid`. Add an explicit monotonic
  insertion column (e.g. an `AUTOINCREMENT` store-sequence, or an `INTEGER PRIMARY KEY` surrogate)
  and cursor on that; or declare the table so its key is a stable rowid alias.

### HOST-M5M6-04 · [P2] Outbound webhook egress is never invoked (unwired feature / dead path)

- Location: `src/channels/egress.ts:245` (`pushChannelWebhooks`), `src/channels/webhooks.ts:153` (`deliverWebhook`)
- Description: `pushChannelWebhooks` — the only driver of `type: "webhook"` channel delivery — has
  no production caller. It is referenced only by the `index.ts` re-export and tests; no scheduler,
  interval, or edge route triggers it (verified: no `setInterval`/timer or handler calls it
  outside `src/channels/**`). Webhook channels can be registered (`addWebhook`) and store
  endpoints, but nothing ever pushes to them.
- Failure scenario: an operator registers a `webhook` channel and endpoints, expecting push
  delivery. Transcript events accumulate in `messages`, the cursor never moves, and the receiver
  is never called — a silently non-functional integration path. The elaborate SSRF/redirect
  hardening in `webhooks.ts` guards code that never runs.
- Suggested fix direction: decide explicitly — wire `pushChannelWebhooks` into an egress loop
  (post-store hook or interval) or remove the webhook egress subsystem until it is wired, so its
  security surface is not carried as latent dead code.

### HOST-M5M6-05 · [P2] Webhook delivery has no timeout and a poison message blocks a task's egress forever

- Location: `src/channels/webhooks.ts:170` (no `AbortSignal`/timeout on `fetchImpl`) + `src/channels/egress.ts:257-278`
- Description: (a) `deliverWebhook` issues `fetchImpl(...)` with no timeout or abort path, so a
  slow/hung endpoint stalls the `await` indefinitely; the push loop delivers endpoints
  sequentially, so one hung endpoint blocks the whole channel push. (b) The cursor advances only
  when **every** endpoint returned 2xx (`egress.ts:276`). A single permanently-failing endpoint
  (a poison message the receiver 4xxs, or a dead endpoint) freezes the `(channel, task)` cursor
  forever and re-reads the identical batch every cycle — head-of-line blocking with no
  dead-letter / max-attempts. Healthy endpoints in the same channel receive the batch again on
  every retry (duplicate fan-out).
- Failure scenario: a channel has endpoints A (healthy) and B (returns 500). Every push cycle:
  A re-receives the same messages, B fails, cursor stays put. The task's transcript never
  progresses past the poison batch, and A is spammed with duplicates indefinitely.
- Suggested fix direction: add a per-request timeout (AbortSignal); track a per-endpoint cursor
  or bounded attempt budget with a dead-letter/skip policy so one bad endpoint cannot wedge the
  task or duplicate-flood the healthy ones.

### HOST-M5M6-06 · [P2] `thread_runs.cron_id` is never populated → host cannot derive cron dispatch counts

- Location: `src/protocol/inbound.ts:132` (hardcoded `source: "chat"`, no `cronId`) + `src/msgstore/thread-runs.ts:38-54`
- Description: `writeThreadResult` accepts `source`/`cronId` as dispatch context, but the sole
  caller (`inbound.ts:132`) hardcodes `source: "chat"` and never supplies `cronId` — the comment
  even says "M4/B3 supplies real source/cronId from dispatch," which does not happen. The
  `thread_result` frame carries only `taskId`/`threadId` (protocol §5), and the inbound handler
  does no thread→cron lookup, so `thread_runs.cron_id` is always NULL and `source` always
  `"chat"`, even for cron fires.
- Failure scenario: `host-architecture.md` §M5 states "host derives *dispatched* counts from
  `thread_runs` by `cron_id`." Because `cron_id` is never written, any such query returns zero;
  cron run accounting / observability is silently broken, and every cron-originated run is
  mislabeled `source='chat'`.
- Suggested fix direction: resolve the owning cron for a thread at inbound time (e.g. record the
  thread→cron mapping when the cron dispatch opens the thread, or persist it on the run row) and
  pass real `source`/`cronId` into `writeThreadResult`.
- Related: HOST-M1M2-06 reports the same gap from the inbound-handler side.

### HOST-M5M6-07 · [P2] DNS-rebinding TOCTOU: the SSRF guard resolves DNS, then `fetch` re-resolves independently (no IP pinning)

- Location: `src/channels/webhooks.ts:162,170,195`
- Description: `assertSafeWebhookUrlResolved` resolves the hostname and validates the answers, but
  `deliverWebhook` then calls `fetchImpl(target.toString())`, which performs its **own** DNS
  resolution when it actually connects. The validated address is never pinned, so a fast-rebinding
  DNS server can return a public A record to the guard and a private one to `fetch`. The same gap
  applies to each re-validated redirect `Location`.
- Failure scenario: attacker controls `evil.example`. The guard's resolver sees `93.184.x.x`
  (public → passes); microseconds later `fetch`'s resolver sees `169.254.169.254` and connects
  there. The DNS check is decorative against an active rebinding attacker.
- Suggested fix direction: pin the validated IP — resolve once, then connect to that literal IP
  (custom `lookup`/agent or dispatcher) with the original Host header — so validation and
  connection target the same address.

### HOST-M5M6-08 · [P2] No retention/GC and unbounded read fan-out on the transcript store

- Location: `src/msgstore/messages.ts:45-52` (`getTranscript`), `src/channels/egress.ts:101-141` (`tailTask` with no default limit), `src/msgstore/attachments.ts`
- Description: mirroring the agent-side "no-queue-GC" finding on the host: `messages`,
  `thread_runs`, and `attachments` have no delete/retention path anywhere in scope, and reads are
  unbounded — `getTranscript` selects an entire thread with no limit, and `tailTask`/`pollChannel`
  return the entire post-cursor backlog when the caller omits `limit`. Attachment descriptors
  accumulate with no orphan GC even after their referencing rows age out.
- Failure scenario: a long-lived conversation or a high-frequency cron produces a very large
  thread; a first-time poll (cursor 0) with no `limit` materializes the whole backlog into one
  response/allocation, and `getTranscript` loads the full thread into memory. Storage grows
  monotonically with no reclamation.
- Suggested fix direction: enforce a default/max batch limit on tail and transcript reads; add a
  retention/GC policy (and orphan-attachment sweep) with the unified attachment/config store GC
  the architecture already describes.

### HOST-M5M6-09 · [P3] `crons.prompt` mirror is permanently empty

- Location: `src/msgstore/crons.ts:63-66`
- Description: `writeCronState` seeds `prompt: ""` on first mirror and deliberately excludes it
  from the conflict set (preserved thereafter). The comment says `prompt` is "owned by the
  dispatch side," but the dispatch side (`orchestrator.emitCronUpsert`) sends a command and never
  writes the `crons` table (M5 is the sole writer). So `crons.prompt` is `""` forever.
- Failure scenario: the admin `/admin/tasks/:id/crons` listing shows every cron with an empty
  prompt; there is no host-visible record of what a cron actually runs.
- Suggested fix direction: decide the owner of `prompt` in the mirror — either have the M4 cron
  dispatch persist it alongside emitting the command, or drop the column from the mirror if the
  wire is authoritative.

### HOST-M5M6-10 · [P3] Egress delivers out of `seq` order under reconnect/replay (orders by arrival rowid)

- Location: `src/channels/egress.ts:124-126`
- Description: egress orders and tails strictly by `rowid` (insertion/arrival order). Protocol §6
  defines the transcript order as `seq`, never arrival. Within one live WS this coincides, but
  outbox replay after a reconnect can insert a lower-`seq` frame *after* a higher-`seq` frame
  already stored, giving it a higher rowid.
- Failure scenario: `seq=5` is stored (rowid 100, delivered), then a reconnect replays the unacked
  `seq=3` (rowid 101); egress delivers `seq=3` after `seq=5`, so the channel consumer sees the
  transcript out of order (not lost, but reordered vs the §6 contract).
- Suggested fix direction: if channel consumers must observe `seq` order, tail/deliver ordered by
  `(thread_id, seq)` (or reconcile within a thread) rather than by raw insertion rowid.
- Related: the cross-cutting Lens C re-reports this as a dedupe; this module entry is canonical.

### HOST-M5M6-11 · [P3] Webhook secret and transcript are re-sent to a redirect `Location` target

- Location: `src/channels/webhooks.ts:175,195`
- Description: on a 3xx, `deliverWebhook` re-validates the `Location` for SSRF (public-only) and
  re-issues the POST — including `x-webhook-secret` and the full transcript payload — to the
  redirect target. The target is only checked for being public, not for being the originally
  registered endpoint.
- Failure scenario: a registered endpoint 302-redirects to `https://attacker.example/`; the guard
  allows it (public), and the shared secret plus transcript contents are delivered to the
  attacker-controlled host.
- Suggested fix direction: do not forward the secret (and reconsider forwarding the body) across a
  cross-origin redirect; restrict redirects to the same origin/host as the registered endpoint, or
  drop redirect-following entirely for webhook delivery.

*Chapter summary: P0=0 P1=2 P2=6 P3=3 (11 findings).*

---
