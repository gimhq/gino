# Host Audit — Cross-Cutting Lenses (UTC discipline · Auth surface · At-least-once/idempotency)

Scope: all of `src/**` (~90 files) read against `docs/protocol.md` §4–6 and
`docs/host-architecture.md` §8. This section covers only the three cross-cutting
lenses and end-to-end composition; per-file module bugs are other auditors' remit.
Ground truth: the host stores all times as epoch-ms integers (UTC), routes by the
envelope, and must be idempotent everywhere (at-least-once both ways).

---

## Lens A — UTC discipline

Requirement: all times UTC end to end; the API layer only *declares* times as UTC;
no timezone-conversion logic anywhere in `src/**`.

Greps run and their result (every hit inspected):

- `toLocale` (toLocaleString/DateString/TimeString) — **clean, 0 hits.**
- `getTimezoneOffset` — **clean, 0 hits.**
- `DateTimeFormat` / `Intl.` — **clean, 0 hits.**
- `localtime` / `strftime` / `datetime(` / `CURRENT_TIMESTAMP` / `unixepoch` /
  `julianday` (SQL defaults, `src/**` + `drizzle/0000_smiling_prism.sql`) —
  **clean.** The only SQL column defaults are `integer DEFAULT 0`; every
  timestamp column is `integer(...).$defaultFn(() => Date.now())` (epoch-ms UTC).
- `getHours` / `getMinutes` / `getDate(` / `getMonth` / `getFullYear` / `setHours`
  — **clean, 0 hits.**
- `timezone` / `timeZone` / `tzOffset` / `offset` (date-related) — **clean.** The
  `offset` hits are cursor/byte offsets, not time.
- `new Date(` — one hit, `src/build-info.ts:10`, `new Date().toISOString()` (UTC,
  compliant).
- `.toString()` — three hits (`proxy/credentials.ts:110` URLSearchParams body,
  `channels/webhooks.ts:170,195` URL) — none are `Date` renders.
- `Date.now` / `toISOString` — all epoch-ms integer writes or the one `toISOString`
  above; no raw `Date` serialized into an API response (`serializeTask` /
  `serializeAgent` / `serializeCron` emit the integer columns directly).

**Violation list (exhaustive): 1 finding.**

### [P2] `next_run` parsed with `Date.parse`, so a timezone-less ISO string is read as LOCAL time
- Location: `src/msgstore/crons.ts:34`
- Description: `toEpochMs` coerces the wire `next_run` string to epoch-ms. A pure
  digit string is handled explicitly, but any non-numeric value falls to
  `Date.parse(value)`. Per ECMAScript, an ISO-8601 date-time **without** a timezone
  designator (e.g. `2026-07-10T09:00:00`) is interpreted in the host's **local**
  time zone, not UTC. This is exactly the implicit local-time conversion the lens
  forbids: the stored mirror value is shifted by the host machine's UTC offset.
- Failure scenario: agent (or admin round-trip) sends `cron_state.next_run =
  "2026-07-10T09:00:00"`; host runs in `TZ=America/New_York`; the mirror stores an
  epoch-ms value 4–5 hours off. Any operator view of "next run" is wrong, and the
  value silently changes meaning if the host's `TZ` changes.
- Suggested fix direction: require an explicit-UTC form on the wire (epoch-ms or a
  `Z`/offset-qualified ISO string) and reject/normalize a bare local datetime;
  treat a TZ-less string as UTC by appending `Z` before parsing, rather than
  deferring to the platform local zone.

---

## Lens B — Auth surface inventory

| route prefix | auth mechanism | timing-safe compare? | token stored hashed? | revocable? | rate-limited? |
|---|---|---|---|---|---|
| `POST /agent/register` | shared register token (body `registerToken`, or Bearer) via `requireSecret`→`safeEqual` | yes (`safeEqual`) | n/a (shared secret, compared to config plaintext) | rotate config only | **no** (see P2) |
| `GET /agent/ws/:agentId` | per-agent connect token `x-internal-token` → `verifyConnectToken` (hash match) + path-id must equal token's agent | yes (`verifyToken`/`safeEqual`) | yes (`connect_token_hash`) | yes (revoked on remove) | no (WS) |
| `GET /agent/config/:agentId`, `POST /agent/output/:agentId` | connect token, `verifyConnectToken` | yes | yes | yes | no |
| `GET/POST /agent/attachments[/:id]` | connect token via `connectTokenAuth` → `agentId` = token subject; authz by task placement / desired-set | yes | yes | yes | no |
| `/admin/*` (agents, tasks, crons, channels, config) | channel token (Bearer) via `channelTokenAuth`→`safeEqual` | yes | no (compared to config plaintext) | rotate config only | no |
| `/messages/:channel/{send,webhook,poll}` | **same channel token** as `/admin/*` | yes | no | rotate config only | no |
| `POST /system/lode/{restart,update,rollback,hold}` | channel token (per-route) | yes | no | rotate config only | no |
| `GET /health`, `/health/ready`, `/system/version`, `/api/docs`, `/openapi.json` | none (public, by design) | n/a | n/a | n/a | no |
| `/llm/*` (proxy) | proxy credential: real `sk-ant-*` key **passthrough**, or static key (`safeEqual`), or per-agent connect token (hash match) | mixed (see P0) | connect-token path hashed; static-key path `safeEqual` | connect-token path revocable | no |

Findings:

### [P0] `/llm/*` proxy auth is bypassed by any `x-api-key: sk-ant-…` prefix, then injects the host's real upstream credentials
- Location: `src/proxy/provider.ts:79` (`isAnthropicTempKey`) gating
  `src/proxy/auth.ts:31` (`verifyProxyCredential`); exploited in
  `src/proxy/handler.ts:62` (bridge) and `src/proxy/handler.ts:124` (OAuth inject).
- Description: `verifyProxyCredential` returns `true` immediately when
  `isAnthropicTempKey(headers)` is true, and that helper checks only that
  `x-api-key` **starts with** `sk-ant-` and not with `sk-ant-proxy-`. There is no
  validation that the value is a genuine Anthropic key. So any unauthenticated
  caller who can reach `/llm/*` and sets `x-api-key: sk-ant-anything` passes the
  gate. Downstream, two paths then apply the host's *real* credentials rather than
  forwarding the caller's bogus key:
  (a) `POST /llm/v1/messages` with a body model that maps to OpenAI routes into
  `bridgeClaudeToOpenAI`, which calls `store.getOpenAI()` and injects the host's
  real OpenAI/Codex OAuth token (`handler.ts:62`, `bridge.ts:62`);
  (b) `/llm/api/oauth/*` injects the host's real Anthropic OAuth token / API key
  (`handler.ts:124`).
- Failure scenario: an attacker with only network reach to the host (no enrolled
  agent, no connect token, no static key) sends
  `POST /llm/v1/messages` `x-api-key: sk-ant-x` with `{"model":"<openai-mapped-alias>", …}`
  → auth passes → the bridge spends the host's OpenAI/Codex subscription; or
  `/llm/api/oauth/…` with the same header → the host's real Anthropic OAuth token is
  attached to an attacker-shaped upstream request and the response returned to the
  attacker. The connect-token gate that is supposed to bind proxy use to an enrolled
  agent is fully bypassed.
- Suggested fix direction: do not treat a bare `sk-ant-` prefix as authenticated.
  Require the caller's credential to validate against a stored `connect_token_hash`
  or the static key before any request is admitted; if a genuine passthrough key is
  a real feature, it must be an *exact* configured value compared constant-time, not
  a prefix, and passthrough requests must never trigger host-credential injection.

### [P2] `POST /agent/register` has no rate limiting; the rate-limit middleware is unused dead code
- Location: `src/edge/agent.ts:184` (register route, no limiter);
  `src/shared/middleware/rate-limit.ts` (entire module — `rateLimit` has **zero**
  call sites in `src/**` or `e2e/**`).
- Description: protocol §3 lists `rate_limited` as a `register_error` reason and the
  host advertises per-IP throttling, but the fully-built `rateLimit` middleware is
  never mounted on any route. Enrollment (a shared long-lived bootstrap secret) is
  therefore brute-forceable with no host-side throttle, and no route in the app is
  rate-limited at all.
- Failure scenario: an attacker guesses `GINO_REGISTER_TOKEN` at line rate — nothing
  slows it; a successful guess mints a connect token and enrolls a rogue agent. The
  `rate_limited` register_error is never produced, so clients cannot observe a limit
  either.
- Suggested fix direction: mount `rateLimit` on `/agent/register` (and other
  unauthenticated/secret-checking edges) keyed by resolved client IP; emit
  `register_error{rate_limited}` on trip. If throttling is deliberately deferred,
  delete the dead middleware so it does not read as wired.

### [P2] No privilege separation: the channel token grants both user message I/O and full admin/lode control
- Location: `src/shared/middleware/auth.ts:85` (`channelTokenAuth`), applied to
  `/admin/*` (`src/edge/admin/index.ts:20`), `/messages/*`
  (`src/edge/messages.ts:84`), and `/system/lode/*` (`src/modules/system/system.routes.ts`).
- Description: the single `HTTP_CHANNEL_TOKEN` authorizes both the user-facing
  channel I/O surface (`/messages/:channel/send|webhook|poll`) and the full control
  plane (agent stop/restart/**remove**, task create/assign, cron commands, lode
  update/rollback/restart/hold). Any integration handed a channel token to push
  messages therefore also holds destructive admin authority.
- Failure scenario: an external chat/webhook integration is provisioned with the
  channel token to call `/messages/*`; that same credential can `POST
  /admin/agents/:id/remove` (revoking connect tokens, orphaning tasks) or
  `POST /system/lode/update` to switch host versions.
- Suggested fix direction: split the control-plane bearer from the channel/user I/O
  bearer (distinct secrets, distinct middleware), so a channel credential cannot
  reach `/admin/*` or `/system/lode/*`.

### [P3] `register_error` reason taxonomy is never emitted (only `unsupported_protocol`)
- Location: `src/edge/agent.ts:196` (returns `unsupported_protocol`);
  auth failure path returns a generic 401 (`shared/middleware/auth.ts:54`).
- Description: protocol §3 defines `register_error` reasons
  `invalid_register_token | rate_limited | invalid_body | registration_disabled |
  unsupported_protocol`. Only `unsupported_protocol` is ever produced; a bad
  register token yields a bare `UnauthorizedError` 401 envelope, not
  `register_error{invalid_register_token}`, and `invalid_body` /
  `registration_disabled` are unreachable. Agents cannot distinguish the cause.
- Suggested fix direction: return the structured `register_error` body with the
  correct reason for each failure class in the register handler.

### [P3] `isSpoofableProxyConfig` is defined but never called, so the documented startup warning never fires
- Location: `src/shared/lib/client-ip.ts:80` (no callers).
- Description: the helper's own contract says "The app should log a startup warning
  when this is true." With `TRUST_PROXY=true` and an empty `TRUSTED_PROXY_IPS`, any
  direct caller can forge `X-Forwarded-For` and defeat IP-keyed logic; nothing warns
  the operator. (Impact is currently limited because no route is IP-rate-limited —
  see the P2 above — but the guard is silently inert.)
- Suggested fix direction: call `isSpoofableProxyConfig` during `bootstrap` and log
  a warning, or remove the dead helper.

---

## Lens C — At-least-once / idempotency end-to-end trace

Trace M4 orchestrator → M2 outbox → WS → agent → inbound frames → M5 store →
M6 egress. Per-invariant implementation map (protocol §4–6, host §8):

| invariant (protocol §) | implementing code | verdict |
|---|---|---|
| persist-before-send (host→agent) | `src/protocol/outbox.ts:46` `enqueue` inserts row then sends | OK |
| unacked replay on reconnect | `src/protocol/outbox.ts:96` `replayUnacked` (on WS `onOpen`, `edge/agent.ts:254`) | OK |
| `ack{id}` stops replay | `src/protocol/outbox.ts:74` `ackOutbox`; `inbound.ts:162` | OK |
| `message` uses `msgId` + `msg_ack` | `outbox.ts:62` sets `msgId`; `inbound.ts:155` `ackMessage` | partial — see P3 (cursor unimplemented) |
| demux by `threadId`, order by `seq` (transcript) | `msgstore/messages.ts:45` `ORDER BY seq` | OK (transcript); egress differs — see P2 |
| idempotent write on `(threadId, seq)` | `msgstore/messages.ts:40` `onConflictDoNothing([threadId,seq])` | OK |
| `thread_result` idempotent (per thread) | `msgstore/thread-runs.ts:55` UPSERT on `threadId` | OK |
| `cron_state` idempotent mirror | `msgstore/crons.ts:69` UPSERT on `id` | OK |
| completeness via `final_seq` (received `1..=final_seq`) | stored at `thread-runs.ts:52`; **no verifier** | MISSING — see P2 |
| cross-agent/cross-task write guard | `protocol/inbound.ts:67` `authorizeWrite` (WS + HTTP fallback) | OK |

### [P2] `final_seq` completeness is never verified — a permanently missing middle `seq` goes undetected
- Location: written at `src/msgstore/thread-runs.ts:52`; no consumer anywhere
  (`final_seq`/`finalSeq` appears only in the schema and the writer).
- Description: protocol §6 states the host uses `thread_result.final_seq` to "verify
  it received `1..=final_seq`". The value is stored but nothing ever compares it
  against the transcript row count/max seq. Because a store-write failure causes the
  frame *not* to be acked (`inbound.ts:190`) and relies on agent replay, a
  transcript with a permanent hole (agent GC/loss, or the P3 undeliverable-message
  edge) is silently accepted as complete.
- Failure scenario: thread emits seq 1..10, `final_seq=10`; seq 7's frame is dropped
  and never replayed. The host stores 9 rows, `thread_runs.final_seq=10`, and no
  code notices the gap; egress delivers an incomplete conversation as if whole.
- Suggested fix direction: on `thread_result`, compare `final_seq` to the stored
  `(thread_id)` seq set (count/`MAX(seq)` and gap check) and surface an incomplete
  thread (metric/log/flag) rather than treating terminal receipt as completeness.

### [P2] Egress orders a thread by store `rowid`, not `seq`, so a late/replayed frame is delivered out of `seq` order
- Location: `src/channels/egress.ts:122-126` (`position: sql\`rowid\``, `rowid >
  ${from}`, `ORDER BY rowid`).
- Description: the egress cursor is the `messages.rowid` insertion position, and the
  comment acknowledges `rowid` ≠ per-thread `seq`. At-least-once means a lower-`seq`
  frame can be **inserted later** (higher rowid) than a higher-`seq` frame from the
  same thread (interleave/replay after a drop). Egress then delivers the higher seq
  first and the recovered lower seq afterward, violating protocol §6 "order each
  thread by `seq`" for the distributed stream (the stored transcript read at
  `messages.ts:45` is correctly seq-ordered; only distribution is affected).
- Failure scenario: seq 5 stores at rowid 100 and is polled/delivered (cursor→100);
  seq 3, delayed, replays and inserts at rowid 101; next poll delivers seq 3 after
  seq 5. A webhook/poll consumer sees the thread out of order.
- Suggested fix direction: order/deliver per `(thread_id, seq)` within a task, or
  hold back a thread's tail until its seq run is contiguous, rather than trusting
  arrival `rowid` as the per-thread order.

### [P2] Host outbox has no GC and no index on the replay predicate — unbounded growth + full-scan on every reconnect
- Location: `src/protocol/outbox.ts:96` (`WHERE agent_id = ? AND acked_at IS NULL`);
  `src/db/schema.ts` `outbox` table (no index on `agent_id`/`acked_at`; no DELETE
  path exists for `outbox`).
- Description: acked outbox rows are never deleted (only the config store GC exists),
  and the replay query has no supporting index, so `replayUnacked` scans an
  ever-growing table on each reconnect. This mirrors the agent-side audit's P1-9 on
  the host: every host→agent frame (a `message`/`task_upsert`/`cron_upsert` per unit
  of work) persists forever.
- Failure scenario: a long-lived deployment with frequent reconnects accumulates
  millions of acked rows; each agent reconnect full-scans them under the DB, and the
  table grows without bound.
- Suggested fix direction: add a retention DELETE for acked rows past a window, and a
  partial index on `(agent_id) WHERE acked_at IS NULL` (or `(agent_id, acked_at)`).

### [P3] `msg_ack` marks the row delivered but the M4 delivery-cursor advance is unimplemented (TODO)
- Location: `src/protocol/outbox.ts:83` `ackMessage` (`TODO(M4/B3)`); §5 maps
  `msg_ack → M4 cursor`.
- Description: `msg_ack` only sets `acked_at` to stop replay; there is no per-task
  delivery cursor advanced on durable-store confirmation as the architecture
  describes. At-least-once still holds (unacked → replay, agent dedupes), so this is
  a completeness/observability gap rather than a data-loss, but the documented cursor
  semantics are absent.
- Suggested fix direction: either implement the per-task delivery cursor advance on
  `msg_ack`, or update the spec/comment to state the acked flag is the cursor.

### [P3] A crash between the `message` insert and the `msgId` back-fill yields an undeliverable, infinitely-replayed frame
- Location: `src/protocol/outbox.ts:62-65` (insert, then a **separate** UPDATE sets
  `msgId = id`); encode at `src/protocol/envelope.ts:89-92`.
- Description: for a `message` the row is inserted with `msgId = null`, then a second
  statement sets `msgId`. If the process dies between them, the row keeps
  `type='message'`, `msgId=null`. `encodeOutbox` emits `msgId` only for a message
  (no `outboxId` fallback), so the replayed frame carries **neither** delivery id:
  the agent cannot `msg_ack` it, `acked_at` never sets, and it replays on every
  reconnect forever.
- Failure scenario: host crash in the sub-millisecond window between the two writes
  → one message row is permanently unackable and head-of-queue on every replay.
- Suggested fix direction: set `msgId` in the same insert (e.g. compute/seed it
  without the second UPDATE), or have `encodeOutbox` fall back to `outboxId` for a
  message row whose `msgId` is null.

---

Section summary: P0=1 P1=0 P2=6 P3=4
