# Host Audit — M5 Message Store & M6 Channels (`src/msgstore/**`, `src/channels/**`)

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

---

### [P1] Poll egress advances the cursor on read, before the client receives the bytes (at-most-once → message loss)
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

### [P1] Webhook SSRF guard is bypassable (IPv4-mapped IPv6, `0.0.0.0`, alternate literal encodings)
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
  (Note: currently latent — see the P2 "webhook egress unwired" finding — but the guard is the
  security boundary and is broken today.)

### [P2] Egress cursor persists the SQLite `rowid`, which is not stable across `VACUUM`
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

### [P2] Outbound webhook egress is never invoked (unwired feature / dead path)
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

### [P2] Webhook delivery has no timeout and a poison message blocks a task's egress forever
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

### [P2] `thread_runs.cron_id` is never populated → host cannot derive cron dispatch counts
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

### [P2] DNS-rebinding TOCTOU: the SSRF guard resolves DNS, then `fetch` re-resolves independently (no IP pinning)
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

### [P2] No retention/GC and unbounded read fan-out on the transcript store
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

### [P3] `crons.prompt` mirror is permanently empty
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

### [P3] Egress delivers out of `seq` order under reconnect/replay (orders by arrival rowid)
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

### [P3] Webhook secret and transcript are re-sent to a redirect `Location` target
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

Section summary: P0=0 P1=2 P2=6 P3=3
