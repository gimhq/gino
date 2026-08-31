# Host ⇄ Agent Protocol Alignment — 2026-08

Cross-implementation audit of the live wire behavior of the host (`src/**`, TypeScript)
against the runner (`gino-agent/**`, Rust), measured on branch `align/host-agent-wire`
after the 2026-07 host/agent audit remediations. Method: independent behavioral maps of
both sides (every frame, endpoint, and delivery mechanism, with `file:line` evidence),
diffed against `docs/protocol.md` v2 and against each other; every P0 finding was then
re-verified in source before fixing.

Verdict before this branch: the two sides passed their own hermetic suites but had
**never been proven against each other** — the host tests script a fake agent, the
runner tests script a stub host, and both stubs mirrored their own side's assumptions.
Three planes were broken in production wiring: delivery acknowledgment, the attachment
byte channel, and provisioning entry parsing.

## Fixed on this branch

### P0-1 · Delivery guarantee: non-`message` host→agent frames were never acked

- The host expected `ack{id}` (`outboxId`) for `task_upsert` / `cron_upsert` /
  `cron_delete` / `close` / `config_update`; the runner **never sends `ack`** — it
  acknowledges durable storage of ANY inbound frame via `msg_ack{msgId}` and dedups on
  `msgId` alone (`gino-agent/src/transport/ws.rs` `classify_inbound`; every
  `InboundFrameV2` variant models `msg_id`).
- Consequence: those outbox rows stayed unacked forever → replayed in full on every
  reconnect, never GC'd (unacked rows are exempt), and — because the runner stored them
  with `msg_id = NULL` — re-applied without dedup each time.
- Fix (host): `encodeOutbox` now stamps `msgId` (= row id) on EVERY durable frame
  (`outboxId` retained on non-`message` frames for compatibility), and `ackMessage`
  clears the row for any type, not just `message`. `src/protocol/envelope.ts`,
  `src/protocol/outbox.ts`, `src/protocol/frames.ts` (+ §8 regen).

### P0-2 · Attachment upload contract mismatch (both directions dead)

- Runner uploads with metadata headers `x-attachment-name` / `x-attachment-sha256`
  and parses a **flat** `{id, ref}` response (`gino-agent/src/transport/http.rs`,
  per agent-architecture §14 A1). The host required `name`/`task_id`/`thread_id`
  query params (400 otherwise) and returned a `{success, data}` envelope — every
  runner upload failed twice over.
- Fix (host): header metadata accepted; `task_id`/`thread_id` optional (must be
  paired; authorization unchanged when present); response is the flat descriptor.
  The upload carries no task/thread binding — the binding lands when
  `thread_result.data.attachments` arrives (which the host previously **discarded**;
  it is now persisted with the envelope's `taskId`/`threadId`). Blob GC's 7-day
  unreferenced grace covers the window between upload and `thread_result`.
- Runner names outbound files by `output/`-relative path (separators included);
  the store requires a single safe segment. A shared `sanitizeAttachmentName`
  flattens deterministically at both the upload route and the registry writer, so
  both derive the same store ref.

### P0-3 · `ref` semantics: host leaked its store-internal ref onto the wire

- The runner resolves a descriptor `ref` against its host base URL and GETs it;
  the host emitted the storage-layout ref `<name>/<sha256[:12]>` → every inbound
  attachment download 404'd.
- Fix (host): the wire `ref` is now always the byte-channel URL
  `/agent/attachments/<id>` (upload responses, and `message.data.attachments` are
  rewritten + registered at emit time); the store ref never leaves the host and is
  re-derived — never trusted from a peer-supplied descriptor.

### P0-4 · Provisioning fetch: runner asked for a URL the host never served

- The runner fetched desired entries at `GET /agent/attachments/<entry-name>`; the
  host serves attachments by descriptor id there, and config artifacts only via
  `?sha256=` (desired-set-gated). Every provisioning byte fetch 404'd.
- Fix (agent): the provisioner now fetches content-addressed
  (`/agent/attachments?sha256=<hex>`) — the entry name is a logical label, only the
  hash identifies the bytes. `gino-agent/src/provisioning/reconcile.rs`.

### P0-5 · Provisioning entry field spelling: host `name` vs runner `id`

- Host entries (`ConfigEntry`, protocol.md §8) carry `name`; the runner's
  `ProvisioningEntry` required `id` — so any `config_update` / `task_upsert` /
  register config carrying a non-empty entry set failed the typed parse (the
  `config_update` was dead-lettered; a register overlay would have exited the
  daemon). Surfaced by the first cross-implementation E2E run of this branch.
- Fix (agent): `#[serde(alias = "name")]` on `ProvisioningEntry.id`; wire spelling
  documented as `name` (protocol.md §8, agent-architecture §9b).

### P1 · `interrupt` violated its own live-only contract on the runner

- Protocol §5: interrupt delivery is LIVE-ONLY (at-most-once), never replayed. The
  host complies (`enqueueLiveOnly`), but the runner persisted interrupts to its
  inbox and re-delivered them on reconnect replay — a stale cancel could fire
  against a future turn after a crash between persist and route.
- Fix (agent): `interrupt` rows are retired (marked processed), not replayed, in
  `replay_inbox` — same policy family as `config_update`.

### Docs brought back in lock-step (`docs/protocol.md`)

- §2: full byte-channel contract (by-id, by-sha desired-set fetch, upload headers,
  flat response, resolvable `ref` semantics).
- §3: real register request fields (`version`/`platform`; `source` optional), and
  the previously undocumented `config.entries` + `config.version` (the §9b global
  provisioning set + version gate) on register / config pull / `config_update`.
- §4.2: `msgId`+`msg_ack` as the durable ack for every host→agent frame.
- §5: `close.data.reason?`, `config_update.data = {entries, version}` (required).
- §8 regenerated from `src/protocol/frames.ts`.

## Verification

- Host: `bun run check` green (lint, typecheck, 673 tests, build).
- Agent: 321 lib tests + 20 hermetic integration tests green; fmt + clippy clean.
- Cross: `e2e/run.sh` (REAL Rust runner × REAL host in compose, fake engine)
  **PASSED**, including the previously-impossible flows: durable frames acked and
  not re-replayed, provisioning entries parsed and reconciled, attachment ingest
  round-trip. The e2e `dir` artifact is now a real uncompressed tar (the previous
  placeholder string made the runner's unpack fail); the runner's stub host now
  serves `?sha256=` content-addressed fetches.

## Known gaps deferred (tracked, not fixed here)

| # | Side | Severity | Gap |
|---|---|---|---|
| D4 | agent | P3 | `heartbeat.activeThreads` never populated (fixed `{}` heartbeat); a `config_update` changing `heartbeatIntervalMs` does not re-arm the running transport. |
| D5 | agent | P3 | Transcript kinds under-emitted: only `assistant_text`/`status`/`error`; `thinking`/`tool_use`/`tool_result`/`user_input` (and `role`/`tool`/`input`/`output`) never leave the runner. |
| D6 | host | P3 | `thread_result.data.result`/`error` are dropped (no mirror columns); `thread_message` does not derive cron dispatch context (only `thread_result` does). |
| D7 | host | P3 | No per-task message delivery cursor (`TODO(M4/B3)` in `src/protocol/outbox.ts`); per-row acks only. |
| D8 | host | P3 | `task_delete` is never emitted (tasks are orphaned on agent removal); the runner implements it as a status-cancel. Documented behavior, no wire use. |
| D9 | both | P3 | Version negotiation is exact-equality (`=== 2`) on both sides; no range negotiation. |

## Fixed in follow-up (runner-side D1–D3)

D1–D3 were closed after this branch landed and have been lifted out of the deferred
table so it stays a list of open work. All three are runner-side; none changed the
wire.

### D1 (P1) · Task status now gates execution — `32cf9b6`

`feat(agent): gate scheduler, executor and chat on task status`. `tasks.status` is
consulted at execution time through the shared gate `Db::task_execution_blocked` /
`executor::task_parked` (only `closed` and `cancelled` park; any other status — and a
missing row, which the runner seeds lazily — keeps prior behavior). It is checked in
the scheduler (at fire time in `tick`, and in `next_wait` so an overdue parked cron
cannot pin the sleep at zero), in the pending-thread drainer (a parked thread stays
`pending` — never failed, never deleted), and in the chat re-scan (a parked task's
message stays unprocessed in the inbox). `task_upsert{status:"active"}` unparks and
wakes both loops through the existing nudge/drain signals; `task_delete` (status
`cancelled`) inherits the gate. No schema migration.

### D2 (P2) · Task definition parsed, persisted and honored — `f1b7430`

`feat(agent): persist and honor task engine/model/workdir/meta`. `TaskUpsertData` now
models all four fields under the same omission-preserves rule that already governed
`entries`; they are carried on `TaskRow` through a v3 → v4 schema ladder step (the
fresh-DB fast path now runs the whole ladder instead of a hardcoded prefix, keeping a
fresh and a migrated DB on the same shape). `apply_task_def` patches only the fields
the host sent, so a partial upsert never wipes a stored definition. The task's `model`
is read at spawn time and layered onto the Claude settings for that session,
overriding `config.engine.model` — apply-on-next-spawn, never hot-swapped into a live
turn, the same policy `config_update` uses. An unsupported `engine` is warned about
and falls back to the one engine this build implements, rather than stranding the
task's threads. `workdir` and `meta` are persisted only; nothing reads them yet.

### D3 (P2) · Transient register failures retry instead of exiting — `ccb89cc`

`fix(agent): retry transient registration failures with capped backoff`.
`register_with_backoff` drives `POST /agent/register` on the same capped exponential
schedule the reconnect loop uses (1 s base, 10 s ceiling, unbounded attempts), and the
retry/fatal decision lives in `register_failure_action`, which fails closed: only
`rate_limited`, HTTP 429, HTTP 5xx, and transport/decode failures retry;
`invalid_register_token`, `registration_disabled`, `invalid_body`,
`unsupported_protocol`, the protocol-version gate, and any unrecognized reason still
abort startup, so a rejection this build has never heard of cannot become a hot loop.
