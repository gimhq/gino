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
| D1 | agent | P1 | `task_upsert{status:"closed"}` / `task_delete` have no execution effect: `tasks.status` is stored but never consulted — crons under a closed/cancelled task keep firing and queued messages still run; only the scoped `interrupt` (live, best-effort) stops the in-flight turn. Runner-side scheduler/executor must gate on task status. |
| D2 | agent | P2 | `task_upsert.data.engine/model/workdir/meta` are parsed away and ignored — per-task engine/model selection does not reach execution. |
| D3 | agent | P2 | Register failure handling: any non-`unsupported_protocol` reason (including `rate_limited`) exits the daemon instead of backing off and retrying. |
| D4 | agent | P3 | `heartbeat.activeThreads` never populated (fixed `{}` heartbeat); a `config_update` changing `heartbeatIntervalMs` does not re-arm the running transport. |
| D5 | agent | P3 | Transcript kinds under-emitted: only `assistant_text`/`status`/`error`; `thinking`/`tool_use`/`tool_result`/`user_input` (and `role`/`tool`/`input`/`output`) never leave the runner. |
| D6 | host | P3 | `thread_result.data.result`/`error` are dropped (no mirror columns); `thread_message` does not derive cron dispatch context (only `thread_result` does). |
| D7 | host | P3 | No per-task message delivery cursor (`TODO(M4/B3)` in `src/protocol/outbox.ts`); per-row acks only. |
| D8 | host | P3 | `task_delete` is never emitted (tasks are orphaned on agent removal); the runner implements it as a status-cancel. Documented behavior, no wire use. |
| D9 | both | P3 | Version negotiation is exact-equality (`=== 2`) on both sides; no range negotiation. |
