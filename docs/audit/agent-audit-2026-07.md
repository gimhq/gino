# gino-agent Repository Audit — 2026-07-02

Scope: `gino-agent/**` (Rust daemon, ~17.7k lines) at commit `cadfba7`.
Method: stack-aware repository audit (pma-cr policy), 5 parallel module-level deep reviews with cross-verification; single-source claims spot-verified against source.

## Summary

Engineering baseline is strong: detailed architecture specs (`docs/agent-architecture.md`, `docs/protocol.md`) closely matching the code; strict lint policy (forbid unsafe, deny unwrap/expect/panic, pedantic); fully parameterized SQL; correct tar-slip protection; `secrecy` handled properly in memory/logs; a complete and well-tested §9b config version gate; CI with fmt + clippy + ~213 unit tests + 50-check hermetic Docker E2E + secret scanning.

Systemic theme of the findings: **the core contracts declared in the spec — offline tolerance, at-least-once delivery, GC-able queues, shared continuity — are each only half-implemented.** The outbound path (outbox→host) is solid; inbound idempotency, retry budgets, the shutdown chain, and resource reclamation all have real breaks. Most P0/P1 findings were independently confirmed by two or more review passes.

---

## P0 — Silent incorrect behavior

- **P0-1 Shared-continuity crons execute only their first fire.** `scheduler.rs:756` (`ensure_thread` no-ops for an existing thread) + `executor.rs:272` (drains only `status='pending'`). A shared thread parks `active` after turn 1; nothing ever re-pends it. Every later fire writes `cron_logs status='dispatched'` while no engine run happens — a lying ledger. The existing test asserts only the ledger row, never a second execution.
- **P0-2 Daemon exits after ~10 min of host outage; cannot boot while host is down.** `transport/mod.rs:80` (first dial is fatal in `Transport::open`), backoff `max_retries: 60` capped at 10 s, then `runner.rs:660` shuts the whole daemon down — killing local cron scheduling too. Contradicts spec §1 ("host never on the execution critical path; offline-tolerant").
- **P0-3 Inbound at-least-once is not closed: redelivery causes duplicate execution.** No `msgId` column/UNIQUE dedup in inbox (`ws.rs:387`); a lost `msg_ack` → host resend → second row → the same user prompt runs twice. Additionally `replay_inbox` re-delivers unprocessed `message` rows on every reconnect, and each replay of an absent-`threadId` message mints a fresh UUID thread (`runner.rs:763`) — N reconnects → N+1 threads racing for one inbox row; losers ship spurious `runner.invalid_request` failures to the host. protocol.md §4.2 mandates dedupe by id.
- **P0-4 A chat message for an in-flight thread is silently lost.** `runner.rs:757-792`: for pending/dispatched/running threads the message is logged "queued" but nothing re-checks the inbox after the run finishes. The second of two quick messages never executes (and was already `msg_ack`ed); a third message re-pends and consumes the second — permanent off-by-one.

## P1 — High-probability correctness / operational / security failures

- **P1-5 "Retryable" failures are terminal under shipped defaults.** Cron threads hardcode `max_attempts: 1` (`scheduler.rs:493`); config default is also 1; the retry gate is `attempt < max_attempts` (`executor.rs:777`), so `1 < 1` never passes. Crash recovery, watchdog reap, and offline-fail paths all claim "will re-run" in comments but go straight to terminal `failed`. Combined with P0-1 a shared thread is killed forever. `config.executor.max_attempts` is only plumbed to chat threads (`runner.rs:194`).
- **P1-6 Shutdown chain broken.** Only SIGINT is handled (`runner.rs:651`); SIGTERM (systemd) takes the default handler. Even on SIGINT, shutdown never trips the active run's cancel (`executor.interrupt()` only wired to the host `interrupt` frame), so `worker.await` can block up to `running_timeout` (30 min) → supervisor SIGKILL → `kill_on_drop` never runs → the bypass-permissions `claude` child is orphaned and keeps executing.
- **P1-7 Connect token stored plaintext in a default-permission (0644) SQLite file.** `db.rs:413,1222`; no chmod hardening anywhere. Any local user can read the token, then fetch `GET /agent/config/:id` which returns `anthropicApiKey` — local read access becomes credential exfiltration.
- **P1-8 Terminal `thread_result` delivery is not crash-safe.** `executor.rs:753`: persist and outbox-enqueue are two separate transactions; a crash between them means the host never receives the result. The schema's designed repair mechanism (`threads.synced` + `list_unsynced_threads`/`mark_thread_synced`) is unwired dead code — `synced` is written `false` at 6 sites and never set true.
- **P1-9 Zero GC on `inbox`/`outbox`/`cron_logs`.** No DELETE exists for these tables (only `config_entries`), contradicting spec §8 "GC-able". A 60 s interval cron ≈ 526k ledger rows/year; every transcript event stays in outbox forever; `WHERE acked_at IS NULL` has no partial index, so reconnect replay full-scans an ever-growing table under the global DB mutex.
- **P1-10 Attachment byte-plane trio.** (a) Download buffers the whole body via `bytes().await` before any size check; a negative declared size maps to 0 via `u64::try_from(..).unwrap_or(0)` and bypasses pre-checks entirely (`attachments.rs:305` + `transport/http.rs:170`) — OOM surface. (b) Inbound descriptors live only in an in-memory one-shot stash (`attachment_source.rs:52`); a retry or daemon restart runs the thread with its snapshotted prompt but zero attachments, reported as success. (c) An absolute `ref` URL gets the connect token attached verbatim (`attachments.rs:157`) — token exfiltration to arbitrary origins.
- **P1-11 Provisioning manifest grows without bound and defeats its own GC.** Task scope is keyed by `thread.id` (`executor.rs:524`) instead of the spec's `task_id`; the only delete path is same-scope reconcile, so every isolated fire leaves permanent `config_entries` rows. GC roots on all rows → store entries referenced by dead threads are immortal; `repair()` re-fetches blobs and rebuilds symlink trees for dead threads at every startup (`reconcile.rs:212`) — startup cost grows linearly with historical fires.
- **P1-12 Inbound robustness stalls/loses.** A known-type frame with a malformed body is neither persisted nor acked (`ws.rs:364`) — the host resends it forever, head-of-line-blocking the entire host→agent stream. The inbox poison policy counts reconnects, not processing failures (`ws.rs:302`): 10 network flaps silently discard a legitimately queued message. Host-supplied `heartbeatIntervalMs: 0` panics the transport task (`ws.rs:153`, `tokio::time::interval` on zero).

## P2 — Structural risks

- **P2-13 `advance_cron` full-row read-modify-write overwrites concurrent edits.** `scheduler.rs:773` + `db.rs:696`: a `cron_upsert`/`cron_delete` landing between tick-read and advance-write is reverted — a deleted cron is resurrected to `active`. Zero transactions exist anywhere; `max_round_seq` read errors are swallowed to 0 (`.ok().flatten().unwrap_or(0)`), corrupting round accounting.
- **P2-14 Failure classification is substring matching over model prose.** `failure.rs:182`: bare needles (`"400"`, `"dns"`, `"blocked"`, …) run against the assistant's own result text and flip retry/resume policy (e.g. "took 1500ms" → `provider_server_error`; any "400" → `invalid_request`, which discards a healthy `engine_session_id`). events.rs already parses the structured `result.subtype` and then flattens it away.
- **P2-15 Blocking work inline on the tokio runtime.** All rusqlite calls (incl. 3 s busy_timeout), 100 MB-scale `fs::read/write`, full-buffer sha256, tar unpack, and a full cache-dir scan on every `cache_put` — with zero `spawn_blocking` in the crate.
- **P2-16 Transport efficiency and liveness.** Every flush retransmits the entire unacked outbox (O(n²) under ack lag); the executor never pokes flush per message — the worker flushes only after the whole drain, so a 30-minute run streams zero transcript to the host ("streaming" is batch-at-end); heartbeat is write-only, so a half-open socket is detected only by TCP retransmit timeout (~15 min); the reqwest client has no timeouts at all (registration can hang forever).
- **P2-17 Store integrity.** Identity check is 12-hex (48-bit) short id with no full-hash re-verification on reuse (collision or a post-crash zero-length file passes `is_file()` forever); store entries are 0644-writable and symlinked into engine cwd — a prompt-injected engine turn can poison shared global instructions/skills for every task (`echo evil >> project/CLAUDE.md`); neither repair (existence-only) nor GC (refcount-only) heals it. Graceful interrupt is a race SIGKILL always wins, yet the resume handle is kept for a hard-killed turn.
- **P2-18 No double-start protection.** No pidfile/flock; `claim()` is an unconditional UPDATE, not CAS — two daemons duplicate engine runs. Every CLI invocation runs `migrate()` (schema drift under a live daemon); no `PRAGMA user_version` migration path — later-added columns (`threads.last_seq`, `tasks.entries`) break on any pre-existing DB.
- **P2-19 Misc operational defects.** Failed frame application still marks the row processed (defeats replay); `cron_upsert` arriving before `task_upsert` is dropped permanently (FK failure swallowed + marked processed); `PRAGMA foreign_keys` never enabled (code comments assume FKs are live); `config_update` WS layer is unimplemented — a rotated `anthropicApiKey` is silently dropped until restart; with `inactivity_timeout_secs=0` a silent engine wedges the serial execution plane forever (total-time watchdog only evaluates on event arrival); archive index read-modify-write is non-atomic (concurrent PreCompact hooks can wipe it) and non-ASCII summaries slugify to empty → same-day archives overwrite each other.

## P3

Protocol version check fails open on non-integer; `http://` accepted end-to-end with no warning (both tokens can travel cleartext); register token accepted as CLI flag (visible in `/proc/*/cmdline`); tool-use-only assistant messages emit empty `assistant_text` frames (seq waste); `RegisterOk` derives `Serialize` (token-leak surface); engine stderr inherited → startup/auth failures unclassifiable; same-millisecond thread mints can cross-wire prompts; CLI `cron list` silently widens scope without task id; malformed outbox rows replay forever (no poison policy).

## Dead Code Findings

- **The reconciliation plane is write-only dead state**: `threads.synced` / `crons.dirty` written but never read; `list_unsynced_threads`/`mark_thread_synced`/`list_dirty_crons`/`set_cron_dirty` have no production callers. Not benign — it is the missing half of P1-8.
- **Two send designs coexist**: `Transport::send`/`immediate_tx` have zero production callers; all producers write the outbox directly and poke flush.
- **The Engine trait's session half is abandoned**: `Mode`, `Input`, `Session::send` (turn-append), `Engine::run_once`, `EngineEvent::Usage`, `LogLevel::{Info,Error}`, `EngineError::Io` all unreachable — the unimplemented turn-append is the root cause of P0-1.
- **Config dead corners**: `max_concurrent_tasks`/`max_queued_tasks` (execution is strictly serial), `log_level` (never consulted), host-layer `paths.dbPath` (DB opened before overlays), `Presigned` backend (selectable but unimplemented).
- **Unreachable taxonomy**: `FailureReason::from_wire`, `RunnerFailure::QueuedExpired` (spec §6 reason never constructed).
- **Stale module-level `#[allow(dead_code)]`** on failure/attachments/provisioning/http/config: the "to be wired later" wiring landed long ago; blanket allows now mask genuinely dead items. Narrow to per-item allows.

## Needs Runtime Verification

- Whether claude CLI session files survive SIGKILL immediately after a turn (the Cancelled-keeps-resume-handle policy bets on it); argv compatibility with the deployed CLI version.
- Host-side behavior on duplicate `(thread_id, seq)`, `final_seq=0`, and un-acked frame resend cadence (host code not in this audit's scope).

## Coverage Gaps

- Host side (Bun/TypeScript, `src/`) and the E2E harness itself were not audited.
- Engine sandbox strength unverified — several MEDIUMs (symlink write-through, store poisoning) scale with whether the claude child shares uid/container with the daemon. The trust boundary is 100% the container wall (unconditional bypassPermissions); consider a refuse-to-start-uncontained runtime assertion.
- **CI gap (confirmed)**: `deny.toml` exists but CI never runs `cargo deny` — the rustls-only supply-chain lock is decorative; `cargo shear` claimed in comments but absent from CI.

---

## Architecture Recommendations (by leverage)

1. **Honor the offline-tolerance contract** (fixes P0-2): reconnect forever with capped backoff; `Transport::open` returns immediately with a background dial; scheduler/executor start independent of the first dial. Acceptance: 24 h host outage does not affect local cron execution; full outbox replay on recovery.
2. **Close the inbound at-least-once loop** (fixes P0-3/4, P1-12, parts of P2-19): `msg_id` column + `ON CONFLICT DO NOTHING` (re-ack on conflict); idempotent thread minting (derive thread id from inbox row id, or record minted id on the row); don't mark processed on handler failure; poison policy counts processing failures; post-terminal inbox re-scan for the thread.
3. **Make the result plane transactional** (fixes P1-8, P2-13): wrap `fire_cron` and `finalize` each in `BEGIN IMMEDIATE`; `advance_cron` becomes a targeted UPDATE; wire the `synced` sweep or delete the dead reconciliation state — decide explicitly.
4. **Unify the cancellation chain** (fixes P1-6, interrupt race in P2-17): shutdown (SIGINT+SIGTERM) → trip active run cancel → bounded graceful drain window (await post-interrupt result or timeout) → kill. The `ThreadExecutor::interrupt` machinery already exists; converge the two unrelated `Cancel` types into one token.
5. **Shrink or fulfill the Engine trait** (fixes P0-1, P2-14): either implement `Session::send` turn-append (the proper shared-cron fix) or reduce the trait to spawn/events/interrupt and re-pend shared threads like chat does. Map the already-parsed `result.subtype` directly to `FailureReason`; retire prose substring classification.
6. **Make the concurrency model an explicit decision**: current state is global serial with head-of-line blocking plus fictional concurrency knobs. Either delete the knobs, or implement bounded concurrency — prerequisites are exactly this report's fixes: CAS claim, status-conditional UPDATEs, DB access via `spawn_blocking`/dedicated thread.
7. **Stream the byte plane** (fixes most of P1-10, P2-15): streaming download to temp file + incremental hash + in-flight size cap, heavy work in `spawn_blocking`. Also unify the store with the attachment cache per spec §9b (today: double storage, two mutually-blind GCs).
8. **Resource reclamation and schema evolution**: retention-window DELETEs for the three tables + partial indexes on `acked_at/processed_at IS NULL`; `config_entries` scope back to `task_id` (or terminal-state cleanup); `PRAGMA user_version` migration ladder; enable `foreign_keys`.
9. **Security hardening batch** (small, do together): chmod 600 DB+WAL/SHM, 700 workspace; store entries read-only after materialize + full-hash verify on repair; token only to same-origin URLs; loud warning on non-TLS host; register token out of argv; drop `Serialize` from response DTOs; flock single-instance lock.
10. **Make policy files real**: add `cargo deny check` and `cargo shear` to CI; add a shared-cron test asserting the second fire actually executes (the blind spot that let P0-1 survive).
11. **Code organization (lower priority)**: move chat-threading policy out of runner.rs to live beside the executor; flatten the `mod v2` migration shell; move the ~1000 lines of in-file executor tests out; merge the three near-identical path sanitizers.

## Suggested Fix Batches

- **Batch 1 — behavioral correctness (independently shippable)**: P0-1..4 + P1-5 (recommendations 1, 2, parts of 3, max_attempts plumbing).
- **Batch 2 — lifecycle & security**: recommendations 4 + 9 (P1-6/7, P2-17 hardening).
- **Batch 3 — structural evolution**: recommendations 5 + 6 + 7 + 8 (plus 10 early — CI gates are cheap and immediate).
