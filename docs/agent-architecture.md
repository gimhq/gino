# Agent Architecture (data plane)

Specification of the **agent / runner (data plane)** — the `gino-agent/**` Rust daemon. Pairs with
[`host-architecture.md`](host-architecture.md) (control plane) and [`protocol.md`](protocol.md) (the wire
contract). Host and agent **share no code**; the protocol is their only coupling.

## 1. Principles

1. **`task` is the single global top category.** Per runner, everything — every trigger, every execution,
   every message, every attachment — **belongs to a `task`** (`task_id`). It is the durable anchor for
   "parse the whole conversation/job by task".
2. **Two planes, one bridge (`thread_id`).**
   - *Scheduling plane* (local, runner-authoritative): `crons` + `cron_logs`.
   - *Result plane* (synced to host): `threads → outbox → host`.
3. **References on the wire, bulk out-of-band.** Frames stay small (control/work + descriptors); large bytes
   (attachments/artifacts) move on a separate channel (§9) and never enter the at-least-once queues or the
   transcript.
4. **Host is a read-model + a source**, never on the execution critical path. The runner is authoritative
   for scheduling/execution and is offline-tolerant.
5. **Continuity ≠ transcript.** Runner continuity = `engine_session_id` resume + `work_dir` reuse. The
   durable transcript lives on the host.

## 2. Entity hierarchy

```
task   ── global top category (per runner); everything belongs to it
 ├── crons   (N)   triggers / schedules           — crons.task_id → task
 └── threads (N)   engine executions / conversations — threads.task_id → task
        ▲
 a cron fire ──┘   (isolated → a NEW thread; shared → continue the cron's thread as a turn)
```

- **`task`** — the umbrella. 1 task : N crons, 1 task : N threads. May span multiple engines (a thread
  carries its `engine`).
- **`cron`** — a trigger (schedule / webhook / api) under a task.
- **`thread`** — one engine conversation/execution under a task.
- **`engine_session_id`** — a *field* on a thread, not an entity: the engine's resume context. A shared
  thread reuses one across its turns; it may rotate.

## 3. Runner data model (SQLite)

```sql
tasks(    id PK, name?, kind?, status, created_at, updated_at )          -- global top category

crons(    id PK, task_id → tasks,                                        -- trigger (1 task : N)
          kind,            -- 'schedule' | 'webhook' | 'api'
          prompt,          -- the instruction this cron runs each fire (a cron = a recurring prompt)
          schedule_type,   -- 'cron' | 'interval' | 'once'   (kind='schedule'; else NULL)
          schedule_value, next_run, last_fired_at,
          continuity,      -- 'isolated' (new thread per fire) | 'shared' (one ongoing thread)
          run_count,       -- CACHE; truth = cron_logs
          status,          -- active | paused | completed | cancelled  (once → completed)
          dirty, created_at, updated_at )

threads(  id PK, task_id → tasks,                                        -- engine execution/conversation (1 task : N)
          cron_id? → crons,        -- which trigger spawned it (NULL = chat/api/direct)
          source,                  -- cron | chat | api
          prompt,                  -- RESOLVED instruction snapshot at attempt 0 (from crons.prompt | message.text)
                                   --   so retries/resume reuse it instead of re-resolving
          engine,                  -- 'claude' | 'codex' | …
          engine_session_id?,      -- claude --resume handle (shared thread reuses across turns; may rotate)
          status,                  -- pending | dispatched | running | active | succeeded | failed | cancelled
          failure_reason,          -- taxonomy (§6); NULL on success
          attempt, max_attempts, work_dir,
          started_at, finished_at, duration_ms, result, error, synced,
          created_at )

cron_logs(id PK, cron_id → crons, task_id → tasks,                       -- per-fire ledger (LOCAL, incl. skips)
          round_seq, scheduled_at, fired_at,
          status,                  -- dispatched | skipped | missed | failed (SCHEDULING outcome)
          skip_reason, thread_id?, -- the thread this fire drove (new, or the shared one it added a turn to)
          UNIQUE(cron_id, scheduled_at),   -- firing idempotency / dedup
          created_at )

inbox ( …, task_id, thread_id? )   -- delivery buffer; GC-able after process
outbox( …, task_id, thread_id? )   -- delivery buffer; GC-able after ack

config_entries(  -- installed manifest + store-refcount index for provisioned config/skills (§9b)
  id PK, scope,            -- 'global' | <task_id>
  name, sha256, short,     -- store key: store/<name>/<sha256[:12]>; full sha256 = version/integrity
  target, kind,            -- relative landing path (meta-driven: CLAUDE.md|AGENTS.md|.claude/skills/x); file|dir
  store_path, link_path, installed_at,
  UNIQUE(scope, target) )
```

Per-execution state lives on `threads`.

## 4. Trigger model (`crons`)

- **`kind`** = schedule | webhook | api (only `schedule` wired now).
- **`schedule_type`** (schedule kind) = cron | interval | **once**. A one-shot is a `once` trigger — fires
  once, then `status='completed'`. All timing in `crons.next_run` → one timing loop.
- **`continuity`** = isolated (each fire → a new thread, fresh engine context) | shared (fires continue
  **one** ongoing thread, appending turns, reusing `engine_session_id`).
- **Deterministic id / dedup.** Firing idempotency is `cron_logs UNIQUE(cron_id, scheduled_at)`. An
  *isolated* fire additionally mints a deterministic `thread_id = "<cron_id>@<scheduled_epoch>"`.

## 5. Thread lifecycle

State machine: `pending → dispatched → running → {succeeded | failed | cancelled}` (a *shared* thread stays
`active` between turns).

```
trigger N due / event arrives:
  ├─ dispatchable?
  │   ├─ yes → isolated: threads.insert{id=cron_id@epoch, task_id, cron_id, status='pending'}
  │   │        shared:   reuse cron's open thread (append a turn)
  │   │        cron_logs.insert{round_seq=N, status='dispatched', thread_id}
  │   │        executor: pending → dispatched → running → terminal
  │   │        → outbox(thread_result, thread_id) + streamed thread_message → host
  │   └─ no  → cron_logs.insert{round_seq=N, status='skipped', skip_reason}  (no thread, no outbox)
  └─ crons.update next_run (NULL+completed if once), last_fired_at, run_count++
```

- Executor is **source-agnostic** (`SELECT … FROM threads WHERE status='pending'`). `dispatched` (claimed,
  engine not started) makes stuck startups detectable.
- **Watchdog**: fail `dispatched > Td` / `running > Tr` with `runner.timeout`; an inactivity watchdog kills
  only when the engine emits no events; on `close`/offline, fail orphans.
- Two `status` axes never overlap: `cron_logs.status` (scheduling) vs `threads.status` (engine).
- **Chat threading** — a `message` frame's `threadId` is the continuity switch: **present** → CONTINUE that
  thread (a resting thread re-pends for the new turn, KEEPING `engine_session_id` so the engine conversation
  resumes); **absent** → START A FRESH thread (the runner mints a new unique id, e.g. `<task>@<uuid>`, with
  no carried `engine_session_id`). The outbound `thread_result`/`thread_message` carry the minted id back
  (the executor drives by `thread.id`) so the host records it as the task's `current_thread_id`. There is no
  implicit per-task continue — every absent-`threadId` message is a new thread. (Sequential absent-`threadId`
  messages drain FIFO to their freshly-minted threads.)

## 6. Failure classification, retry, resume-safety

`threads.failure_reason`, two groups:

- **`runner.*`** — `queued_expired`, `runtime_offline`, `timeout`, `iteration_limit`, `agent_blocked`,
  `invalid_request`.
- **`agent.*`** — `provider_auth`, `provider_quota`, `provider_rate_limit`, `provider_server_error`,
  `provider_network`, `process_failure`, `empty_or_unparseable_output`, `context_overflow`,
  `model_unavailable`, `missing_config`, `unknown`.

`classify(rawError)` maps free text → reason. (`invalid_request` is **`runner.*`** — a malformed/incomplete
request is the host/runner's fault, not the provider's.) Then: **retry only `runner.*`** while
`attempt < max_attempts` (and the thread's `prompt` snapshot, §8, makes the retry re-runnable);
**resume-unsafe** reasons — a flagged subset where the engine session may be corrupted (e.g.
`agent.process_failure`, an engine crash mid-turn) — **do not persist `engine_session_id`** (next turn
starts fresh).

## 7. Scheduling plane: `cron_logs`

- Records **every** fire incl. ones producing no thread (`skipped`/`missed`) — so it can't be derived from
  `threads`. Each dispatched row links its `thread_id`.
- **Round-count truth = `cron_logs`** (`MAX(round_seq)`); `crons.run_count` is a cache reconciled at startup.
- **Local-only**: `{round_count, skipped_count, last_status}` rides `cron_state`/`heartbeat`; host derives
  dispatched counts from its mirror by `cron_id`.
- **Catch-up**: `interval` collapses missed periods; `cron` recomputes from wall-clock; `once` fires if
  still future else logs `missed`.

## 8. Continuity vs transcript

- **Thread instruction** = each thread snapshots its **resolved `prompt`** at attempt 0 — from `crons.prompt`
  (cron-fired) or `message.text` (chat/api). Retries and resume reuse the snapshot rather than re-resolving
  (a chat thread's inbox message is consumed once, so without the snapshot a retried `runner.timeout` would
  re-resolve to `runner.invalid_request`).
- **Runner continuity** = `engine_session_id` resume + `work_dir` reuse; no transcript replay. A changed
  resume id downgrades to fresh.
- **Durable transcript = host `messages`** (§11), keyed by `task_id`/`thread_id`, `seq`-ordered,
  `kind`-classified, fed by streamed `thread_message` frames.
- **Runner `inbox`/`outbox` are delivery buffers, not the transcript** — they carry `task_id`/`thread_id` as
  **indexed columns** (correlation/routing) while `payload` stays an opaque JSON blob: **outbox payload = the
  frame's `data`** (the envelope `{type, …ids, data}` is rebuilt on send from columns); **inbox payload = the
  full received frame** (re-delivered verbatim on replay; the `type` column is the replay *classification*).
  GC-able after ack/process. Kept as two tables (sender-queue vs receiver-log — different roles).
- **Concurrency / ordering**: many threads share one WS, so frames interleave. The host **demuxes by
  `threadId`** and **orders each thread by `seq`** (§10) — never by arrival order or `outboxId`.

Parse: whole job `WHERE task_id=?`; one conversation `WHERE task_id=? AND thread_id=? ORDER BY seq`.

## 9. Attachments & file sync (out-of-band)

Code/repos → git; attachments/artifacts → out-of-band object storage (this section).

- **Descriptor on the wire only**: `{ id, name, content_type, size, sha256, ref, direction? }`. Bytes never
  travel in frames.
- **Byte backend, config-selected**: **host-proxy by default** (`GET /agent/attachments/:id` +
  `POST /agent/attachments`, connect-token auth, host streams); **presigned URL / CDN** when object storage
  is configured (S3/MinIO/CloudFront).
- **Materialize into `threads.work_dir`**: inbound → `work_dir/inputs/`; outbound → upload everything under
  `work_dir/output/`; descriptors returned on `thread_result`.
- **Integrity/dedup/safety**: verify `sha256`; content-addressed cache by `sha256` under the workspace dir,
  shared within a workspace, size-cap + LRU GC; size limits (100 MB single / 500 MB per execution) + optional
  content-type allowlist; sanitize names vs `../`; short-lived presigned URLs, not persisted.

## 9b. Engine config & skills provisioning

Engine config (instruction file + skills) is provisioned as **versioned, content-addressed artifacts**, named
by **meta** (engine-agnostic), loaded by **symlink** from one store.

- **Entry** `{ id, sha256, target, kind }` — `id`=logical name, `sha256`=version/integrity, `target`=
  meta-driven landing path (`CLAUDE.md` | `AGENTS.md` | `design.md` | `.claude/skills/<name>` …),
  `kind`=`file|dir`. A **skill** = a `kind:dir` entry; an **instruction file** = a `kind:file` entry. The
  runner is engine-agnostic; the host sets `target` per engine.
- **Two scopes, same shape**: **global** (root `$ENGINE_CONFIG_DIR/`, via `config.entries`) and **task**
  (root `<thread.work_dir>/project/`, via `task_upsert.data.entries`).
- **Store + symlink**: `store/<name>/<sha256[:12]>` (git-style short id; full `sha256` in the wire ref for
  integrity). `<scope-root>/<target>` is a **symlink** into the store — zero copy, update = repoint, per-name
  version history. Bytes fetched out-of-band via `/agent/attachments`; **store unified with the §9 attachment
  cache**; refcount/LRU GC.
- **`config_entries`** (§3) = installed manifest + store-refcount index (not a content copy; bytes in the
  store, desired set declared by the host). Drives reconcile (diff desired vs rows), GC (refcount by
  `(name, sha256)`), restart repair (verify/recreate symlinks).
- **`project/` layout** (the thread cwd): `project/<target instruction file>` + `project/.claude/skills/*`
  (symlinks) + **`project/workspace/`** = the git working area (code isolated here → no conflict with
  injected config) + sibling `inputs/output/logs`. Engine launches with `ENGINE_CONFIG_DIR` set and
  `cwd=project/`.
- **Global config VERSION gate (restart durability)**: the global config carries a host-assigned **monotonic
  `version`** served on register / `GET /agent/config` / `config_update`. At startup the runner fetches
  `GET /agent/config` (authoritative — a restart reuses its identity, so the register response no longer
  carries the desired set) and reconciles the global scope **only when the version advances** past the applied
  version (persisted in a durable `config_meta` KV); same/absent version ⇒ **repair-only** (store + manifest
  survive). A *versioned* empty set removes all (authoritative-empty); a *non-versioned / offline* empty set
  never wipes. The task scope is restart-durable already (it persists via `tasks.entries` + `config_entries`).

## 10. Transport & protocol

WS primary (at-least-once via outbox/inbox + ack/msg_ack); HTTP for register/config/output + attachment proxy.

**Envelope vs payload.** Correlation/delivery metadata lives at the **envelope top level**; `data` carries
the type-specific business payload only:

```json
{ "type": "thread_message", "outboxId": 43, "taskId": "t-1", "threadId": "c-9@1718…",
  "data": { "seq": 3, "kind": "assistant_text", "content": "…" } }
```

- Envelope: `type`, `outboxId`/`msgId` (delivery), **`taskId`/`threadId`/`cronId` (correlation)**.
- `data`: business fields only — **never** the correlation ids. `engine_session_id` is a *value* of the
  thread and stays in `data` (stored, not routed-by).
- The host routes/correlates by the **envelope** ids without parsing `data`, mirroring the runner's
  `inbox`/`outbox` correlation columns and the host read-model keys.

**host → agent** (envelope ids · data)

| type | envelope | data |
|---|---|---|
| `task_upsert` / `task_delete` | taskId | `{name?, kind?, …}` |
| `cron_upsert` / `cron_delete` | taskId, cronId | `{kind, prompt, schedule_type?, schedule_value?, continuity, …}` |
| `message` | taskId, threadId? | `{text, attachments?}` |
| `interrupt` / `close` / `config_update` / `ack` | — | — |

**agent → host** (envelope ids · data)

| type | envelope | data |
|---|---|---|
| `thread_result` | taskId, threadId | `{status, result?, error?, failure_reason?, engine_session_id?, duration_ms, final_seq, attachments?}` |
| `thread_message` | taskId, threadId | `{seq, kind, role?, content?, tool?, input?, output?}` |
| `cron_state` | taskId, cronId | `{kind, schedule_type?, …, round_count, skipped_count, last_status}` |
| `heartbeat` | — | `{activeThreads?, …}` |
| `msg_ack` / `ack` | top-level `msgId`/`id` | — |

`kind` ∈ `{user_input, assistant_text, thinking, tool_use, tool_result, status, error}` (`thinking`/`log`
config-filterable). Bytes never in frames — only descriptors.

**Multiplexing & ordering (one WS, N parallel threads).** Concurrent threads' frames interleave. The host
**demuxes by `threadId`**, **orders each thread's transcript by `seq`** (per-thread monotonic, assigned
serially by the runner), and writes idempotently on `(thread_id, seq)` (dedup, since at-least-once replay can
duplicate). `thread_result.final_seq` lets the host verify it received `1..final_seq`. `outboxId` is
delivery/ack only; `seq` is the per-thread continuity contract.

## 11. Host-side read models (mirror; otherwise host-private)

- **`tasks`** — mirror of the top-level entity.
- **`crons`** — trigger defs (from `cron_state`).
- **`thread_runs`** — one row per execution (from `thread_result`), keyed by `thread_id`, with `task_id`,
  `cron_id`, `status`, `failure_reason`, `duration_ms`.
- **`messages`** — durable transcript `{task_id, thread_id, seq, kind, role, content, tool, input, output,
  created_at}` (from `thread_message`); the conversation-parse source. **Composite key `(task_id, thread_id,
  seq)`** (index/grouping); minimal uniqueness/dedup `(thread_id, seq)` (thread_id is globally unique →
  determines task_id). Order a thread by `seq`; order threads within a task by `thread.created_at`.
- **`attachments`** — `{id, task_id, thread_id, name, content_type, size, sha256, ref, direction}`,
  referenced from `messages` by `id`.
- `task_id ↔ delivery channel` mapping stays host-private.

## 12. Identifiers

| id | layer | meaning | cardinality |
|---|---|---|---|
| **`task_id`** | top | global category per runner; everything belongs to it | the anchor |
| **`cron_id`** | child of task | a trigger/schedule | 1 task : N |
| **`thread_id`** | child of task | one engine execution/conversation | 1 task : N |
| **`engine_session_id`** | *field on thread* | the engine (claude) `--resume` handle | per-thread, may rotate / be shared across a shared thread's turns |

"session" is **only** `engine_session_id` (a field), never an entity.
