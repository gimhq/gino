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

- **Entry** `{ name, sha256, target, kind }` (wire spelling `name` — protocol.md §8 `ConfigEntry`; the
  runner's internal field `id` accepts it as an alias) — `name`=logical name, `sha256`=version/integrity, `target`=
  meta-driven landing path (`CLAUDE.md` | `AGENTS.md` | `design.md` | `.claude/skills/<name>` …),
  `kind`=`file|dir`. A **skill** = a `kind:dir` entry; an **instruction file** = a `kind:file` entry. The
  runner is engine-agnostic; the host sets `target` per engine.
- **Two scopes, same shape**: **global** (root `$ENGINE_CONFIG_DIR/`, via `config.entries`) and **task**
  (root `<thread.work_dir>/project/`, via `task_upsert.data.entries`).
- **Store + symlink**: `store/<name>/<sha256[:12]>` (git-style short id; full `sha256` in the wire ref for
  integrity). `<scope-root>/<target>` is a **symlink** into the store — zero copy, update = repoint, per-name
  version history. Bytes fetched out-of-band, content-addressed by full hash
  (`GET /agent/attachments?sha256=<hex>`, authorized by desired-set membership); **store unified with the §9 attachment
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

## 9c. Engine backends (`claude`, `codex`)

The runner holds a **name → backend registry** with a default (`claude`), not one fixed engine. At every
spawn the executor picks the backend in this order: the **task** row's `engine` (the host's
`task_upsert.data.engine`), else the **thread** row's `engine`, else the **default**. The task row is read
at spawn — the same apply-on-next-run policy as the per-task `model` override — so a `task_upsert` that
changes the engine reaches the *next* run without disturbing the one in flight. An **unregistered engine
name is not fatal**: the registry warns and answers the default.

**The claude backend.** One prompt can end in several `result` lines: after a background subagent
reports back, the CLI runs another turn by itself and emits another `result`. The child is started with
`CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS=1`, and the run ends on `system/session_state_changed` → `idle`
with the **latest** `result`. A CLI that never reports session state ends on its first `result`, and a
result still held when the child exits is reported as it is.

**The codex backend.**

- **One `codex exec --json` child per TURN** — unlike claude, a single long-lived child driven over a
  bidirectional control protocol. The prompt is written to the child's stdin, which is then **closed**, and
  the runner reads JSONL `ThreadEvent`s off stdout until the process exits.
- **Continuity is the thread id**: `thread.started` reports it, the runner persists it as the thread's
  `engine_session_id`, and the next turn passes it back as a trailing positional `resume <thread_id>`.
  Rewinding to a message has no codex equivalent, so a resume point is ignored rather than approximated.
- **argv**: `codex exec --json [--model M] [--ask-for-approval P] [--sandbox S] [--skip-git-repo-check]
  [extra args…] [resume <thread_id>]`, with `cwd` = the thread's `project/` dir (§9b).
- **The defaults are deliberate** (all three are configurable):

| flag | default | why |
|---|---|---|
| `--ask-for-approval` | `never` | a turn must never block on an approval prompt the runner has no way to answer |
| `--sandbox` | `danger-full-access` | parity with the claude backend's `--dangerously-skip-permissions`; isolation in this product is the **container** boundary, and a narrower default would make the two backends run the same task differently |
| `--skip-git-repo-check` | on | the per-thread cwd `work_dir/project/` is not a git repository, and `codex exec` refuses to run outside one |

**Event mapping** (codex JSONL → engine-agnostic event → host transcript kind):

| codex event | `EngineEvent` | transcript |
|---|---|---|
| `thread.started` (`thread_id`) | `SessionStarted` | — (persisted as the resume handle) |
| `item.completed` `agent_message` | `Output` | `assistant_text` |
| `item.completed` `reasoning` | `Thinking` | `thinking` |
| `item.started` `command_execution` / `file_change` / `mcp_tool_call` / `collab_tool_call` / `web_search` | `ToolUse` | `tool_use` |
| `item.completed` for those same kinds | `ToolResult` | `tool_result` |
| `item.completed` `error` | `Log` (error) | a log line — **not** terminal |
| `turn.completed` | `Result` (success) | `thread_result` succeeded |
| `turn.failed` | `Result` (error, `Execution` → `agent.unknown`) | `thread_result` failed |
| `error` (stream-level, e.g. a reconnect notice) | `Log` (warn) | a status line — **not** terminal |
| `item.updated`, `todo_list`, anything unknown | — | dropped |

- The tool **name is the codex item kind verbatim** (`command_execution`, `file_change`, …), never aliased
  onto claude's tool names — aliasing would make the transcript claim codex did something it did not.
- `input` and the `tool_result` `content` carry the **whole item minus `id`/`item_type`/`type`**, deliberately
  shape-agnostic: codex item payloads differ per kind and per CLI version, so a hand-picked field list would
  silently drop fields.
- **`parent_tool_use_id` is always absent for codex.** Codex reports no parent linkage and the runner does
  not invent one, so codex transcripts carry no sub-agent attribution (§10).

**Interrupt.** Claude receives an in-band control `interrupt`; codex has no such channel — stdin is already
closed by the time the turn runs — so the runner **SIGINTs the child**, waits a bounded 2 s grace, then kills
it. The cancelled turn settles through the existing executor path: the child dies, stdout hits EOF, the event
stream ends, and `drain_cancelled` settles the thread `cancelled`.

**Config.** The `engine.codex` block — `binary`, `model`, `approvalPolicy`, `sandboxMode`,
`skipGitRepoCheck`, `extraArgs`, plus `openaiApiKey` / `openaiBaseUrl` (handed to the child as
`OPENAI_API_KEY` / `OPENAI_BASE_URL`) — is additive: an absent field leaves the backend default in place. The
provisioned engine config dir (§9b) is exported to a codex child as `ENGINE_CONFIG_DIR` + **`CODEX_HOME`**,
the way it is `ENGINE_CONFIG_DIR` + `CLAUDE_CONFIG_DIR` for claude.

**Operator-visible constraints.**

- The runner registers the codex backend and advertises the `codex` **capability** only when a **stat-probe**
  finds the configured binary. The probe never *executes* the binary.
- **Capabilities are computed once, during the register call.** Installing a codex binary on an
  already-registered runner does not make it codex-capable until it re-registers; a `config_update` reaches
  the settings seam but not the registry until restart.
- The host **matches `task.engine` against the agent's advertised `capabilities.engines` at placement time**
  (both the auto-ranking and manual-assign paths). So the probe decides **routability**, not just
  runnability: a task naming an engine no permitted agent advertises is not rejected — it stays unplaced
  (`no-permitted-agent`) in `pending_assignment`.
- **Known limitation.** If an agent advertised `codex` at register and the binary later disappears (or config
  repoints it), the host keeps routing codex tasks there, and the runner's fall-back-to-default rule means the
  run **completes on claude**; a warning in the agent log is the only signal. That is the trade-off of never
  failing a task on an engine-name mismatch.

## 9d. Native deployment and engine sandbox

Outside a container the runner can confine each engine run (claude or codex) in a **bubblewrap sandbox**.
Isolation is **off by default**; with it off, launches are unchanged.

**Requirements.** A Linux host; `bubblewrap` with `bwrap` on the daemon's `PATH`; unprivileged user
namespaces enabled. Run the agent as a dedicated unprivileged user; a sandboxed run fails closed
before spawn when the agent runs as root. Network mode `allowlist` also needs `/dev/net/tun`.

**Config.** The `engine.isolation` block (without a local isolation file, an overlay that sets it
replaces the earlier block wholesale; see *Local isolation file* below):

```json
{ "engine": { "isolation": {
  "mode": "sandbox",
  "network": { "mode": "none" },
  "readPaths": ["/opt/node"],
  "writePaths": ["/data/cache"],
  "allowTaskOverride": false
} } }
```

- `mode` ∈ `off | sandbox | vm`; absent = `off`. `network.mode` ∈ `none | full | allowlist`; absent = `none`.
- `readPaths` / `writePaths` are extra host paths bound read-only / read-write. Each must be absolute,
  contain no `..`, and be neither the workspace root nor a parent of it. They are checked only when the
  resolved mode is not `off`.
- **Per-task override**: `meta.isolation` on the task (`task_upsert`) overrides `mode` and `network.mode`
  only; extra paths always come from the config. Meta that is not a JSON object, or has no `isolation`
  key, means no override.
- **Tighten-only overrides.** Unless the config sets `allowTaskOverride: true` (absent = `false`; the key
  inside `meta.isolation` is ignored), an override may only tighten the default. Modes order
  `off` < `sandbox` < `vm`; network modes order `full` < `allowlist` < `none`, compared against the
  default's effective value (absent mode = `off`, absent network = `none`). A default of `off` confines no
  network, so any network override tightens it. A loosening override (e.g. `sandbox` → `off`, `none` →
  `full`) fails the run with "task meta.isolation would loosen `<setting>` from … to …; set
  engine.isolation.allowTaskOverride to allow this", before any not-supported check; neither value is used
  as a fallback. With `allowTaskOverride: true` the override wins as-is.
- `vm` fails the run with "`… is not supported yet`". Unknown values, an invalid path or a
  malformed `meta.isolation` also fail the run. All of these are classified as `runner.invalid_request` (§6) and
  are not retried.

**Local isolation file.** `GINO_ISOLATION_FILE` (env only, no CLI flag) names an agent-local file
holding the `engine.isolation` block as a JSON object. It is read in `ConfigOverlay::paths_from_env`, so
both the daemon and `gino-agent login` fail at startup on a missing, malformed or invalid file; the
error names the path, and an invalid rule also names its index (`rule <n>: …`). The file is the
**floor**: it becomes the default, and a host `engine.isolation` overlay is layered on the floor (not on
the previous host block) and may only tighten `mode` (`off` < `sandbox`) and `network.mode` (`full` <
`allowlist` < `none`). A loosening value, and any host `network.allow`, `readPaths`, `writePaths` or
`allowTaskOverride`, is ignored with one warning per field. Without a file the host block replaces the
default as before. The host does not send an `engine.isolation` block yet, so today the file plus task
`meta.isolation` (under the tighten-only rules above) are the only inputs.

**Allowlist.** `network.mode: "allowlist"` gives the engine a network that reaches only listed
destinations:

```json
{ "mode": "sandbox", "network": { "mode": "allowlist", "allow": [
  { "host": "registry.npmjs.org", "ports": [443] },
  { "host": "*.github.com", "ports": [443, 22] },
  { "cidr": "10.20.0.0/16", "ports": "*", "proto": "udp" }
] } }
```

- Each rule has exactly one of `host` (an exact name, or `*.suffix` matching strict subdomains only) or
  `cidr` (IPv4/IPv6 `addr/prefix`, matched against literal-IP destinations). `ports` is a non-empty list
  of non-zero ports or `"*"`; `proto` is `tcp` (default) or `udp`. The first matching rule wins.
- Rules come only from the config default (the local file); `network.allow` in task `meta.isolation` is
  dropped. A task override may still switch the network mode (tighten-only unless `allowTaskOverride`).
- **LLM endpoint, always allowed** (TCP): the host and port of `ANTHROPIC_BASE_URL`, else
  `api.anthropic.com:443`. A loopback base URL (`localhost`, or a loopback / unspecified IP) is
  rewritten for the engine to `10.0.2.2` with the same scheme, userinfo, port, path and query; only that
  gateway port is forwarded to the host loopback address. A non-loopback IP literal is allowed as that exact address and
  port. An always-allowed endpoint is exempt from the private-address refusal (e.g. an LLM proxy on the
  LAN) for its exact host:port; loopback, unspecified and link-local (`169.254.0.0/16`, `fe80::/10`)
  addresses stay refused, and a link-local IP-literal base URL fails the run before spawn. `full` mode
  keeps the original URL.
- **LLM endpoint under a floor.** With a local isolation file the always-allowed endpoint (and the
  loopback gateway forward) comes only from the daemon's own `ANTHROPIC_BASE_URL` (its process env), never
  from a host `engine.anthropicBaseUrl`. A host base URL that differs from the daemon's gets no
  always-allow: it is admitted only when a floor `network.allow` rule already covers its host and port
  over TCP (and then follows the normal rule path, private-address refusal included); a loopback one is
  never admitted. Otherwise the run fails before spawn with an error naming `engine.anthropicBaseUrl`,
  without echoing the URL.
- **DNS.** The engine's `resolv.conf` is `nameserver 10.0.2.2`. Allowed names get a stable fake IPv4
  address from `198.18.0.0/15`, which the exit maps back to the name; the real address is resolved on the
  host. Names matched by neither a host rule nor the LLM endpoint get NXDOMAIN. IPv4 only: `AAAA` (and
  other types) for an allowed name get no data.
- **Refusal.** A flow is decided before the guest handshake completes and, when allowed, only after the
  host connect succeeded; a refused TCP flow gets an RST, a refused UDP flow an ICMP port unreachable —
  never accept-then-close. Private, loopback, link-local and other reserved destinations (also when a
  name resolves to one; IPv4-mapped/NAT64/6to4 forms checked as IPv4) are refused unless a `cidr` rule
  covers them.
- **Limits** per run: 256 live TCP connections, 1024 UDP flows, 128 KiB TCP buffers.
- **Decision log.** Every flow logs one `network decision` event on target `gino_agent::net` with
  `thread_id`, `proto`, `target_name` (`-` when unknown), `target_ip`, `port` and `decision`: `allow`
  (info, `rule_index` or `always`), `deny` (warn, `reason`), or `refused` for an allowed flow that still
  failed (warn, `reject_kind` = `limit` / `reset` / `unreachable`, `reason`).

**Launch chain.** daemon → `bwrap` → `gino-agent sandbox-init` (pid 1) → engine.

- `bwrap` unshares every namespace (`--unshare-all`, `--die-with-parent`, `--new-session`) and runs
  `sandbox-init` as uid/gid 0 of the sandbox user namespace with **only `CAP_SETFCAP`** — needed to map a
  nested user namespace onto that uid 0 (kernel ≥ 5.12).
- `sandbox-init` enters a nested user namespace, switches to **uid/gid 1000**, clears every capability
  set, sets `no_new_privs`, verifies `CapEff`/`CapPrm` are empty, then spawns the engine with the inherited
  stdio and environment and exits with its status (128 + signal when killed). Any setup failure exits
  **125** with a `sandbox-init: …` line on stderr.
- `bwrap` does not forward signals, so `sandbox-init` forwards SIGINT/SIGTERM to the engine; the codex
  interrupt (§9c) SIGINTs `sandbox-init` rather than `bwrap`.
- **Allowlist only:** the daemon creates a private 0700 per-run dir (`$TMPDIR/gino-net-<uuid>`) with the
  control socket and `resolv.conf`. `bwrap` additionally gets `--cap-add CAP_NET_ADMIN`,
  `--dev-bind /dev/net/tun`, that `resolv.conf` read-only, and the socket at `/run/gino/ctl.sock`.
  `sandbox-init --ctl-sock` creates `gino0` (`10.0.2.15/24`, gateway `10.0.2.2`), sends its fd to the
  daemon, closes it and waits for the daemon's ready byte before dropping capabilities and starting the
  engine; any failure exits 125. The daemon receives the fd, starts the run's user-space exit and releases
  the engine (30 s timeout); a setup failure kills the sandbox and fails the run with "engine sandbox
  network setup failed: …". The exit lives as long as the session.
- The stdio stream and the claude control protocol are unchanged.
- The engine binary is resolved on the daemon's `PATH` and canonicalized; its install dir is bound
  read-only unless it is under `/usr`, and so is the agent binary. An engine that needs files elsewhere
  (e.g. a node runtime outside `/usr`) needs them in `readPaths`.

**Mounts.**

| visible | how |
|---|---|
| `/usr`, `/bin` `/lib` `/lib64` `/sbin` (mirroring the host's dirs/symlinks), `/etc` | read-only |
| engine install dir, agent binary, `readPaths` | read-only |
| the thread's `work_dir`, `writePaths` | read-write |
| `/tmp`, `$HOME` (the daemon's, else `/home/gino`) | fresh tmpfs |
| `/proc`, `/dev` | new instances |

Not visible: the workspace root and other threads' work dirs, `agent.db`, the global engine config dir
(except the two binds below), `conversations/` under the workspace, `/run`, `/var`.

**Engine home.** Each thread gets `<work_dir>/.engine-home`, exported as `ENGINE_CONFIG_DIR` plus
`CLAUDE_CONFIG_DIR` (claude) or `CODEX_HOME` (codex). The global `CLAUDE.md` and `skills/` (§9b) are bound
into it read-only when present. `claude login` state in the global dir is **not** visible, so credentials
must come from the environment: `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, or the host proxy
(`ANTHROPIC_BASE_URL`). The engine also gets `HOME`, `TMPDIR=/tmp` and `GINO_SANDBOX=1`.

**Archive and `gino` CLI.** The PreCompact archive (`gino-agent archive`) writes to `<work_dir>/conversations` via
`GINO_ARCHIVE_DIR`. The in-engine `gino` CLI cannot open the agent DB and reports "agent state is not
available inside the sandbox".
The target design replaces direct DB access with the local agent API (§10b).

**Network.** `none`: own network namespace with loopback only. `full`: the host network
(`--share-net`); when `/etc/resolv.conf` links outside `/etc`, its target is bound read-only.
`allowlist`: own namespace whose default route is the TUN device (see *Allowlist*).

**Fail-closed.** The first sandboxed run resolves `bwrap` and probes it once per process (cached). If
`bwrap` is missing or the probe fails, every sandboxed run fails with "engine sandbox is unavailable: …";
it never falls back to running unconfined.

**Testing.** Privileged integration tests (`tests/sandbox.rs`, `#[ignore]` and gated on
`GINO_SANDBOX_IT=1`) run via `gino-agent/scripts/sandbox-it.sh`: it builds as root in a sibling container
with the moby seccomp profile extended for user namespaces, `/dev/net/tun` and unconfined
AppArmor/system paths, then runs the tests as uid 1000. The real-engine e2e
`SANDBOX_NETWORK=none|allowlist|full ./e2e/run-real-sandbox.sh` (default `none`; `--smoke` skips the
model call) drives the real claude CLI in the sandbox through the host `/llm` proxy; exit 3 means
Anthropic was reached but rate-limited.

## 10. Transport & protocol

WS primary (at-least-once via outbox/inbox + ack/msg_ack); HTTP for register/config/output + attachment proxy.

**Envelope vs payload.** Correlation/delivery metadata lives at the **envelope top level**; `data` carries
the type-specific business payload only:

```json
{ "type": "thread_message", "outboxId": "01jab3x7k2m4n5p6q7r8s9t0vw", "taskId": "t-1", "threadId": "c-9@1718…",
  "data": { "seq": 3, "kind": "assistant_text", "content": "…" } }
```

- Envelope: `type`, `outboxId`/`msgId` (delivery, lowercase ULIDs minted by the sender), **`taskId`/`threadId`/`cronId` (correlation)**.
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
| `fs_request` (live-only) | reqId | `{op, path, to?, recursive?, ref? + sha256? + size?, upload?}` |

**agent → host** (envelope ids · data)

| type | envelope | data |
|---|---|---|
| `thread_result` | taskId, threadId | `{status, result?, error?, failure_reason?, engine_session_id?, duration_ms, final_seq, attachments?}` |
| `thread_message` | taskId, threadId | `{seq, kind, role?, content?, tool?, input?, output?, tool_use_id?, parent_tool_use_id?}` |
| `cron_state` | taskId, cronId | `{kind, schedule_type?, …, round_count, skipped_count, last_status}` |
| `heartbeat` | — | `{activeThreads?, …}` |
| `fs_response` (live-only) | reqId | `{ok, entries? \| stat? \| sha256 + size, error?}` |
| `msg_ack` | top-level `msgId` | — |

`kind` ∈ `{user_input, assistant_text, thinking, tool_use, tool_result, status, error}` (`thinking`/`log`
config-filterable). Bytes never in frames — only descriptors.

The runner emits `assistant_text`, `thinking`, `tool_use`, `tool_result`, `status` and `error`;
`user_input` is defined but not emitted by the runner. `parent_tool_use_id` is present on every event
produced inside a sub-agent (a `Task` tool call) and absent for the main agent's own events — absence
means the main agent. `tool_use_id` is the engine's id for a `tool_use` block, echoed on the matching
`tool_result`, so a reader can pair a call with its return value; `tool_result.output` carries
`{content, is_error}`. Attribution introduces **no hierarchy**: `seq` stays a single per-thread monotonic
stream across all kinds, and there are no sub-threads or parent tasks.

**Engine-free file operations (`remote_ops/`).** `fs_request` lets the host list, stat, read, write,
create, rename and remove files without prompting an engine (protocol §5.1). The transport recognizes the
frame **before** the inbox write and answers it off the socket task: the operation runs on the blocking
pool and its `fs_response` is sent straight to the socket, like `heartbeat`. Neither frame touches `inbox`
or `outbox`, so nothing is replayed after a reconnect, and a response produced for a dropped connection is
discarded. **File contents never ride in a frame**: `put` downloads the blob the host staged on the
attachment byte channel (§9) into a temp file beside the destination, verifies its sha256 and renames it
into place; `get` uploads the file to the byte channel under the name the host chose. Both go through
the attachment backend directly — no content-type allowlist and no copy in the content-addressed cache —
and are bounded by the single-attachment size limit. The group is **off** until the host-assigned config sets `remoteOps.fs` (register config,
`GET /agent/config`, or a live `config_update`); the agent has no local switch and answers `disabled`
meanwhile. **Paths are not confined**: a relative path resolves against `workspace_dir`, an absolute one is
used as given — whoever holds the host's admin token can reach every file the agent's uid can, on every
agent where the group is enabled. Standalone command requests remain deferred.

**Interactive terminals (`remote_ops/pty.rs`).** `remoteOps.pty` enables connection-scoped
`pty_*` frames (protocol §5.2). `pty-process` supplies asynchronous PTY allocation and process
spawning. Each session has ordered bounded input, raw-byte output, resize, cancellation and
explicit child reaping. Disabling PTY revokes active sessions through a configuration epoch.
The socket owns the session registry and a bounded live output channel; both are discarded
and workers joined before reconnect. No terminal frame enters inbox/outbox or starts an engine.

**Multiplexing & ordering (one WS, N parallel threads).** Concurrent threads' frames interleave. The host
**demuxes by `threadId`**, **orders each thread's transcript by `seq`** (per-thread monotonic, assigned
serially by the runner), and writes idempotently on `(thread_id, seq)` (dedup, since at-least-once replay can
duplicate). `thread_result.final_seq` lets the host verify it received `1..final_seq`. `outboxId` is
delivery/ack only; `seq` is the per-thread continuity contract.

## 10b. Local agent API

**Purpose.** A local, per-workspace API through which in-engine tools (the `gino` CLI, skills, a future VM
guest) read and change agent state. It is the one contract they all follow. Target design: **nothing
outside the daemon opens `agent.db`** — the in-engine CLI becomes a client of this API instead of opening
the database itself.

**Transport.** Unix stream socket at `<workspace_dir>/run/agent.sock`.

- The daemon creates `<workspace_dir>/run` with mode **0700** (and chmods an existing dir to 0700), removes
  any stale file at the socket path, binds, then sets the socket file to **0600**.
- The socket lives as long as the daemon: on shutdown the listener stops, open connections close and the
  socket file is removed (best-effort).

**Framing.** Newline-delimited JSON (UTF-8). One request per line, exactly one response line per request,
in order; a connection may carry any number of requests.

```json
{"v":1,"id":"1","method":"cron.list","params":{}}
{"v":1,"id":"1","result":{"crons":[],"scoped":true}}
{"v":1,"id":"2","method":"thread.show","params":{"id":"nope"}}
{"v":1,"id":"2","error":{"code":"not_found","message":"no such thread: nope"}}
```

- `params` is optional (absent = `{}`) and must be a JSON object when present.
- `id` is a client-chosen string, echoed verbatim; it is `null` in the response when the line could not be
  parsed far enough to read a string `id`.
- Every response carries `"v":1` and exactly one of `result` / `error` (`{code, message}`).

**Limits and timeouts.**

| limit | value | on violation |
|---|---|---|
| request line length (excluding `\n`) | 65536 bytes | one `invalid_params` response (id `null`, "request line exceeds 65536 bytes"), then the connection is closed |
| idle connection (no complete request line) | 60 s | connection closed |
| method handler | 10 s | `unavailable` ("request timed out") |
| concurrent connections, total | 64 | connection closed immediately, no response |
| concurrent authenticated connections per run token | 8 | that `auth` returns `unavailable` ("too many connections for this token"); the connection stays unauthenticated |
| concurrent operator connections | 8 | connection closed immediately, no response (counted separately from run tokens) |

**Versioning.** `v` must be the integer `1`. A missing or different `v` returns `unsupported_version`
("unsupported protocol version; this agent speaks v1"). Future versions bump `v`; the daemon answers only
versions it implements.

**Authentication.** A connection's scope is either **operator** (full access) or **run**
(`{task_id, thread_id}`); a new connection has no scope unless it qualifies as operator. Any method other
than `auth` on a connection without scope returns `unauthenticated`.

- **Run tokens.** 256 random bits from `/dev/urandom`, hex-encoded (64 lowercase hex chars). The registry
  stores only the SHA-256 of each token mapped to its scope, so lookups never compare the raw secret. The
  executor registers a token **before spawning** an engine run and **revokes** it when the run ends. The
  engine receives `GINO_AGENT_SOCK` (socket path as seen by the engine), `GINO_AGENT_TOKEN`, and the
  existing `GINO_TASK_ID` / `GINO_THREAD_ID`.
- **`auth`** `{"token":"<hex>"}` → `{"scope":"run","task_id":"…","thread_id":"…"}`. An unknown or revoked
  token returns `unauthenticated` and leaves the connection with **no** scope — an operator connection that
  presents a bad token loses operator access too.
- **Revocation.** Once revoked, every later request on a connection that authenticated with the token gets
  `unauthenticated`, and new `auth` with it fails.
- **Operator access.** At accept time the server reads the peer credentials (`SO_PEERCRED`). The connection
  is operator-scoped iff the peer uid equals the daemon's uid **and** the peer pid is known **and** the
  targets of `/proc/<pid>/ns/user` and `/proc/<pid>/ns/pid` equal the daemon's own
  (`/proc/self/ns/{user,pid}`). Any read failure → not operator. A sandboxed engine runs in nested
  namespaces, so it never gets operator access even when its outer uid matches. Operator connections need
  no `auth`.
- **A presented token always wins:** an operator connection that authenticates with a valid token becomes
  run-scoped.

**Run-scope rules.** A run-scoped caller only sees and changes its own `task_id`:

- an explicit `task_id` param that is not its own → `forbidden`;
- a task, thread or cron named by id that belongs to another task → `not_found`, with exactly the
  response a missing id gets, so another task's objects are indistinguishable from nonexistent ones
  (and nothing is changed);
- listings are filtered to its own task; `system.info` is redacted (below).

**Threat-model limit.** Unsandboxed engines run as the daemon's uid and can read another engine's token
from `/proc/<pid>/environ`, so run scope is a hard boundary **only under the engine sandbox** (§9d,
separate PID namespace) — matching the isolation model.

**Object shapes.** Field names are snake_case; optional DB columns are `null` when empty.

| shape | fields |
|---|---|
| TaskSummary | `id, name, kind, status, created_at` (`name`/`kind` nullable) |
| ThreadSummary | `id, task_id, status, source, attempt, max_attempts, engine_session_id` |
| ThreadDetail | ThreadSummary + `cron_id, failure_reason, engine, duration_ms, prompt, result, error` |
| CronInfo | `id, task_id, kind, status, schedule_type, schedule_value, next_run, last_fired_at, continuity, run_count, prompt, created_at, updated_at` |

**Methods.**

| method | params | result |
|---|---|---|
| `auth` | `{token}` | `{scope:"run", task_id, thread_id}` |
| `system.info` | `{}` | see below |
| `task.list` | `{}` | `{tasks:[TaskSummary]}` |
| `task.show` | `{id}` | `{task:TaskSummary, threads:[ThreadSummary], crons:[CronInfo]}` |
| `thread.list` | `{task_id?}` | `{threads:[ThreadSummary]}` |
| `thread.show` | `{id}` | `{thread:ThreadDetail}` |
| `cron.create` | `{type:"cron"\|"interval"\|"once", value, prompt, continuity?="isolated", task_id?}` | `{cron:CronInfo}` |
| `cron.list` | `{task_id?}` | `{crons:[CronInfo], scoped:bool}` |
| `cron.delete` | `{id}` | `{cron:CronInfo}` (status `cancelled`, `next_run` null) |
| `cron.pause` | `{id}` | `{cron:CronInfo}` (status `paused`, `next_run` kept) |
| `cron.resume` | `{id}` | `{cron:CronInfo}` (status `active`, `next_run` recomputed) |

- **`system.info`.** Operator:
  `{"scope":"operator","version","agent_id","name","host_url","capabilities":["claude"],"workspace_dir","db_path","tasks":N,"active_crons":N,"threads":{"pending":N,"running":N}}`.
  Run: the same without `workspace_dir` / `db_path`, `"scope":"run"`, and counts covering only the caller's
  own task. `version` is the crate version; `agent_id` the persisted identity's id (`null` if not
  registered); `name` / `host_url` the daemon config `agent_name` / `host_url` (nullable). It never
  includes the connect token, the enrollment token or any other task's data.
- **`task.list`.** Run: only its own task, if it exists.
- **`task.show`.** Missing, or (run scope) another task's id → `not_found` ("no such task: <id>").
- **`thread.list`.** Run: defaults to (and only allows) its own task. Operator: `task_id` required
  (`invalid_params` "task_id is required").
- **`thread.show`.** Missing, or (run scope) another task's thread → `not_found` ("no such thread: <id>").
- **`cron.create`.** Run: task defaults to its own; operator: `task_id` required. A `continuity` other than
  `isolated`/`shared` → `invalid_params` ("continuity must be 'isolated' or 'shared' (got '<x>')"); a
  schedule rejected by `next_run_at_creation` → `invalid_params` ("invalid schedule: type=<t> value=<v>:
  <reason>"); unknown task → `not_found` ("no such task: <id>"). The row is built like the CLI's
  `cron create`: id `cron-<uuid v4>`, kind `schedule`, status `active`, `run_count` 0,
  `created_at` = `updated_at` = now.
- **`cron.list`.** Run: all of its own task's crons (every status), `scoped:true`. Operator: with
  `task_id` → that task's crons, `scoped:true`; without → active crons across all tasks, `scoped:false`.
- **`cron.delete` / `cron.pause` / `cron.resume`.** Missing, or (run scope) another task's cron →
  `not_found` ("no such cron: <id>"), with no side effects. `cron.resume` recomputes `next_run` forward from now via
  `next_run_at_creation` (`null` if that fails).
- **Cron side effects.** Every successful cron mutation (`create`, `delete`, `pause`, `resume`) updates
  `updated_at`, enqueues a `cron_state` outbox row (best-effort; a failure is logged, not returned), then
  nudges the scheduler and the transport flush.

**Error codes** (stable snake_case strings).

| code | meaning |
|---|---|
| `unauthenticated` | no valid scope on this connection (no `auth` yet, bad token, revoked token) |
| `forbidden` | an explicit `task_id` param outside the caller's scope |
| `not_found` | the referenced task / thread / cron does not exist, or is outside the caller's scope |
| `invalid_params` | malformed JSON, missing / ill-typed fields, invalid schedule, unknown method ("unknown method: <name>") |
| `unsupported_version` | `v` missing or not `1` |
| `unavailable` | handler timeout, connection limits, daemon shutting down |
| `internal` | unexpected daemon-side failure (e.g. a database error); generic message |

**Reachability.** The protocol is the same everywhere; only the path differs.

| run | socket as seen by the engine |
|---|---|
| unsandboxed | `<workspace_dir>/run/agent.sock` directly |
| sandboxed (§9d) | bound read-write at `/run/gino/agent.sock` |
| VM (later) | a guest vsock port that libkrun maps to the same host Unix socket — no protocol change |

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
