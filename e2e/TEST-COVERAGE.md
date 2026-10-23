# E2E test coverage

What the docker-compose e2e (`./e2e/run.sh`) actually exercises, check by check.
Latest run: **79/79 functional checks passed**.

The suite is a **black-box integration test**: it drives only the host's public
HTTP API and observes effects through it, while a **real gino-agent** does the
work over its WebSocket. Nothing is stubbed in-process — the only fakes are at the
system edges (the LLM engine and the LLM upstream), so every layer in between is
the real code path.

## Test environment

| Component | What it is | Why |
|-----------|-----------|-----|
| `host` | the real control plane, `bun apps/host/src/index.ts`, repo bind-mounted read-only | the system under test |
| `agent` | the real Rust `gino-agent` binary + a fake `claude` engine (`fake-claude`) and a fake `codex` engine (`fake-codex`) | the real WS/runner code path on BOTH backends, no model/network |
| `mock-llm` | a tiny Bun server emulating the Anthropic upstream | lets `/llm` proxy be tested without real credentials |
| `tester` | the Bun test driver (`e2e.ts`) | drives the host API + asserts |

- **`fake-claude`** speaks the runner's stream-json control protocol (`system/init`
  → `assistant` text → `result/success`) and echoes the user's prompt back, so a
  task run is deterministic and content-verifiable end to end. No LLM is called.
- **`fake-codex`** speaks the codex backend's JSONL `ThreadEvent` stream
  (`thread.started` → `reasoning` / `command_execution` / `agent_message` items →
  `turn.completed`) and echoes the prompt back. The runner probes for a `codex`
  binary on `PATH` and registers/advertises the backend only when it finds one,
  so shipping it is what makes the agent multi-engine here.
- **`mock-llm`** answers every request 200 and echoes the request path + the auth
  header it received, so the proxy's prefix-strip and credential injection are
  observable.
- The host writes its db/store to a **tmpfs** (repo mounted read-only) — a run never
  touches the working tree. No Anthropic/OpenAI credentials are required.
- Liveness is probed by **work** (does a fresh task actually run?), not the `online`
  flag — a dead agent still reads online for the offline-grace window (~3×heartbeat).

## Coverage matrix (79 checks across 12 groups)

### 1. Health & version — 3 checks
| Check | Request | Asserts |
|-------|---------|---------|
| status=ok | `GET /health` | 200, `{status:"ok"}` |
| ready (200) | `GET /health/ready` | DB reachable → `ready` |
| 200 + data | `GET /system/version` | build info envelope |

### 2. Auth gates (fail-closed) — 7 checks
| Check | Request | Asserts |
|-------|---------|---------|
| register 401 | `POST /agent/register` w/ wrong register token | 401 |
| admin 401 | `GET /admin/agents` w/o bearer | 401 |
| admin w/ channel token 401 | `GET /admin/agents` w/ the CHANNEL bearer | 401 — the channel token does not reach the control plane (HOST-XC-02, no fallback) |
| metrics w/ channel token 401 | `GET /metrics` w/ the CHANNEL bearer | 401 — metrics is an admin-only surface |
| messages w/ admin token 401 | `POST /messages/:id/poll` w/ the ADMIN bearer | 401 — the admin token does not reach the user I/O plane |
| admin w/ admin token 200 | `GET /admin/agents` w/ the ADMIN bearer | 200 — the right token works |
| llm 401 | `GET /llm/v1/models` w/o connect token | 401 |

Covers all four token kinds (register / channel / admin / connect) rejecting
unauthenticated calls, plus the admin/channel privilege separation in both
directions (wrong-kind-of-token → 401).

### 3. Real agent enrollment over WebSocket — 3 checks
| Check | Observed via | Asserts |
|-------|--------------|---------|
| online=true | `GET /admin/agents` (poll ≤45s) | the runner self-registered (`POST /agent/register`), opened `/agent/ws/:id`, and shows `online` |
| capability claude | same | advertised engine capability persisted |
| capability codex | same | the codex backend's `PATH` probe found `fake-codex`, so the runner advertises the second engine too |

Proves the full bootstrap: `register → connect-token → WS attach → liveness`.

### 4. Task round-trip through the real runner — 14 checks
The core path: `host → outbox → WS → runner → engine → WS → host → admin`.
| Check | Request | Asserts |
|-------|---------|---------|
| granted | `POST /admin/agents/:id/grant {owner}` | owner→agent access (OD13) |
| dispatch placed | `POST /admin/tasks` | auto-dispatch placed the task |
| placed on real agent | same | placement chose the online runner |
| seeded | `POST /admin/tasks/:id/message` | first message seeds `task_upsert` |
| threadId returned | same | a thread is opened |
| thread run succeeded | `GET /admin/tasks/:id` (poll ≤40s) | the runner executed the engine and reported `thread_result:succeeded` |
| transcript carries the echoed nonce | same | the M5 transcript mirrors the engine output; the unique prompt nonce survived host→runner→engine→host **with content fidelity** |
| transcript has the Task tool_use | same | the `Task` `tool_use` frame reached the host, keyed by the engine's `tool_use_id` |
| tool_use input preserved | same | the tool `input` object survived the hop verbatim (`description`) |
| tool_use is main-agent attributed | same | the opening `Task` call carries **no** `parent_tool_use_id` — the main agent made it |
| transcript has the matching tool_result | same | the `tool_result` closing the same `tool_use_id`, `{content, is_error:false}` |
| sub-agent text carries parent_tool_use_id | same | the sub-agent's own `assistant_text` is attributed to the `Task` call that spawned it |
| main-agent text is unattributed | same | the main agent's reply has no `parent_tool_use_id` — absence means the main agent (protocol §5) |
| seq stays one strictly increasing stream | same | attribution adds no hierarchy: one per-thread monotonic `seq` across main- and sub-agent frames |

### 4b. Codex engine round-trip through the real runner — 14 checks
The SAME runner, the OTHER backend: a task whose `engine` is `codex` is placed on
an agent that ADVERTISED codex, then routed at spawn time onto the codex backend,
and its JSONL items mirror as transcript rows.
| Check | Request | Asserts |
|-------|---------|---------|
| capabilities.engines includes codex | the enrollment group's advertised capabilities | fail-fast precondition: placement matches the task's `engine` against the agent's advertised engines, so an agent without it leaves the task unplaced in `pending_assignment` — an opaque timeout instead of a diagnosis |
| granted | `POST /admin/agents/:id/grant {owner}` | owner→agent access |
| dispatch placed | `POST /admin/tasks {engine:"codex"}` | the task frame's `engine` is free-form, but its VALUE is matched against `capabilities.engines` at placement; `placed:false, reason:"no-permitted-agent"` here means that match failed |
| placed on real agent | same | placement chose the online runner |
| seeded | `POST /admin/tasks/:id/message` | the first message seeds a `task_upsert` carrying `engine: "codex"` |
| thread run succeeded | `GET /admin/tasks/:id` (poll ≤40s) | the runner drove `codex exec --json` to `turn.completed` |
| transcript carries the echoed nonce | `GET /admin/tasks/:id?thread=` | the `agent_message` item round-tripped the prompt with content fidelity |
| transcript has a thinking row | same | a `reasoning` item maps to `thinking` |
| transcript has the command_execution tool_use | same | `item.started` opens a `tool_use` whose `tool` is the codex ITEM KIND, forwarded verbatim (never aliased onto a claude tool name) |
| tool_use input preserved | same | the whole item minus the envelope keys rides as `input` (the `command` field survives) |
| transcript has the matching tool_result | same | `item.completed` closes the SAME id with `is_error: false` |
| no row carries a parent_tool_use_id | same | codex reports no sub-agent linkage, so every event is main-agent attributed |
| seq stays one strictly increasing stream | same | one per-thread sequence across both engines' event shapes |
| engineSessionId is a codex thread id | same | `thread.started` was captured as the resume handle (`codex-e2e-*`) — the claude path could never mint it |

### 5. Channels: ingress → run → egress — 7 checks
| Check | Request | Asserts |
|-------|---------|---------|
| granted | grant owner-ch → agent | access |
| channel id | `POST /admin/channels` | channel registered |
| created new task | `POST /messages/:ch/send` (new conversation) | a task is created + dispatched |
| dispatch placed | same | placed on the agent |
| created=false (continued) | `POST /messages/:ch/send` (same `conversation_ref`) | routing continues the same task |
| same taskId | same | conversation→task binding is stable |
| assistant echo present in egress | `POST /messages/:ch/poll` (poll ≤40s) | the agent's reply (echoed nonce) is surfaced through egress |

Proves the inbound message → task routing → real run → outbound transcript loop.

### 6. Crons: command → agent → host mirror — 7 checks
| Check | Request | Asserts |
|-------|---------|---------|
| cronId + outboxId | `POST /admin/tasks/:id/crons` (after a message seeds the task) | host emits a `cron_upsert` command |
| mirror has the cron | `GET /admin/tasks/:id/crons` (poll ≤25s for `roundCount ≥ 1` — the host seeds a prompt-only row at POST, so only a fire-driven update counts) | the agent applied + **fired** it, reported `cron_state`, and M5 wrote the read-only mirror |
| mirror carries scheduling fields | same | `scheduleType=interval` round-tripped |
| mirror shows it fired (roundCount ≥ 1) | same | a real fire happened on the agent (the scheduling fields are fire-driven) |
| 200 | `DELETE /admin/tasks/:id/crons/:cronId` | `cron_delete` command emitted |
| host accepts the cron (201) | probe: cron on an un-messaged task | host returns 201 |
| cron on un-messaged task applied + fired | same probe (poll ≤15s) | the agent seeds a minimal task row when the cron arrives before its `task_upsert` (P2-19), so the cron applies AND fires |

Proves the OD14 cron model end to end: emit → apply → fire → `cron_state` → mirror.

### 7. Attachments: byte channel + IDOR — 9 checks
Authenticated by the per-agent connect token; authorized by task placement.
| Check | Request | Asserts |
|-------|---------|---------|
| A enrolled / B enrolled | two probe `POST /agent/register` | two connect tokens |
| assigned to A | `POST /admin/tasks/:id/assign {agent_id}` | force placement on probe A |
| size matches | `POST /agent/attachments?...` (A's token) | bytes ingested, descriptor returned |
| resolvable wire ref | same | the descriptor's `ref` is the byte-channel path `/agent/attachments/<id>`, never a host-internal storage path |
| 200 + bytes (by id) | `GET /agent/attachments/:id` | content-addressed download |
| x-content-sha256 header | same | integrity header present |
| 200 + bytes (by sha) | `GET /agent/attachments?sha256=...` | download by digest |
| 403 (IDOR) | probe B downloads A's attachment | cross-agent access denied |

### 8. /llm credential proxy — 4 checks
| Check | Request | Asserts |
|-------|---------|---------|
| reached mock upstream | `POST /llm/v1/messages` (connect-token auth) | 200, the proxy forwarded to the upstream |
| /llm prefix stripped | same (mock echoes path) | upstream saw `/v1/messages`, not `/llm/...` |
| real upstream key injected | same (mock echoes auth) | the host injected `sk-ant-host-real`; the engine's token never leaks upstream |
| 426 upgrade required | `GET /llm/v1/responses` (no Upgrade) | WS-only endpoint rejects a plain GET |

### 9. Config / skill provisioning — 3 checks
| Check | Request | Asserts |
|-------|---------|---------|
| dedup → same sha | `POST /admin/config/artifacts` ×2 (same bytes) | content-addressed store dedups |
| targets resolved | `PUT /admin/config/entries {scope:"global"}` | host resolves `CLAUDE.md`→`CLAUDE.md`, dir→`.claude/skills/<name>` |
| register config carries entries | fresh `POST /agent/register` | the resolved global desired-set is delivered on register |

### 9b. Engine-free file operations — 9 checks

Added after the latest recorded run; not yet part of the 79/79 figure above.

| Check | Request | Asserts |
|-------|---------|---------|
| 200 + disabled | `POST /admin/agents/:id/fs {op:"list"}` | the file group is off by default; the agent answers `disabled` |
| remote-ops 200 | `POST /admin/agents/:id/remote-ops {fs:true}` | the switch is stored and pushed; the agent starts accepting requests |
| upload ok / download returns the bytes | `PUT /admin/agents/:id/files?path=` then `GET` | a file uploaded through the host downloads back identically (binary-safe) over the byte channel |
| list shows the file | `fs {op:"list"}` | the directory listing carries the new file |
| rename ok / old path is not_found | `fs {op:"rename"}` then `{op:"stat"}` | rename moves the file |
| remove ok | `fs {op:"remove", recursive:true}` | the directory is deleted |
| disabled again | `remote-ops {fs:false}` then `fs` | switching the group off takes effect live |

No engine runs in this group: the fake engines are never invoked.

### 10. Agent OOB control — 3 checks
| Check | Request | Asserts |
|-------|---------|---------|
| 200 (stop) | `POST /admin/agents/:id/stop` | OOB stop (ws-close fallback for `manual`) |
| 200 (remove) | `POST /admin/agents/:id/remove` | agent removal |
| 200 granted=false (revoke) | `POST /admin/agents/:id/revoke {owner}` | access revoked |

### 11. Task lifecycle: interrupt / close — 5 checks
| Check | Request | Asserts |
|-------|---------|---------|
| granted | grant owner-life → real agent | deterministic placement |
| interrupt 200 | `POST /admin/tasks/:id/interrupt` | host emits `interrupt` |
| agent still runs a new task after interrupt | run a fresh task (≤15s) | interrupt is **non-destructive** — agent keeps executing |
| close 200 | `POST /admin/tasks/:id/close` | host side succeeds |
| agent still runs a new task after a per-task close | run a fresh task (≤12s) | per-task close is **non-destructive** — the runner keeps serving other tasks (hard assertion) |

## Formerly-known issues (now fixed and hard-asserted)

> Three earlier bugs are **fixed** and now hard-asserted: the runner rejecting the
> host's register config via `deny_unknown_fields` (`gino-agent/src/config.rs`);
> a per-task close shutting the whole runner down (the host no longer emits
> the runner-level `close` frame for a single task — see lifecycle check above);
> and a cron scheduled before the task's first message being silently dropped on
> the agent (fixed agent-side by P2-19: a `cron_upsert` for an unknown task seeds
> a minimal task row — see the cron-ordering probe above).

## Terminal integration checks

The terminal suite runs separately from this compose harness:

- `packages/host-hono/tests/integration/real-pty.test.ts`, with `GINO_PTY_TEST_BIN` set to a
  built agent: real Host, Agent and browser WebSocket; default-off enablement, TTY input/output,
  resize, final output/exit status, revocation, no PTY inbox/outbox rows and no engine thread.
- `gino-agent/src/remote_ops/pty/tests.rs`: PTY allocation, binary input, Ctrl-C, invalid opens,
  process reaping, rapid disable/re-enable and closed-consumer cleanup.
- `gino-agent/src/transport/mod.rs`: disconnect/reconnect reaps the old process, ignores stale
  terminal input and never replays terminal frames.
- Host SDK and Hono/standalone tests cover bounded buffers, pending-open cancellation, session
  drop paths, ticket authorization/single use/agent and origin binding, rate limits, and the
  Bun and Node WebSocket adapters. The TLS-termination case binds the authenticated origin.

## Not covered (out of scope for this harness)

- Real LLM engine behaviour (the engine is faked deterministically).
- The host's **docker launcher** spawning agent containers itself (`/admin/agents/launch`
  with a real docker daemon / DinD); here the agent is a sibling compose service
  that self-registers.
- Reconnect/replay under network partition, outbox retry exhaustion, and the
  offline-grace sweep timing (only the steady-state online path is exercised).
- Codex/OpenAI proxy bridging and OAuth refresh (only the Anthropic `/llm` path);
  the codex ENGINE is covered above, its credential proxy path is not.
