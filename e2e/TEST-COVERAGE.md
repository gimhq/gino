# E2E test coverage

What the docker-compose e2e (`./e2e/run.sh`) actually exercises, check by check.
Latest run: **51/51 functional checks passed**, 1 known issue reported (see end).

The suite is a **black-box integration test**: it drives only the host's public
HTTP API and observes effects through it, while a **real gino-agent** does the
work over its WebSocket. Nothing is stubbed in-process — the only fakes are at the
system edges (the LLM engine and the LLM upstream), so every layer in between is
the real code path.

## Test environment

| Component | What it is | Why |
|-----------|-----------|-----|
| `host` | the real control plane, `bun src/index.ts`, repo bind-mounted read-only | the system under test |
| `agent` | the real Rust `gino-agent` binary + a fake `claude` engine (`fake-claude`) | the real WS/runner code path, no model/network |
| `mock-llm` | a tiny Bun server emulating the Anthropic upstream | lets `/llm` proxy be tested without real credentials |
| `tester` | the Bun test driver (`e2e.ts`) | drives the host API + asserts |

- **`fake-claude`** speaks the runner's stream-json control protocol (`system/init`
  → `assistant` text → `result/success`) and echoes the user's prompt back, so a
  task run is deterministic and content-verifiable end to end. No LLM is called.
- **`mock-llm`** answers every request 200 and echoes the request path + the auth
  header it received, so the proxy's prefix-strip and credential injection are
  observable.
- The host writes its db/store to a **tmpfs** (repo mounted read-only) — a run never
  touches the working tree. No Anthropic/OpenAI credentials are required.
- Liveness is probed by **work** (does a fresh task actually run?), not the `online`
  flag — a dead agent still reads online for the offline-grace window (~3×heartbeat).

## Coverage matrix (51 checks across 11 groups)

### 1. Health & version — 3 checks
| Check | Request | Asserts |
|-------|---------|---------|
| status=ok | `GET /health` | 200, `{status:"ok"}` |
| ready (200) | `GET /health/ready` | DB reachable → `ready` |
| 200 + data | `GET /system/version` | build info envelope |

### 2. Auth gates (fail-closed) — 3 checks
| Check | Request | Asserts |
|-------|---------|---------|
| register 401 | `POST /agent/register` w/ wrong register token | 401 |
| admin 401 | `GET /admin/agents` w/o channel bearer | 401 |
| llm 401 | `GET /llm/v1/models` w/o connect token | 401 |

Covers all three token kinds (register / channel / connect) rejecting unauthenticated calls.

### 3. Real agent enrollment over WebSocket — 2 checks
| Check | Observed via | Asserts |
|-------|--------------|---------|
| online=true | `GET /admin/agents` (poll ≤45s) | the runner self-registered (`POST /agent/register`), opened `/agent/ws/:id`, and shows `online` |
| capability claude | same | advertised engine capability persisted |

Proves the full bootstrap: `register → connect-token → WS attach → liveness`.

### 4. Task round-trip through the real runner — 7 checks
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

### 6. Crons: command → agent → host mirror — 6 checks (+1 known issue)
| Check | Request | Asserts |
|-------|---------|---------|
| cronId + outboxId | `POST /admin/tasks/:id/crons` (after a message seeds the task) | host emits a `cron_upsert` command |
| mirror has the cron | `GET /admin/tasks/:id/crons` (poll ≤25s) | the agent applied + **fired** it, reported `cron_state`, and M5 wrote the read-only mirror |
| mirror carries scheduling fields | same | `scheduleType=interval` round-tripped |
| mirror shows it fired (roundCount ≥ 1) | same | a real fire happened on the agent (the mirror is fire-driven) |
| 200 | `DELETE /admin/tasks/:id/crons/:cronId` | `cron_delete` command emitted |
| host accepts the cron (201) | probe: cron on an un-messaged task | host returns 201 (→ see known issue #1) |

Proves the OD14 cron model end to end: emit → apply → fire → `cron_state` → mirror.

### 7. Attachments: byte channel + IDOR — 8 checks
Authenticated by the per-agent connect token; authorized by task placement.
| Check | Request | Asserts |
|-------|---------|---------|
| A enrolled / B enrolled | two probe `POST /agent/register` | two connect tokens |
| assigned to A | `POST /admin/tasks/:id/assign {agent_id}` | force placement on probe A |
| size matches | `POST /agent/attachments?...` (A's token) | bytes ingested, descriptor returned |
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

## Known issues (reported, do not fail the suite)

These are pre-existing host↔runner design gaps the suite surfaces and reports
without changing wire semantics unilaterally.

1. **Cron before the first message fails on the agent.** The agent's `crons` table
   FK-references its local `tasks` row, created only by a `task_upsert` frame — which
   the host seeds lazily on the first *message*, not the first cron. So
   `POST /admin/tasks/:id/crons` on a never-messaged task returns 201 host-side, but
   the agent rejects `cron_upsert` with `FOREIGN KEY constraint failed` and the cron
   never applies/fires. Suggested fix: seed `task_upsert` on the first cron too.

> Two earlier bugs are **fixed** and now hard-asserted: the runner rejecting the
> host's register config via `deny_unknown_fields` (`gino-agent/src/config.rs`),
> and a per-task close shutting the whole runner down (the host no longer emits
> the runner-level `close` frame for a single task — see lifecycle check above).

## Not covered (out of scope for this harness)

- Real LLM engine behaviour (the engine is faked deterministically).
- The host's **docker launcher** spawning agent containers itself (`/admin/agents/launch`
  with a real docker daemon / DinD); here the agent is a sibling compose service
  that self-registers.
- Reconnect/replay under network partition, outbox retry exhaustion, and the
  offline-grace sweep timing (only the steady-state online path is exercised).
- Codex/OpenAI proxy bridging and OAuth refresh (only the Anthropic `/llm` path).
