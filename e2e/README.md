# Gino docker-compose end-to-end test

A black-box, docker-orchestrated e2e that runs the **real host** (`apps/host/src/`,
control plane) and a **real gino-agent** (`gino-agent/`, the Rust
daemon) side by side and drives the host's entire HTTP surface to exercise every
major feature. It is the first test that runs **host-v2 and runner-v2 together** —
they were built in separate campaigns and had never been integration-tested — so
it doubles as a wire-compatibility check.

## Run it

```bash
./e2e/run.sh            # compile the runner, build images, run the suite, tear down
./e2e/run.sh --rebuild  # force a fresh cargo build of the runner binary
./e2e/run.sh --logs     # also dump host/agent/mock-llm logs after the run
./e2e/run.sh --keep     # leave the stack up afterwards (debugging)
```

Two modes:

- **Hermetic (default, `run.sh`)** — fake engine + mock LLM upstream; deterministic,
  credential-free; the latest run is 79/79 checks. This file + `TEST-COVERAGE.md`.
- **Real flow (`run-real.sh`)** — the **real Claude CLI** drives a **real model**
  through the host `/llm` proxy to **real Anthropic**. Needs a credential you supply
  (`GINO_PROXY_ANTHROPIC_API_KEY` or `_OAUTH_TOKEN`). See `REAL-FLOW-REPORT.md`.

Exit code is the tester's: `0` = all functional checks passed.

Requirements: Docker + compose v2, and a cargo registry cache or network (the
runner is compiled in `rust:1.96-bookworm`). No Anthropic/OpenAI credentials are
needed — the engine and the LLM upstream are both faked.

## What's in here

| File | Role |
|------|------|
| `docker-compose.yml` | orchestration: `mock-llm`, `host`, `agent`, `tester` on one network |
| `Dockerfile.agent` | runtime image for the agent: bookworm-slim + the runner binary + the fake engine |
| `fake-claude` | a `claude` stand-in (bash) that speaks the runner's stream-json control protocol; echoes the prompt back, deterministic, no network |
| `agent-entrypoint.sh` | starts the runner daemon |
| `mock-llm.ts` | canned Anthropic-ish upstream; echoes the path + injected auth so the `/llm` proxy can be asserted |
| `e2e.ts` | the test driver — drives the host API and asserts end-to-end behaviour |
| `run.sh` | builds the runner binary (warm cargo cache) and orchestrates the run |
| `gino-agent.bin` | the staged runner binary (gitignored; produced by `run.sh`) |

## Topology

```
 tester ──HTTP──▶ host ◀──WS / HTTP── agent (runner + fake claude engine)
   (oven/bun)    (bun apps/host/src/index.ts)  (Dockerfile.agent)
                    │  /llm proxy
                    ▼
                 mock-llm
```

The host serves under `/api`; the runner appends `/agent/*` to `GINO_HOST_URL`
verbatim, so the agent is pointed at `http://host:3000/api`. The agent
self-registers (`POST /agent/register`), opens its WebSocket, and runs tasks via
the fake `claude` engine. The host writes its db/store to a tmpfs (the repo is
bind-mounted read-only), so a run never touches the working tree.

## Coverage (79 checks, latest run)

See [`TEST-COVERAGE.md`](TEST-COVERAGE.md) for the full check-by-check matrix
(every request, what it asserts, and the path it proves). Summary:

- **health & version** — `/health`, `/health/ready`, `/system/version`
- **auth gates** — register / admin / `/llm` all fail-closed (401), plus the
  admin/channel token split (HOST-XC-02): the channel token on `/admin` or
  `/metrics` and the admin token on `/messages` are all rejected (401, no
  fallback), while the admin token is accepted on `/admin`
- **agent enrollment** — the real runner registers + attaches over WS, shows online
- **task round-trip** — create → auto-dispatch → message → the runner executes via
  the engine → `thread_result` → the transcript mirrors the echoed prompt
  (full host→ws→runner→engine→ws→host→admin path, content-verified)
- **channels** — ingress `/send` (new task + continuation) → run → egress `/poll`
- **crons** — `cron_upsert` → agent applies + fires → `cron_state` → host mirror
  (proves a real fire via `roundCount ≥ 1`) → `cron_delete`
- **attachments** — connect-token byte channel: ingest, download by id + sha256,
  and the cross-agent IDOR guard (403)
- **/llm proxy** — credential injection + `/llm` prefix stripping (verified via the
  mock upstream) + the `426` upgrade gate
- **config provisioning** — artifact upload (+ dedup), global desired-set entries,
  resolved targets delivered on register
- **agent OOB control** — stop / remove / grant / revoke
- **task lifecycle** — interrupt (non-destructive) + close

## Findings

This e2e found three host-v2 ↔ runner-v2 interop bugs on first integration.

1. **[FIXED] Runner rejected the host's register config.** The runner's host-config
   overlay used `#[serde(deny_unknown_fields)]`, so it rejected the spec-mandated
   `wsUrl`/`configUrl`/`outputUrl` fields the host sends in the register config —
   registration failed outright (`unknown field 'configUrl'`). protocol.md §1/§4
   states config objects are **open** (unknown fields accepted). Fixed in
   `gino-agent/src/config.rs` by dropping `deny_unknown_fields` on the host-config
   overlays (and inverting the corresponding unit test).

2. **[FIXED] Per-task close shut the whole runner down.** `POST /tasks/:id/close`
   emitted a protocol `close` frame, which protocol.md §4 defines as runner-level
   *wind-down and exit* (no `taskId`) — the runner obeyed and the whole agent
   exited, so closing one task killed a multi-task agent. Fixed: the host no
   longer emits the runner-level `close` for a single task, and the suite now
   **hard-asserts** the agent keeps serving other tasks after a per-task close.

3. **[FIXED] Scheduling a cron before the first message silently failed.** The agent's
   `crons` table FK-references its local `tasks` row, which only exists after a
   `task_upsert` frame — and the host seeds `task_upsert` lazily on the first
   *message*, not the first cron. So `POST /tasks/:id/crons` on a never-messaged task
   returned 201 host-side while the agent rejected `cron_upsert` with
   `FOREIGN KEY constraint failed` and the cron never applied or fired. Fixed
   agent-side (P2-19): a `cron_upsert` for an unknown task seeds a minimal task
   row. The suite now hard-asserts the cron applies AND fires in that order.
