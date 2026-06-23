# Gino docker-compose end-to-end test

A black-box, docker-orchestrated e2e that runs the **real host** (`src/`,
protocol-v2 control plane) and a **real gino-agent** (`gino-agent/`, the Rust
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
  credential-free, 50/50 checks. This file + `TEST-COVERAGE.md`.
- **Real flow (`run-real.sh`)** — the **real Claude CLI** drives a **real model**
  through the host `/llm` proxy to **real Anthropic**. Needs a credential you supply
  (`GINO_PROXY_ANTHROPIC_API_KEY` or `_OAUTH_TOKEN`). See `REAL-FLOW-REPORT.md`.

Exit code is the tester's: `0` = all functional checks passed. Known issues
(see below) are reported but do **not** fail the suite.

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
   (oven/bun)    (bun src/index.ts)        (Dockerfile.agent)
                    │  /llm proxy
                    ▼
                 mock-llm
```

The host serves under `/api`; the runner appends `/agent/*` to `GINO_HOST_URL`
verbatim, so the agent is pointed at `http://host:3000/api`. The agent
self-registers (`POST /agent/register`), opens its WebSocket, and runs tasks via
the fake `claude` engine. The host writes its db/store to a tmpfs (the repo is
bind-mounted read-only), so a run never touches the working tree.

## Coverage (50 checks)

See [`TEST-COVERAGE.md`](TEST-COVERAGE.md) for the full check-by-check matrix
(every request, what it asserts, and the path it proves). Summary:

- **health & version** — `/health`, `/health/ready`, `/system/version`
- **auth gates** — register / admin / `/llm` all fail-closed (401)
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

2. **[OPEN] Per-task close shuts the whole runner down.** `POST /tasks/:id/close`
   emits a protocol `close` frame, but protocol.md §4 defines `close` as
   runner-level *wind-down and exit* (no `taskId`). The runner obeys
   (`close received; shutting down` → `gino-agent stopped`), so closing one task
   kills a multi-task agent. Suggested fix: the host should mark the task closed
   (and `interrupt` any in-flight turn) without emitting the global `close` — or the
   protocol + runner gain a real per-task close frame.

3. **[OPEN] Scheduling a cron before the first message silently fails.** The agent's
   `crons` table FK-references its local `tasks` row, which only exists after a
   `task_upsert` frame — and the host seeds `task_upsert` lazily on the first
   *message*, not the first cron. So `POST /tasks/:id/crons` on a never-messaged task
   returns 201 host-side, but the agent rejects `cron_upsert` with
   `FOREIGN KEY constraint failed` and the cron never applies or fires. Suggested
   fix: the host should seed `task_upsert` on the first cron too (as it does on the
   first message).

Issues 2 and 3 are reported as **KNOWN ISSUES** in the run output and do not fail
the suite; they are design-level decisions left for the maintainer rather than
changed unilaterally.
