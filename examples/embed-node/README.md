# examples/embed-node

The Gino host SDK embedded in a **plain Node process**, round-tripping one task
against the **real `gino-agent` runner** — no Bun at runtime, no docker.

It proves the P2 claim of `docs/host-sdk-design.md` §6: `@gino/host-core`,
`@gino/host-store-sqlite` (better-sqlite3 driver) and `@gino/host-hono`
(`@hono/node-ws` upgrade) compose into a working protocol-v2 host under Node,
consumed through the packages' `dist/` builds exactly as an external embedder
would consume them.

## What it does

`src/server.ts`

- `createHost({ storage: sqliteStorage(openDatabase({ driver: nodeSqliteDriver, … })), logger, agents })`
- mounts `agentRouter` under `/api/agent` (the runner's `GINO_HOST_URL` is `http://127.0.0.1:PORT/api`)
- serves the Hono app with `@hono/node-server`, WebSocket upgrades injected by `nodeWebSocket(app)`
- logs every `thread.result` from `host.events`
- stops cleanly on SIGINT/SIGTERM (`host.stop()` closes agent sockets and the database)

`scripts/roundtrip.ts` (run by `node`)

1. starts the server in-process on a free port
2. spawns `e2e/gino-agent.bin` with `e2e/fake-claude` first on `PATH` as `claude`
3. waits for `agent.online`, creates a task pinned to that agent, sends `hello from embed-node`
4. awaits `thread.result`, asserts the transcript echoes the prompt
5. SIGTERMs the runner, stops the host, removes its temp dir, exits 0

Expected event order: `agent.enrolled → agent.online → task.placed → transcript.message → thread.result`.

## Prerequisites

```bash
bun install                                  # at the repo root
bun run build                                # or per package: host-core, host-store-sqlite, host-channels, host-hono
bash e2e/run.sh --build-only                 # stages e2e/gino-agent.bin (docker + cargo; scripts/roundtrip.sh does this when missing)
```

Node 24+ (`node` on PATH) — the example runs `.ts` through Node's type stripping,
so relative imports carry `.ts` and there are no enums or parameter properties.

## Run

```bash
bun run --filter @gino/example-embed-node e2e     # the round-trip
bun run --filter @gino/example-embed-node start   # just the server (PORT, GINO_REGISTER_TOKEN, GINO_DB_PATH, GINO_STORE_DIR)
bun run --filter @gino/example-embed-node check   # lint + typecheck (hermetic)
```

`check` deliberately excludes the round-trip: it needs the staged runner binary
(and docker to build it), so it is not hermetic. The root `check` chain runs the
example's `check`; the round-trip is opt-in via `e2e`.

## Operator API

Unauthenticated, loopback by default — this is an example, not a deployment.

| method | path | body | result |
|---|---|---|---|
| GET | `/health` | | `{ status: "ok" }` |
| GET | `/api/agents` | | `{ agents: AgentRow[] }` (without the connect-token hash) |
| POST | `/api/tasks` | `{ title?, ownerId, agentId?, engine?, model? }` | `201 { task, dispatch }` — with `agentId` the owner is granted on that agent and the task pinned to it (`host.tasks.assign`); without, `host.tasks.place` |
| POST | `/api/tasks/:id/messages` | `{ text }` | `{ threadId, seeded, outboxId }` (`host.tasks.send`; the first send also seeds `task_upsert`) |
| GET | `/api/tasks/:id/threads` | | `{ taskId, threads: [{ threadId, run }] }` |
| GET | `/api/tasks/:id/threads/:threadId` | | `{ taskId, threadId, run, messages }` |

`engine` defaults to `claude`, `model` to `sonnet`. `AppError`s map to the
protocol error envelope through `errorHandler` from `@gino/host-hono`.

## Not mounted here

The Bun-only pieces of the standalone host — the LLM proxy's WebSocket bridge
(`@gino/host-proxy`, D11) and the `bun:sqlite` driver — are simply not used.
Channels, the admin router, rate limiting, CORS, OpenAPI docs and metrics are
the embedder's choice and left out of this minimal example.
