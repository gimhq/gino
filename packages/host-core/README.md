# @gino/host-core

The Gino host SDK core: the agent protocol (v2 frames), the domain (agents,
tasks, crons, transcript, config store) and the ports it runs on. No HTTP
framework, no Bun-only API, no `process.env`, no timers started implicitly.
Design: `docs/host-sdk-design.md`.

## Install

Workspace members depend on it as `"@gino/host-core": "workspace:*"`. Publishing
to the internal registry is a P3 item; until then consume it from this repo.
Bun resolves the `bun` export condition to `src/`; Node consumers get `dist/`
(`bun run --filter @gino/host-core build`).

## Minimal host

```ts
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";

const host = createHost({
  storage: memoryStorage(),                 // repositories + blob store
  logger: noopLogger,                       // any pino-compatible subset
  agents: { registerToken: "…", registrationEnabled: true },
  attachments: { maxUploadBytes: 64 << 20 },
});

host.start();                               // liveness sweep, outbox GC, config GC, launch reaper (unref'd)
host.stop();                                // closes agent sockets, timers and the storage
```

SQLite through `@gino/host-store-sqlite`, with either driver:

```ts
import { openDatabase, sqliteStorage } from "@gino/host-store-sqlite";
import { bunSqliteDriver } from "@gino/host-store-sqlite/bun";    // Bun runtime
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node";  // Node runtime (better-sqlite3)

const db = openDatabase({ driver: nodeSqliteDriver, path: "data/gino.db" }); // migrates on open
const host = createHost({ storage: sqliteStorage(db, { storeDir: "data/store" }), logger });
```

One host per database file. Everything on `host.agents`, `host.tasks`,
`host.crons`, `host.transcript` and `host.config` takes and returns plain data;
failures are `AppError` subclasses (`NotFoundError`, `ForbiddenError`,
`UnauthorizedError`, `ValidationError`, `PayloadTooLargeError`) so an adapter can
map them to HTTP without the SDK knowing HTTP.

## Mounting the data plane

`host.wire` is the whole agent-facing contract, framework-free:

| member | maps to |
|---|---|
| `register(body, { bearer })` | `POST /agent/register` (returns `HttpResult { status, headers?, body }`) |
| `resolveAgent(connectToken)` | connect-token auth on every other route |
| `configFor(agentId)` | `GET /agent/config/:agentId` |
| `ingestHttpFrame(agentId, raw)` | `POST /agent/output/:agentId` |
| `attachSocket` / `onSocketMessage` / `detachSocket` | WebSocket open / message / close (`WsLike` is `{ send, close }`) |
| `attachments.{serveById, serveBySha, ingest}` | the attachment byte channel |

The WebSocket lifecycle ordering (attach → online → replay; detach → offline
only when the current socket was evicted) is protocol-visible: call these three
in that order and do not re-implement them.

`@gino/host-hono` is the ready-made mount:

```ts
import { adminRouter, agentRouter, errorHandler, messagesRouter } from "@gino/host-hono";
import { bunWebSocket } from "@gino/host-hono/bun";      // Bun:  { upgradeWebSocket, websocket }
import { nodeWebSocket } from "@gino/host-hono/node";    // Node: { upgradeWebSocket, injectWebSocket } over @hono/node-ws

const { upgradeWebSocket } = bunWebSocket();             // or nodeWebSocket(app)
app.route("/api/agent", agentRouter(host, { upgradeWebSocket }));
app.route("/api/admin", adminRouter(host, { authorize }));     // authorize: (c) => Principal | null
app.route("/api/messages", messagesRouter(host, { authorize }));
app.onError(errorHandler);
```

Rate limiting, CORS, request ids, logging middleware and bearer-token policy
are the embedder's; apply them before the mounts.

## Events

```ts
host.events.on("thread.result", ({ taskId, threadId, data }) => { /* … */ });
```

| event | payload |
|---|---|
| `transcript.message` | `{ agentId, taskId, threadId, seq, data }` |
| `thread.result` | `{ agentId, taskId, threadId, source, cronId?, data }` |
| `cron.state` | `{ agentId, taskId, cronId, data }` |
| `agent.enrolled` / `agent.online` / `agent.removed` | `{ agentId }` |
| `agent.offline` | `{ agentId, reason: "close" \| "sweep" }` |
| `task.placed` | `{ taskId, agentId }` |
| `outbox.acked` | `{ agentId, id, type: "ack" \| "msg_ack" }` |

Events fire synchronously after the durable write. They are at-least-once
hints, never the record: the store is the source of truth, so re-read through
`host.transcript.get(threadId, { afterSeq })` rather than relying on a listener
having seen every message. A throwing listener is logged and never reaches the
write path. `on` returns the unsubscribe function.

## Storage adapters

`Storage` (`src/host/ports/storage.ts`) is the port every adapter implements:
`memoryStorage()` ships here, `sqliteStorage(db)` in `@gino/host-store-sqlite`.
Any adapter must pass the conformance suite exported as
`@gino/host-core/testing`; it is runner-agnostic and takes the primitives of
`bun:test` or `node:test` + `expect`:

```ts
import { describeStorageConformance } from "@gino/host-core/testing";

describeStorageConformance({ name: "mine", open: () => ({ storage, tamperBlob, close }) },
  { describe, test, beforeEach, afterEach, expect });
```

## `@gino/host-core/domain`

Re-exports the domain modules wholesale for the sibling `@gino/host-*`
packages. Semi-internal: not part of the frozen API (P3 decides what
graduates). `dist/index.js` itself re-exports through this entry, so under Node
there is one copy of every class regardless of which entry you import from.

## Example

`examples/embed-node` embeds the SDK in a plain Node process (`@hono/node-server`
+ `@hono/node-ws` + better-sqlite3) and round-trips one task against the real
`gino-agent` runner: `bun run example:embed-node` from the repo root.
