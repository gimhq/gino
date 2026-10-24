# @gino/host-core

The Gino host SDK core: the agent protocol (frames), the domain (agents,
tasks, crons, transcript, config store) and the ports it runs on. No HTTP
framework, no Bun-only API, no `process.env`, no timers started implicitly.
Design: `docs/host-sdk-design.md`.

## Install

Workspace members depend on it as `"@gino/host-core": "workspace:*"`. Publishing
to the internal registry is a P3 item; until then consume it from this repo.

```jsonc
{
  "dependencies": {
    "@gino/host-core": "workspace:*"
  }
}
```

Bun resolves the `bun` export condition to `src/`; Node consumers get `dist/`
(`bun run --filter @gino/host-core build`). Three entries are published: `.`
(the frozen API below), `./domain` (semi-internal, for the sibling `@gino/host-*`
packages) and `./testing` (the storage conformance suite).

## Quickstart

```ts
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";

const host = createHost({
  storage: memoryStorage(),                 // required — repositories + blob store
  logger: noopLogger,                       // required — any pino-compatible subset
  agents: { registerToken: "…", registrationEnabled: true },
  attachments: { maxUploadBytes: 64 << 20 },
});

host.start();                               // liveness sweep, outbox GC, config GC, launch reaper (unref'd)
```

Only `storage` and `logger` are required; every other option has a default (see
`DEFAULT_HOST_OPTIONS`). Construction is pure — no timer runs and no socket
opens until `start()`. `stop()` closes agent sockets, stops the timers and
closes the storage you passed in, in that order.

Drive one task end to end through the namespaces:

```ts
const task = host.tasks.create({ ownerId: "alice", engine: "claude", model: "sonnet" });
host.tasks.place(task.id);                  // owner → a permitted agent, pinned once
const { threadId } = host.tasks.send(task.id, { text: "summarise the changelog" });

host.events.on("thread.result", (evt) => {
  if (evt.threadId !== threadId) return;
  const messages = host.transcript.get(threadId, { limit: 200 });
  console.log(evt.data.status, messages.length);
});
```

Everything on `host.agents`, `host.tasks`, `host.crons`, `host.transcript` and
`host.config` takes and returns plain data; failures are `AppError` subclasses
(`NotFoundError`, `ForbiddenError`, `UnauthorizedError`, `ValidationError`,
`PayloadTooLargeError`) so an adapter can map them to HTTP without the SDK
knowing HTTP. One host per database file.

## Mounting the data plane

`host.wire` is the whole agent-facing contract, framework-free:

| member | maps to |
|---|---|
| `register(body, { bearer })` | `POST /agent/register` (returns `HttpResult { status, headers?, body }`) |
| `resolveAgent(connectToken)` | connect-token auth on every other route |
| `authorizeAgent(agentId, connectToken)` | per-route check when the id comes from the path |
| `configFor(agentId)` | `GET /agent/config/:agentId` |
| `ingestHttpFrame(agentId, raw)` | `POST /agent/output/:agentId` |
| `attachSocket` / `onSocketMessage` / `detachSocket` | WebSocket open / message / close (`WsLike` is `{ send, close }`) |
| `attachments.{serveById, serveBySha, ingest}` | the attachment byte channel |

The WebSocket lifecycle ordering (attach → online → replay; detach → offline
only when the current socket was evicted) is protocol-visible: call these three
in that order, always with the same socket handle, and do not re-implement them.
A `WsLike` whose identity changes per event must expose the stable socket as
`raw` — the wire compares that.

`@gino/host-hono` is the ready-made mount. Under **Bun**, take the shared
upgrade pair and register `websocket` on the server:

```ts
import type { HostEnv } from "@gino/host-hono";
import { agentRouter, errorHandler } from "@gino/host-hono";
import { bunWebSocket } from "@gino/host-hono/bun";
import { Hono } from "hono";

const app = new Hono<HostEnv>();
const { upgradeWebSocket, websocket } = bunWebSocket();
app.route("/api/agent", agentRouter(host, { upgradeWebSocket }));
app.onError(errorHandler);

Bun.serve({ port: 3000, fetch: app.fetch, websocket });
```

Under **Node** the upgrade is bound to the app and injected into the server
after it is listening:

```ts
import type { HostEnv } from "@gino/host-hono";
import { agentRouter, errorHandler } from "@gino/host-hono";
import { nodeWebSocket } from "@gino/host-hono/node";
import { serve } from "@hono/node-server";
import { Hono } from "hono";

const app = new Hono<HostEnv>();
const { upgradeWebSocket, injectWebSocket } = nodeWebSocket(app);
app.route("/api/agent", agentRouter(host, { upgradeWebSocket }));
app.onError(errorHandler);

const server = serve({ fetch: app.fetch, port: 3000, hostname: "127.0.0.1" });
injectWebSocket(server);
```

`examples/embed-node` is the working Node reference, including the optional
operator routes. The admin and channel-facing routers are opt-in and take the
embedder's own authorization:

```ts
app.route("/api/admin", adminRouter(host, { authorize }));       // authorize: (c) => Principal | null
app.route("/api/messages", messagesRouter(host, { authorize }));
```

Rate limiting, CORS, request ids, logging middleware and the bearer-token policy
are the embedder's; apply them before the mounts.

## Events

```ts
const off = host.events.on("thread.result", ({ taskId, threadId, data }) => { /* … */ });
```

| event | payload |
|---|---|
| `transcript.message` | `{ agentId, taskId, threadId, seq, data }` |
| `thread.result` | `{ agentId, taskId, threadId, source, cronId?, data }` |
| `cron.state` | `{ agentId, taskId, cronId, data }` |
| `agent.enrolled` / `agent.online` / `agent.removed` | `{ agentId }` |
| `agent.offline` | `{ agentId, reason: "close" \| "sweep" }` |
| `task.placed` | `{ taskId, agentId }` |
| `outbox.acked` | `{ agentId, id }` |

**Events are hints, never the record.** Each one fires synchronously AFTER the
durable write it describes, on the writing thread, inside the request that
produced it — so keep listeners short and push slow work onto your own queue.
They are at-least-once: the store stays the source of truth, so re-read through
`host.transcript.get(threadId, { afterSeq })` rather than assuming a listener saw
every message. Nothing is buffered before you subscribe and nothing is replayed
after a restart, so a consumer that treats the payload as the record will lose
data. A throwing listener is logged and never reaches the write path, which also
means it is never retried. `on` returns the unsubscribe function.

## Storage adapters

`Storage` (`src/host/ports/storage.ts`) is the port every adapter implements.
Three ways to get one:

```ts
import { memoryStorage } from "@gino/host-core";                  // tests, prototypes
import { openDatabase, sqliteStorage } from "@gino/host-store-sqlite";
import { bunSqliteDriver } from "@gino/host-store-sqlite/bun";    // Bun runtime (bun:sqlite)
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node";  // Node runtime (better-sqlite3)

const db = openDatabase({ driver: nodeSqliteDriver, path: "data/gino.db" }); // migrates on open
const storage = sqliteStorage(db, { storeDir: "data/store" });
```

The two sqlite drivers are interchangeable — same schema, same migrations, same
conformance results. `memoryStorage()` keeps everything in Maps and drops it on
`close()`.

### Writing your own adapter

The invariants an adapter must uphold are documented on each repository method
and are NOT inferable from the type signatures: outbox rows stored under the
caller-minted ULID and replayed in id order (the id is the wire `msgId`), `(threadId, seq)` first-writer-wins on the
transcript, compare-and-set agent status with `removed` terminal, set-once task
placement, an atomic `ConfigRepo.replaceScope` that bumps the global version in
the same unit, and `readVerified` refusing to serve bytes that fail their hash.

Prove your adapter against them with the conformance suite exported as
`@gino/host-core/testing`. It is runner-agnostic and takes the primitives of
`bun:test` or `node:test` plus an `expect`:

```ts
import { describeStorageConformance } from "@gino/host-core/testing";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";

describeStorageConformance(
  { name: "mine", open: () => ({ storage: myStorage(), tamperBlob, close: () => {} }) },
  { describe, test, beforeEach, afterEach, expect },
);
```

`tamperBlob(ref)` must corrupt the bytes behind a stored ref — it is how the
suite checks that `readVerified` throws instead of serving them.

## What is NOT in the SDK

Deliberate omissions. Each is the embedder's own stack, not a gap:

- **No HTTP framework opinion.** Core returns `{ status, headers?, body }` and
  throws `AppError`; `@gino/host-hono` is one mount, not the only one.
- **No rate limiting, CORS, request ids or logging middleware.** Apply yours
  ahead of the mounts.
- **No auth policy and no token storage** beyond the protocol credentials the
  wire needs (the shared register token and the per-agent connect token hashes).
  Who may call your admin API is your decision — `adminRouter` takes an injected
  `authorize`.
- **No channels in the default install.** Poll and webhook delivery live in the
  separate `@gino/host-channels`; the primary consumption model is
  `host.events` plus `host.transcript`.
- **No engine-specific behaviour.** An agent advertises the engines it can run
  and the host matches tasks against that list. The set is open — nothing in
  core enumerates it, and a newer agent build may advertise engines this host
  has never heard of.
- **No generic `transaction()` on the storage port**, so a non-SQL adapter stays
  possible. The one cross-row atomic operation is a named method.
- **No scheduler.** Crons live on the agent; the host emits commands and mirrors
  what comes back.

## `@gino/host-core/domain`

Re-exports the domain modules wholesale for the sibling `@gino/host-*`
packages. Semi-internal: not part of the frozen API. `dist/index.js` itself
re-exports through this entry, so under Node there is one copy of every class
regardless of which entry you import from.

## Example

`examples/embed-node` embeds the SDK in a plain Node process (`@hono/node-server`
+ `@hono/node-ws` + better-sqlite3) and round-trips one task against the real
`gino-agent` runner: `bun run example:embed-node` from the repo root.

## Live terminals

```typescript
host.agents.setRemoteOps(agentId, { pty: true });
const terminal = await host.agents.openPty(agentId, { cols: 100, rows: 30 });
terminal.write(new TextEncoder().encode("pwd\n"));
const reader = terminal.output.getReader();
const chunk = await reader.read();
reader.releaseLock();
terminal.close();
await terminal.exited;
```

Configuration delivery is asynchronous: opening before the agent applies the enablement may
still return `disabled`. Sessions end on disconnect and are never restored or replayed. Pass an
AbortSignal as the third argument to cancel even while opening. See protocol §5.2 for limits.
