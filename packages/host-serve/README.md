# @gino/host-serve

The `/agent` data plane over `@gino/host-core` with no HTTP framework: a
runtime-neutral `Request → Response` handler, and two ready-made listeners.

## When you need it

When you want to embed the host without Hono. `@gino/host-hono` is the same
data plane as routers; this package is the same data plane as one function you
put behind any listener. Pick one or the other — both mount `host.wire`, so the
protocol behaviour is identical.

## Entry points

**`@gino/host-serve/bun`** — `Bun.serve`.

```ts
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { serveHostBun } from "@gino/host-serve/bun";

const host = createHost({ storage: memoryStorage(), logger: noopLogger, agents: { registerToken: "..." } });
const served = serveHostBun(host, {
  port: 3000,
  fallback: req => Response.json({ path: new URL(req.url).pathname }), // your own routes, same port
});
console.log(served.url); // agents dial `${served.url}` with GINO_HOST_URL
```

**`@gino/host-serve/node`** — `node:http` (or `node:https` with `tls`) plus `ws`.

```ts
import { serveHostNode } from "@gino/host-serve/node";

const served = await serveHostNode(host, { port: 3000, tls: { key, cert } });
await served.stop();
```

**`@gino/host-serve`** — the handler alone, for any other listener.

```ts
import { createAgentHandler } from "@gino/host-serve";

const handler = createAgentHandler(host, { basePath: "/agent" });
// HTTP: `await handler.fetch(req)` → a Response, or `null` outside the data plane.
// WS:   `handler.socket(req)` → `{ agentId }` to upgrade, a 401/403 Response to send
//       instead, or `null` when it is not the `/ws/:agentId` path. After the upgrade,
//       call `host.wire.attachSocket` / `onSocketMessage` / `detachSocket`.
```

Options shared by both listeners: `port` (default `0`, a free port), `hostname`
(default `127.0.0.1`), `basePath` (default `/agent`), `maxUploadBytes` (default
`host.options.maxUploadBytes`), `tls`, `fallback` (default: a 404 envelope).

## What it does not do

Rate limiting, operator/admin APIs, OpenAPI metadata — those are the embedder's
or `@gino/host-hono`'s. Terminating TLS at a reverse proxy is the usual
deployment; `tls` is for the in-process case.
