# @gino/host-hono

The Hono adapter over `@gino/host-core`: routers that mount `host.wire` and the
optional operator APIs, plus the two WebSocket upgrades.

## When you need it

When you want the agent data plane served over HTTP without writing the mount
yourself. It is runtime-neutral — the WebSocket upgrade is injected, so the same
routers run under Bun and under Node. If you are not on Hono, mount `host.wire`
directly; `attachSocket` / `onSocketMessage` / `detachSocket` is the whole
transport contract.

## Entry points

**`@gino/host-hono`** — the routers.

```ts
import type { HostEnv } from "@gino/host-hono";
import { adminRouter, agentRouter, errorHandler, messagesRouter } from "@gino/host-hono";
import { Hono } from "hono";

const app = new Hono<HostEnv>(); // `HostEnv` — `errorHandler` and the routers read it

app.route("/api/agent", agentRouter(host, { upgradeWebSocket }));
app.route("/api/admin", adminRouter(host, { authorize }));
app.route("/api/messages", messagesRouter(host, { authorize }));
app.onError(errorHandler);
```

`agentRouter` is the protocol and the only required mount. The other two are
conveniences over `host.agents` / `host.tasks` and the channel I/O surface; both
take an `Authorize` callback returning a `Principal` or `null`, so the token
policy stays yours. `errorHandler` renders `AppError` as the protocol error
envelope.

**`/bun`** — the Bun upgrade. Register `websocket` on the server.

```ts
import { bunWebSocket } from "@gino/host-hono/bun";

const { upgradeWebSocket, websocket } = bunWebSocket();
Bun.serve({ port: 3000, fetch: app.fetch, websocket });
```

**`/node`** — the upgrade over `@hono/node-ws`. Bind it to the app, then inject
it into the listening server.

```ts
import { nodeWebSocket } from "@gino/host-hono/node";

const { upgradeWebSocket, injectWebSocket } = nodeWebSocket(app);
injectWebSocket(serve({ fetch: app.fetch, port: 3000 }));
```

Also exported: `bindHost` (puts the host in the request context), the
`connectTokenAuth` / `bearerToken` / `requireAuthorized` middleware, the
`{ success, data, meta? }` envelope helpers, and the OpenAPI helpers so your own
routes join the same spec.

## What it does not do

- **No policy.** Rate limiting, CORS, request ids, logging and the bearer tokens
  themselves are yours; apply them ahead of the mounts.
- **No re-implementation of the socket lifecycle.** The ordering lives in
  host-core and is protocol-visible; this package only wires the callbacks up.
- **No server.** It builds routers; you call `Bun.serve` or `@hono/node-server`.
- **No storage or host construction.** Pass in a `Host` you already built.

## Browser terminals

Pass the same `upgradeWebSocket` helper to `adminRouter(host, {authorize, upgradeWebSocket})`
and `agentRouter`. This enables `POST /admin/agents/:id/pty-ticket` (admin authorization) and
`GET /admin/agents/:id/pty?ticket=...` (single-use, 30-second ticket). A supplied browser Origin
must match the authenticated ticket request's Origin, falling back to its URL origin when
absent. This supports TLS termination without trusting forwarded headers. Existing admin
routes retain their normal authorization.

The browser sends `open`, waits for `ready`, then sends `input` (base64 bytes), `resize`, or
`close`. The server emits `output` and `exit`; closing the socket ends its terminal. See
[`protocol §5.2`](../../docs/protocol.md#52-interactive-terminals-live-only) for full payloads.

The real host/agent integration check runs with
`GINO_PTY_TEST_BIN=/absolute/path/to/gino-agent bun test tests/integration/real-pty.test.ts`.
It is skipped by ordinary package tests when the binary path is absent.
