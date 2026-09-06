// Node WebSocket upgrade for the agent router via `@hono/node-ws` (over the
// `ws` library). Create it with the app the routers are mounted on, pass
// `upgradeWebSocket` to `agentRouter`, then `injectWebSocket(server)` on the
// `@hono/node-server` (or `node:http`) server so upgrade requests are routed
// through the app.
//
// The node `WSContext` satisfies `WsLike`: `@hono/node-ws` builds ONE context
// object per connection whose `raw` is the underlying `ws` socket, so the
// wire's current-socket identity check (`raw`) holds for the lifetime of the
// connection.
import type { NodeWebSocketInit } from "@hono/node-ws";
import { createNodeWebSocket } from "@hono/node-ws";

export function nodeWebSocket(app: NodeWebSocketInit["app"], baseUrl?: NodeWebSocketInit["baseUrl"]) {
  const { upgradeWebSocket, injectWebSocket } = createNodeWebSocket({ app, ...(baseUrl !== undefined ? { baseUrl } : {}) });
  return { upgradeWebSocket, injectWebSocket };
}
