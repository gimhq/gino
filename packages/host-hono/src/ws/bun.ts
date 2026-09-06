// Bun WebSocket upgrade for the agent router. `hono/bun` exports one shared
// `upgradeWebSocket` / `websocket` pair per process: pass `upgradeWebSocket`
// to `agentRouter` and register `websocket` on `Bun.serve({ websocket })`,
// threading `server` into the fetch env so the adapter can upgrade.
//
// The bun `WSContext` satisfies `WsLike`: it wraps the raw Bun socket in a
// FRESH wrapper per event, so the wire compares `raw` for current-socket
// identity (host-core `WsLike.raw`).
import { upgradeWebSocket, websocket } from "hono/bun";

export function bunWebSocket() {
  return { upgradeWebSocket, websocket };
}
