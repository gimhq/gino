// @gino/host-proxy/hono — the `/llm/*` Hono router. BUN-ONLY: `/v1/responses`
// upgrades over `hono/bun` and relays through the Bun-only Codex WS bridge
// (host-sdk-design §7.5); Node embedders mount the HTTP forward path themselves
// via `createProxyHandler` from the "." entry.
export { llmRouter, stripLlmPrefix } from "./hono/llm";
export type { LlmRouterOptions } from "./hono/llm";
