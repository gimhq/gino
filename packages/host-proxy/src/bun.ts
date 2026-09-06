// @gino/host-proxy/bun — Bun-only entry (Bun WebSocket types); Node embedders
// do not get the raw Codex passthrough (host-sdk-design §7.5). Everything
// runtime-neutral stays on the "." entry (`./index.ts`).
export { createCodexRelay } from "./ws-bridge";
export type { ClientSocket, CodexRelay, CodexRelayDeps } from "./ws-bridge";
