// @gino/host-hono — the Hono adapter over `@gino/host-core` (docs/host-sdk-design
// §3.2, §7.1, §7.3). Runtime-neutral: the WebSocket upgrade is injected
// (`./bun` → the Bun adapter, `./node` → @hono/node-ws). Rate limiting, CORS,
// request ids, logging and the bearer-token policy are the embedder's stack.

export { adminRouter } from "./admin";
export type { AdminRouterOptions } from "./admin";
export { agentRouter } from "./agent";
export type { AgentRouterOptions } from "./agent";
export { err, ok, paged } from "./api-response";
export type { ApiError, ApiMeta, ApiResponse, ApiSuccess } from "./api-response";
export { bearerToken, connectTokenAuth, requireAuthorized } from "./auth";
export { bindHost } from "./env";
export type { Authorize, HostEnv, Principal } from "./env";
export { errorHandler } from "./errors";
export { messagesRouter } from "./messages";
export type { MessagesRouterOptions } from "./messages";
export { describeRoute, errors, jsonCreated, jsonOk, jsonPaged, noContent, raw, resolver, SECURITY, TAGS, validator } from "./openapi";
