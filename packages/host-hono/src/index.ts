// @gino/host-hono — the Hono adapter over `@gino/host-core` (docs/host-sdk-design
// §3.2, §7.1, §7.3). Runtime-neutral: the WebSocket upgrade is injected
// (`./bun` → the Bun adapter, `./node` → @hono/node-ws). Rate limiting, CORS,
// request ids, logging and the bearer-token policy are the embedder's stack.
//
// FROZEN as of P3 (docs/host-sdk-design.md §5 step 8). This list is the
// package's API: an explicit allow-list, no wildcards, so the surface cannot
// grow by accident when a module gains an export.
// `tests/unit/api-surface.test.ts` snapshots it — adding or removing a name
// here fails that test until the snapshot is updated deliberately.
//
// Grouped by source module, which `perfectionist/sort-exports` keeps in
// alphabetical order: the three routers (`./admin`, `./agent`, `./messages`),
// the response envelope (`./api-response`), the auth middleware (`./auth`),
// the Hono environment (`./env`), the protocol error handler (`./errors`) and
// the OpenAPI helpers (`./openapi`).

// ─── The admin control plane (§7.1) — mounted behind an injected `authorize` ───
export { adminRouter } from "./admin";
export type { AdminRouterOptions } from "./admin";

// ─── The `/agent` data plane (§3.2) — HTTP + WS over `host.wire` ───
export { agentRouter } from "./agent";
export type { AgentRouterOptions } from "./agent";

// ─── The `{ success, data, meta? }` response envelope ───
export { err, ok, paged } from "./api-response";
export type { ApiError, ApiMeta, ApiResponse, ApiSuccess } from "./api-response";

// ─── Auth middleware (the policy itself stays the embedder's) ───
export { bearerToken, connectTokenAuth, requireAuthorized } from "./auth";

// ─── The Hono environment the routers run in ───
export { bindHost } from "./env";
export type { Authorize, HostEnv, Principal } from "./env";

// ─── The protocol error envelope ───
export { errorHandler } from "./errors";

// ─── The channel-facing messages router (§7.1) ───
export { messagesRouter } from "./messages";
export type { MessagesRouterOptions } from "./messages";

// ─── OpenAPI helpers, so an embedder's own routes join the same spec ───
// `describeRoute` and `resolver` are re-exported verbatim from `hono-openapi`.
// A consumer that needs to NAME `resolver`'s return type in its own emitted
// `.d.ts` has to reach `hono-openapi` / `openapi-types` itself — those types
// belong to that library, not to this package, so there is nothing to add here.
export { describeRoute, errors, jsonCreated, jsonOk, jsonPaged, noContent, raw, resolver, SECURITY, TAGS, validator } from "./openapi";
