import type { Context, MiddlewareHandler } from "hono";
import type { Authorize, HostEnv } from "./env";
import { ForbiddenError, UnauthorizedError } from "@gino/host-core";

// ─── Protocol credentials (M1 / OD12 / HOST-XC-02) ───
//
// register token  — shared, long-lived bootstrap secret on POST /agent/register
//                   (carried in the request body per protocol §3). Validated IN
//                   the register handler (`host.wire.register`), not here, so
//                   every failure renders the protocol §3 `register_error`
//                   envelope (HOST-M1M2-14).
// connect token   — per-agent secret on the WS + HTTP data plane, header
//                   `x-internal-token`, resolved to its owning agent by
//                   `agents.connect_token_hash` (`host.wire.resolveAgent`).
//
// The channel token (/messages/*) and the admin token (/admin/*) are the
// embedder's policy (host-sdk-design §4, §7.1): the routers take an injected
// `authorize` callback and never carry a built-in bearer secret.
//
// Failures throw `UnauthorizedError` so `errorHandler` renders the standard
// 401 envelope.

const BEARER_PREFIX = "bearer ";

/** Extract the `Authorization: Bearer` token, or null. Shared with the embedder's authorize / rate-limit keyers. */
export function bearerToken(c: Context): string | null {
  const header = c.req.header("authorization")?.trim();
  if (!header || !header.toLowerCase().startsWith(BEARER_PREFIX))
    return null;
  const token = header.slice(BEARER_PREFIX.length).trim();
  return token.length > 0 ? token : null;
}

/**
 * WS + HTTP data plane (`/agent/ws|config|output|attachments`) — authenticate AND
 * identify the caller from the per-agent connect token in `x-internal-token`. The
 * token is resolved to its owning agent by `connect_token_hash` (the token IS the
 * identity — never a client-supplied `?agent_id`/`x-agent-id`); the resolved
 * `agentId` is placed in context as the authorization subject for downstream
 * handlers (the byte channel authorizes against it). Resolution rules:
 *   - missing/empty token            → 401
 *   - token matches no live agent    → 401 (unknown / `removed`, fails closed)
 *   - route carries a `:agentId` path segment that is NOT the token's agent
 *     (config/output/ws) → 403 (cross-agent access on an agent-scoped route)
 */
export const connectTokenAuth: MiddlewareHandler<HostEnv> = async (c, next) => {
  const token = c.req.header("x-internal-token");
  if (!token || token.trim().length === 0)
    throw new UnauthorizedError();
  const agent = c.get("host").wire.resolveAgent(token);
  if (!agent)
    throw new UnauthorizedError();
  c.set("agentId", agent.agentId);
  const pathAgentId = c.req.param("agentId");
  if (pathAgentId != null && pathAgentId !== agent.agentId)
    throw new ForbiddenError();
  await next();
};

/** Gate every route behind the embedder's `authorize`: `null` → 401 (the callback may throw `ForbiddenError` for 403). */
export function requireAuthorized(authorize: Authorize): MiddlewareHandler<HostEnv> {
  return async (c, next) => {
    if (!authorize(c))
      throw new UnauthorizedError();
    await next();
  };
}
