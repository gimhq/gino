import type { Context, MiddlewareHandler } from "hono";
import type { AppEnv } from "@/shared/lib/types";
import { getAgentByConnectToken } from "@/agents/registry";
import { safeEqual } from "@/platform";
import { ForbiddenError, UnauthorizedError } from "@/shared/lib/errors";

// ─── Three-token auth (M1 / OD12) ───
//
// register token  — shared, long-lived bootstrap secret on POST /agent/register
//                   (carried in the request body per protocol §3).
// connect token   — per-agent secret on the WS + HTTP data plane, header
//                   `x-internal-token`. B0 is a PLACEHOLDER: presence/non-empty
//                   only. The per-agent `agents.connect_token_hash` lookup
//                   (verifyToken) lands with M3 in B2 — see the TODO below.
// channel token   — admin/user bearer on /admin/*, header
//                   `Authorization: Bearer <token>`.
//
// All comparisons are constant-time via platform `safeEqual`. Misconfiguration
// (an empty expected secret) fails CLOSED: every request is rejected. Failures
// throw `UnauthorizedError` so the scaffold error-handler renders the standard
// 401 envelope.

const BEARER_PREFIX = "bearer ";

/** Extract the `Authorization: Bearer` token, or null. Shared with the rate-limit keyers. */
export function bearerToken(c: Context<AppEnv>): string | null {
  const header = c.req.header("authorization")?.trim();
  if (!header || !header.toLowerCase().startsWith(BEARER_PREFIX))
    return null;
  const token = header.slice(BEARER_PREFIX.length).trim();
  return token.length > 0 ? token : null;
}

/** Reject unless `provided` is present and constant-time-equal to a non-empty `expected`. */
function requireSecret(expected: string, provided: string | null | undefined): void {
  if (!expected || !provided || !safeEqual(provided, expected))
    throw new UnauthorizedError();
}

/**
 * POST /agent/register — validate the shared `GINO_REGISTER_TOKEN`. The token
 * is read from the JSON body (`{ registerToken }`, protocol §3); an
 * `Authorization: Bearer` header is accepted as a fallback. A malformed/empty
 * body is treated as "no token" → 401.
 */
export const registerTokenAuth: MiddlewareHandler<AppEnv> = async (c, next) => {
  const expected = c.get("config").REGISTER_TOKEN;
  let provided = bearerToken(c);
  if (!provided) {
    // Hono caches the parsed body, so re-reading it in the stub handler is safe.
    const body = await c.req.json().catch(() => null) as { registerToken?: unknown } | null;
    if (body && typeof body.registerToken === "string")
      provided = body.registerToken;
  }
  requireSecret(expected, provided);
  await next();
};

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
export const connectTokenAuth: MiddlewareHandler<AppEnv> = async (c, next) => {
  const token = c.req.header("x-internal-token");
  if (!token || token.trim().length === 0)
    throw new UnauthorizedError();
  const agent = getAgentByConnectToken(c.get("db"), token);
  if (!agent)
    throw new UnauthorizedError();
  c.set("agentId", agent.id);
  const pathAgentId = c.req.param("agentId");
  if (pathAgentId != null && pathAgentId !== agent.id)
    throw new ForbiddenError();
  await next();
};

/** /admin/* — validate the `GINO_HTTP_CHANNEL_TOKEN` admin/user bearer. */
export const channelTokenAuth: MiddlewareHandler<AppEnv> = async (c, next) => {
  const expected = c.get("config").HTTP_CHANNEL_TOKEN;
  requireSecret(expected, bearerToken(c));
  await next();
};
