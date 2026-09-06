import type { Authorize } from "@gino/host-hono";
import type { MiddlewareHandler } from "hono";
import type { Config } from "@/config";
import type { AppEnv } from "@/shared/lib/types";
import { UnauthorizedError } from "@gino/host-core";
import { safeEqual } from "@gino/host-core/domain";
import { bearerToken } from "@gino/host-hono";

// ─── App-side bearer policy (M1 / OD12 / HOST-XC-02) ───
//
// The protocol credentials (register token, per-agent connect token) live in
// `@gino/host-hono` (`connectTokenAuth`) and the wire. The two OPERATOR
// bearers are the standalone app's policy and are injected into the
// routers as `authorize` callbacks (host-sdk-design §7.1):
//
// channel token   — user I/O bearer on /messages/*, header
//                   `Authorization: Bearer <token>`.
// admin token     — control-plane bearer on /admin/*, /system/lode/*, and
//                   GET /metrics (HOST-XC-02). A DISTINCT secret from the
//                   channel token: an integration holding the channel token to
//                   push/poll messages must not reach agent remove, task
//                   dispatch, or lode update. There is NO fallback from one
//                   token to the other; a single-operator deployment may
//                   explicitly set both env vars to the same value.
//
// All comparisons are constant-time via platform `safeEqual`. Misconfiguration
// (an empty expected secret) fails CLOSED: every request is rejected. Failures
// throw `UnauthorizedError` so the scaffold error-handler renders the standard
// 401 envelope.

/** True when `provided` is present and constant-time-equal to a non-empty `expected`. */
function matchesSecret(expected: string, provided: string | null | undefined): boolean {
  return Boolean(expected && provided && safeEqual(provided, expected));
}

/** Reject unless `provided` is present and constant-time-equal to a non-empty `expected`. */
function requireSecret(expected: string, provided: string | null | undefined): void {
  if (!matchesSecret(expected, provided))
    throw new UnauthorizedError();
}

/** `authorize` for `messagesRouter` — validate the `GINO_HTTP_CHANNEL_TOKEN` user I/O bearer. */
export function channelTokenAuthorize(config: Config): Authorize {
  return c => (matchesSecret(config.HTTP_CHANNEL_TOKEN, bearerToken(c)) ? { kind: "channel" } : null);
}

/**
 * `authorize` for `adminRouter` — validate the `GINO_HTTP_ADMIN_TOKEN`
 * control-plane bearer (HOST-XC-02). Unconfigured (empty) fails CLOSED: every
 * admin request is rejected until the token is set. Never falls back to the
 * channel token.
 */
export function adminTokenAuthorize(config: Config): Authorize {
  return c => (matchesSecret(config.HTTP_ADMIN_TOKEN, bearerToken(c)) ? { kind: "admin" } : null);
}

/**
 * /system/lode/*, GET /metrics — the admin bearer as plain middleware for the
 * app-owned routes (same policy as `adminTokenAuthorize`).
 */
export const adminTokenAuth: MiddlewareHandler<AppEnv> = async (c, next) => {
  const expected = c.get("config").HTTP_ADMIN_TOKEN;
  requireSecret(expected, bearerToken(c));
  await next();
};
