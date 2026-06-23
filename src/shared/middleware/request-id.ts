import type { MiddlewareHandler } from "hono";
import type { AppEnv } from "../lib/types";

/**
 * Echoes the inbound or freshly-minted `requestId` back to the client as
 * an `X-Request-Id` response header. Paired with `hono/request-id`'s
 * accept-inbound behaviour, this gives every response a stable id that
 * (a) appears in the log line (`loggingMiddleware`), (b) is forwarded by
 * outbound callers via `withRequestIdHeader`, and (c) the client can
 * quote back when reporting a failure.
 */
export const propagateRequestId: MiddlewareHandler<AppEnv> = async (c, next) => {
  await next();
  const id = c.get("requestId");
  if (id && !c.res.headers.has("X-Request-Id"))
    c.res.headers.set("X-Request-Id", id);
};

/**
 * Adds `X-Request-Id` to outbound `fetch` headers. Use it from any code
 * path that fans out to an external service inside a request handler (e.g.
 * the M7 LLM proxy, webhook dispatch) so the downstream system can correlate.
 *
 * `id` is typically `c.get("requestId")`; pass `undefined` to no-op.
 */
export function withRequestIdHeader(
  init: RequestInit | undefined,
  id: string | undefined,
): RequestInit {
  if (!id)
    return init ?? {};
  const headers = new Headers(init?.headers ?? {});
  if (!headers.has("X-Request-Id"))
    headers.set("X-Request-Id", id);
  return { ...init, headers };
}
