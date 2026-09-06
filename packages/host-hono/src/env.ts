import type { Host, Logger } from "@gino/host-core";
import type { Context, MiddlewareHandler } from "hono";

/**
 * Hono environment shared by every router in this package. An embedder may
 * extend `Variables` (the standalone host adds its resolved `config`); the
 * routers themselves only read `host`, `logger` and the connect-token-resolved
 * `agentId`.
 */
export interface HostEnv {
  Bindings: {
    /** Peer address when the runtime threads it in (Bun: `srv.requestIP(req)`). */
    IP?: { address: string; port: number; family: "IPv4" | "IPv6" } | null;
  };
  Variables: {
    /** The per-instance host (storage, sessions, launchers, wire, namespaces, …). */
    host: Host;
    logger: Logger;
    /**
     * The connect-token-resolved agent identity on the `/agent/*` data plane.
     * Set by `connectTokenAuth` from the token's `connect_token_hash` (never a
     * client-supplied param) — the authorization subject for the byte channel.
     */
    agentId: string;
    requestId?: string;
  };
}

/** The caller an embedder-supplied `authorize` admitted (admin / messages routers). */
export interface Principal {
  /** Which credential class admitted the caller (e.g. `"admin"`, `"channel"`). */
  readonly kind: string;
  /** Optional stable caller identifier (an operator id, an integration name, …). */
  readonly id?: string;
}

/**
 * Resolve the caller of an admin / messages request. `null` → the standard 401
 * envelope; throw `ForbiddenError` from the callback for a 403.
 */
export type Authorize = (c: Context<HostEnv>) => Principal | null;

/**
 * Put `host` into the request context (and its logger, unless the app already
 * set a request-scoped one) so the routers work when mounted on a bare app.
 */
export function bindHost(host: Host): MiddlewareHandler<HostEnv> {
  return async (c, next) => {
    c.set("host", host);
    if (!c.get("logger"))
      c.set("logger", host.logger);
    await next();
  };
}
