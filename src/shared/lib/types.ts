import type { Host, Logger } from "@gino/host-core";
import type { Config } from "@/config";

/**
 * Hono environment shared by every route + middleware. The data-plane
 * modules (M1–M8) extend `Variables` with their own request-scoped state as
 * they land — for the scaffold it carries the request id, the resolved
 * config, the logger, and the host instance.
 */
export interface AppEnv {
  Bindings: {
    IP: { address: string; port: number; family: "IPv4" | "IPv6" } | null;
  };
  Variables: {
    requestId: string;
    config: Config;
    logger: Logger;
    /** The per-instance host (storage, sessions, launchers, wire, namespaces, …). */
    host: Host;
    /**
     * The connect-token-resolved agent identity on the `/agent/*` data plane.
     * Set by `connectTokenAuth` from the token's `connect_token_hash` (never a
     * client-supplied param) — the authorization subject for the byte channel.
     */
    agentId: string;
  };
}
