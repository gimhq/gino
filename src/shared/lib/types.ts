import type { WebhookPump } from "@/channels/pump";
import type { Config } from "@/config";
import type { AppDatabase } from "@/db";
import type { Logger } from "@/shared/lib/logger";

/**
 * Hono environment shared by every route + middleware. The data-plane
 * modules (M1–M8) extend `Variables` with their own request-scoped state as
 * they land — for the scaffold it carries the request id, the resolved
 * config, the logger, and the v2 database handle.
 */
export interface AppEnv {
  Bindings: {
    IP: { address: string; port: number; family: "IPv4" | "IPv6" } | null;
  };
  Variables: {
    requestId: string;
    db: AppDatabase;
    config: Config;
    logger: Logger;
    /**
     * The connect-token-resolved agent identity on the `/agent/*` data plane.
     * Set by `connectTokenAuth` from the token's `connect_token_hash` (never a
     * client-supplied param) — the authorization subject for the byte channel.
     */
    agentId: string;
    /**
     * M6 webhook egress pump (HOST-M5M6-04). Present only when bootstrap
     * started one — the inbound routes use it to trigger a push when a
     * transcript row lands; absent in tests that compose the app directly.
     */
    webhookPump?: WebhookPump;
  };
}
