import type { HostEnv } from "@gino/host-hono";
import type { Config } from "@/config";

/**
 * Hono environment shared by every route + middleware: the adapter's
 * `HostEnv` (host / logger / connect-token `agentId`) plus the standalone
 * app's request id and resolved env config (read by the rate limits, the
 * bearer-token policy and the system routes — never by the packages).
 */
export interface AppEnv {
  Bindings: HostEnv["Bindings"];
  Variables: HostEnv["Variables"] & {
    requestId: string;
    config: Config;
  };
}
