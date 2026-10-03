import type { Host } from "@gino/host-core";
import type { Authorize, HostEnv } from "../env";
import { Hono } from "hono";
import { requireAuthorized } from "../auth";
import { bindHost } from "../env";
import { adminAgentRoutes } from "./agents";
import { adminChannelRoutes } from "./channels";
import { adminConfigRoutes } from "./config";
import { adminTaskRoutes } from "./tasks";

export interface AdminRouterOptions {
  /**
   * The embedder's control-plane authorization (the standalone
   * host checks its admin bearer here; a channel credential must NOT be
   * admitted). `null` → 401 on every admin route.
   */
  readonly authorize: Authorize;
}

/**
 * M1 — `/admin/*` control API (mount at `/admin`). Guarded by the injected
 * `authorize` (host-sdk-design §7.1 — no built-in token). Agents (M3) +
 * Tasks/Crons (M4) + Channels (M6) + Config/Skills (M8) are LIVE. Explicit
 * routes only — no catch-all, so an unknown `/admin/...` path 404s (when
 * authorised) rather than falling through.
 */
export function adminRouter(host: Host, opts: AdminRouterOptions) {
  const router = new Hono<HostEnv>();

  router.use("*", bindHost(host));

  // The embedder's authorization guards every admin route (incl. the mounted sub-routers).
  router.use("*", requireAuthorized(opts.authorize));

  // Agents (M3) — live OOB observe + control API.
  router.route("/agents", adminAgentRoutes());

  // Tasks + crons (M4) — live task identity + dispatch + work API.
  router.route("/tasks", adminTaskRoutes());

  // Channels (M6) — live registration + webhook/binding API.
  router.route("/channels", adminChannelRoutes());

  // Config / skills (M8) — live content-addressed registry + desired-set API.
  router.route("/config", adminConfigRoutes());

  // Lode loader status + update/restart/rollback/hold control now lives in the
  // system module at `/system/lode/*` (host-upgrade-design §6).

  return router;
}
