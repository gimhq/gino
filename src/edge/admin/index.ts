import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { channelTokenAuth } from "@/shared/middleware/auth";
import { adminAgentRoutes } from "./agents";
import { adminChannelRoutes } from "./channels";
import { adminConfigRoutes } from "./config";
import { adminSystemRoutes } from "./system";
import { adminTaskRoutes } from "./tasks";

/**
 * M1 — `/admin/*` control API (mounted at `/admin`). Guarded by the channel
 * token (`Authorization: Bearer`). Agents (M3) + Tasks/Crons (M4) + Channels
 * (M6) + Config/Skills (M8) are LIVE. Explicit routes only — no catch-all, so
 * an unknown `/admin/...` path 404s (when authorised) rather than falling
 * through.
 */
export function adminRoutes() {
  const router = new Hono<AppEnv>();

  // Channel-token bearer guards every admin route (incl. the mounted sub-routers).
  router.use("*", channelTokenAuth);

  // Agents (M3) — live OOB observe + control API.
  router.route("/agents", adminAgentRoutes());

  // Tasks + crons (M4) — live task identity + dispatch + work API.
  router.route("/tasks", adminTaskRoutes());

  // Channels (M6) — live registration + webhook/binding API.
  router.route("/channels", adminChannelRoutes());

  // Config / skills (M8) — live content-addressed registry + desired-set API.
  router.route("/config", adminConfigRoutes());

  // System — lode loader status + update/restart control (host-upgrade-design §6.2).
  router.route("/system", adminSystemRoutes());

  return router;
}
