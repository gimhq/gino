import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { adminTokenAuth } from "@/shared/middleware/auth";
import { adminAgentRoutes } from "./agents";
import { adminChannelRoutes } from "./channels";
import { adminConfigRoutes } from "./config";
import { adminTaskRoutes } from "./tasks";

/**
 * M1 — `/admin/*` control API (mounted at `/admin`). Guarded by the admin
 * token (`Authorization: Bearer`, HOST-XC-02 — the channel token does NOT
 * grant this surface). Agents (M3) + Tasks/Crons (M4) + Channels (M6) +
 * Config/Skills (M8) are LIVE. Explicit routes only — no catch-all, so an
 * unknown `/admin/...` path 404s (when authorised) rather than falling
 * through.
 */
export function adminRoutes() {
  const router = new Hono<AppEnv>();

  // Admin-token bearer guards every admin route (incl. the mounted sub-routers).
  router.use("*", adminTokenAuth);

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
