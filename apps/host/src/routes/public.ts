import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { systemRoutes } from "@/modules/system";

/**
 * Public (unauthenticated) routes. For the scaffold this is just the system
 * module (health / readiness / version). Authenticated data-plane and admin
 * routers are added here (and in a sibling `protected.ts`) by workstreams
 * C/D as M1–M8 land.
 */
export function publicRoutes() {
  const router = new Hono<AppEnv>();
  router.route("/", systemRoutes());
  return router;
}
