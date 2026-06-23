// M1 — EDGE (transport + auth + route mounting; HTTP/WS termination only).
//
// The edge owns the explicit v2 prefix topology. `edgeRoutes()` composes the
// three prefix routers; `app.ts` mounts it onto the single `Hono<AppEnv>` app
// after the public (system) routes. Each prefix router carries its own auth
// (register/connect token on `/agent/*`, channel token on `/admin/*`) and
// returns 501 stubs until B1–B6 fill them in. `/llm/*` (M7) is the credential
// proxy for the engine's LLM traffic (B5).
//
// No catch-all: an unknown path falls through to the scaffold's clean 404.
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { adminRoutes } from "./admin";
import { agentRoutes } from "./agent";
import { llmRoutes } from "./llm";
import { messagesRoutes } from "./messages";

export { adminRoutes } from "./admin";
export { agentRoutes } from "./agent";
export { llmRoutes } from "./llm";
export { messagesRoutes } from "./messages";

export function edgeRoutes() {
  const router = new Hono<AppEnv>();
  router.route("/agent", agentRoutes());
  router.route("/admin", adminRoutes());
  router.route("/messages", messagesRoutes());
  router.route("/llm", llmRoutes());
  return router;
}

export const MODULE = "edge" as const;
