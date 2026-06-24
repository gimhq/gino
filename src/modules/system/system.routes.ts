import type { AppEnv } from "@/shared/lib/types";
import { sql } from "drizzle-orm";
import { Hono } from "hono";
import { z } from "zod";
import { BUILD_INFO } from "@/build-info";
import { isUnderLode, readLodeState } from "@/platform";
import { describeRoute, errors, jsonOk, resolver, TAGS } from "@/shared/lib/openapi";

/**
 * System routes — the only routes wired in the scaffold. Liveness +
 * readiness probes for the orchestrator, and a build-version read. The
 * data-plane routes (`/agent/*`, `/admin/*`, `/messages/*`, `/llm/*`) are
 * mounted by workstreams C/D as M1–M8 land.
 */
export function systemRoutes() {
  const router = new Hono<AppEnv>();

  // Liveness — k8s livenessProbe / Docker HEALTHCHECK.
  router.get(
    "/health",
    describeRoute({
      tags: [TAGS.System],
      summary: "Liveness probe",
      description: "Returns 200 while the process is up. Used by orchestrator liveness probes.",
      responses: {
        200: { description: "Alive", content: { "application/json": { schema: resolver(z.object({ status: z.literal("ok") })) } } },
      },
    }),
    c => c.json({ status: "ok" }),
  );

  // Readiness — DB reachable. 503 drains traffic.
  router.get(
    "/health/ready",
    describeRoute({
      tags: [TAGS.System],
      summary: "Readiness probe",
      description: "200 when the DB is reachable; 503 (with a `status` reason) otherwise, to drain traffic.",
      responses: {
        200: { description: "Ready", content: { "application/json": { schema: resolver(z.object({ status: z.literal("ready") })) } } },
        503: { description: "Not ready", content: { "application/json": { schema: resolver(z.object({ status: z.literal("db_unavailable") })) } } },
      },
    }),
    (c) => {
      try {
        c.get("db").run(sql`SELECT 1`);
      }
      catch (err) {
        c.get("logger").error({ err }, "readiness probe: db ping failed");
        c.status(503);
        return c.json({ status: "db_unavailable" });
      }
      return c.json({ status: "ready" });
    },
  );

  router.get(
    "/system/version",
    describeRoute({
      tags: [TAGS.System],
      summary: "Build version",
      responses: {
        ...jsonOk(z.object({ commit: z.string(), buildTime: z.string(), version: z.string(), lode: z.any() }), "Build identifiers"),
        ...errors(500),
      },
    }),
    (c) => {
      // Surface the lode loader's view (host-upgrade-design §6.3) so the admin
      // UI can show "on version X, update available". Read-only mirror; absent
      // when not running under lode.
      const managed = isUnderLode();
      const s = managed ? readLodeState() : null;
      const lode = managed
        ? {
            managed: true,
            current: s?.current ?? null,
            last_good: s?.lastGood ?? null,
            available: s?.available ?? null,
            channel: s?.channel ?? null,
            status: s?.status ?? null,
          }
        : { managed: false };
      return c.json({ success: true, data: { ...BUILD_INFO, lode } });
    },
  );

  return router;
}
