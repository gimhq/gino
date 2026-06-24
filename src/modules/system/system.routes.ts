import type { AppEnv } from "@/shared/lib/types";
import { sql } from "drizzle-orm";
import { Hono } from "hono";
import { z } from "zod";
import { BUILD_INFO } from "@/build-info";
import { getLodeSummary, requestLodeRestart, requestLodeRollback, requestLodeUpdate, setLodeHold } from "@/lode";
import { ok } from "@/shared/lib/api-response";
import { AppError } from "@/shared/lib/errors";
import { describeRoute, errors, jsonOk, resolver, SECURITY, TAGS, validator } from "@/shared/lib/openapi";
import { channelTokenAuth } from "@/shared/middleware/auth";

// A lode version token: a concrete version or the "latest" channel pointer.
const lodeVersion = z.union([
  z.literal("latest"),
  z.string().trim().min(1).max(64).regex(/^[\w.+-]+$/, "must be a version or \"latest\""),
]);
const lodeUpdateSchema = z.object({ target: lodeVersion });
// Roll back to a specific version, or omit to use lode's recorded last_good.
const lodeRollbackSchema = z.object({ version: lodeVersion.optional() });
const lodeHoldSchema = z.object({ hold: z.boolean() });

// The lode operator actions are inert outside the supervisor — there is nothing
// to drive. Surface that as a 409 so callers can distinguish "not managed" from
// a real failure.
function lodeNotActive(): never {
  throw new AppError("host is not running under the lode supervisor", 409, "LODE_NOT_ACTIVE");
}

/**
 * System routes — liveness + readiness probes for the orchestrator, the
 * build-version read (with the lode loader's upgrade view), and the lode
 * operator actions (`/system/lode/*`). The probes + version read are public;
 * the mutating lode actions are channel-token guarded per-route (this router is
 * mounted on the public surface, so health stays unauthenticated).
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
      summary: "Build version and lode upgrade summary",
      responses: {
        ...jsonOk(z.object({ commit: z.string(), buildTime: z.string(), version: z.string(), lode: z.unknown() }), "Build identifiers and lode upgrade status"),
        ...errors(500),
      },
    }),
    // Surface the lode loader's view (host-upgrade-design §6) so the admin UI can
    // show "on version X, update available". Read-only; `supervised:false` when
    // not running under lode.
    c => c.json({ success: true, data: { ...BUILD_INFO, lode: getLodeSummary() } }),
  );

  // Restart the running version via lode (also applies a pending lode.toml edit —
  // the relaunch re-reads it). Channel-token guarded; 409 when not under lode.
  router.post(
    "/system/lode/restart",
    describeRoute({
      tags: [TAGS.System],
      summary: "Restart via lode",
      description: "Bumps state.json `restart_nonce` so lode relaunches the current version (re-reading lode.toml).",
      security: SECURITY.bearer,
      responses: {
        ...jsonOk(z.object({ status: z.literal("ok"), restartNonce: z.number() }), "Restart requested"),
        ...errors(401, 409),
      },
    }),
    channelTokenAuth,
    (c) => {
      const result = requestLodeRestart();
      if (result.status === "not_active")
        lodeNotActive();
      c.get("logger").info({ restartNonce: result.restartNonce }, "lode: restart requested via admin");
      return c.json(ok({ status: "ok", restartNonce: result.restartNonce }));
    },
  );

  // Request an up/down-grade by setting lode's `target` (a version or "latest").
  router.post(
    "/system/lode/update",
    describeRoute({
      tags: [TAGS.System],
      summary: "Update via lode",
      description: "Sets state.json `target` so lode resolves, downloads, verifies, and switches to the requested version.",
      security: SECURITY.bearer,
      responses: {
        ...jsonOk(z.object({ status: z.literal("ok"), target: z.string() }), "Update requested"),
        ...errors(401, 409, 422),
      },
    }),
    channelTokenAuth,
    validator("json", lodeUpdateSchema),
    (c) => {
      const { target } = c.req.valid("json");
      const result = requestLodeUpdate(target);
      if (result.status === "not_active")
        lodeNotActive();
      c.get("logger").info({ target: result.target }, "lode: update requested via admin");
      return c.json(ok({ status: "ok", target: result.target }));
    },
  );

  // Roll back to a version, or omit `version` to use lode's recorded last_good.
  router.post(
    "/system/lode/rollback",
    describeRoute({
      tags: [TAGS.System],
      summary: "Roll back via lode",
      description: "Sets `target` to the given version, else the recorded `last_good`, so lode switches back.",
      security: SECURITY.bearer,
      responses: {
        ...jsonOk(z.object({ status: z.literal("ok"), target: z.string() }), "Rollback requested"),
        ...errors(401, 409, 422),
      },
    }),
    channelTokenAuth,
    validator("json", lodeRollbackSchema),
    (c) => {
      const { version } = c.req.valid("json");
      const result = requestLodeRollback(version);
      if (result.status === "not_active")
        lodeNotActive();
      if (result.status === "no_target")
        throw new AppError("no rollback target: lode has not recorded a last-good version", 409, "LODE_NO_ROLLBACK_TARGET");
      c.get("logger").info({ target: result.target }, "lode: rollback requested via admin");
      return c.json(ok({ status: "ok", target: result.target }));
    },
  );

  // Set/clear the maintenance hold — lode stops (re)starting the process.
  router.post(
    "/system/lode/hold",
    describeRoute({
      tags: [TAGS.System],
      summary: "Set lode maintenance hold",
      description: "Sets state.json `hold` so lode will NOT (re)start the process (status \"held\"); clear with `hold:false`.",
      security: SECURITY.bearer,
      responses: {
        ...jsonOk(z.object({ status: z.literal("ok"), hold: z.boolean() }), "Hold updated"),
        ...errors(401, 409, 422),
      },
    }),
    channelTokenAuth,
    validator("json", lodeHoldSchema),
    (c) => {
      const { hold } = c.req.valid("json");
      const result = setLodeHold(hold);
      if (result.status === "not_active")
        lodeNotActive();
      c.get("logger").info({ hold: result.hold }, "lode: hold updated via admin");
      return c.json(ok({ status: "ok", hold: result.hold }));
    },
  );

  return router;
}
