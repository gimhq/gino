import type { Context } from "hono";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { z } from "zod";
import { isUnderLode, readLodeState, requestRestart, requestRollback, requestUpdate } from "@/platform";
import { ok } from "@/shared/lib/api-response";
import { describeRoute, errors, jsonOk, TAGS } from "@/shared/lib/openapi";

/**
 * `/admin/system/*` — operator control of the host's own lifecycle when it runs
 * under the **lode** loader (docs/host-upgrade-design.md §6.2). These write the
 * app-owned fields of `$LODE_DATA_DIR/state.json`; lode polls the file (~1s) and
 * applies the hot-update / restart. When the host is NOT under lode, the
 * mutating routes 409 (there is nothing to drive). Channel-token guarded by the
 * parent `/admin` router.
 */
function notManaged(c: Context<AppEnv>) {
  return c.json(
    { success: false, error: { code: "NOT_MANAGED", message: "host is not running under the lode loader" } },
    409,
  );
}

export function adminSystemRoutes() {
  const router = new Hono<AppEnv>();

  // ── Lode-managed status (read-only mirror of state.json) ───────────────────
  router.get(
    "/",
    describeRoute({
      tags: [TAGS.System],
      summary: "Lode loader status (current / last_good / available / channel)",
      responses: { ...jsonOk(z.any(), "Status"), ...errors(401) },
    }),
    c => c.json(ok({ managed: isUnderLode(), state: isUnderLode() ? readLodeState() : null })),
  );

  // ── Request a hot-update to a version (or "latest") ────────────────────────
  router.post(
    "/update",
    describeRoute({
      tags: [TAGS.System],
      summary: "Request a lode hot-update ({version?}; default \"latest\")",
      responses: { ...jsonOk(z.any(), "Requested"), ...errors(401, 409) },
    }),
    async (c) => {
      if (!isUnderLode())
        return notManaged(c);
      const body = (await c.req.json().catch(() => ({}))) as { version?: unknown };
      const version = typeof body.version === "string" && body.version ? body.version : "latest";
      requestUpdate(version);
      c.get("logger").info({ version }, "lode: update requested via admin");
      return c.json(ok({ requested: version }));
    },
  );

  // ── Request a rollback (to a version, or last_good) ────────────────────────
  router.post(
    "/rollback",
    describeRoute({
      tags: [TAGS.System],
      summary: "Request a lode rollback ({version?}; default last_good)",
      responses: { ...jsonOk(z.any(), "Requested"), ...errors(401, 409) },
    }),
    async (c) => {
      if (!isUnderLode())
        return notManaged(c);
      const body = (await c.req.json().catch(() => ({}))) as { version?: unknown };
      const version = typeof body.version === "string" && body.version ? body.version : undefined;
      const chosen = requestRollback(version);
      if (!chosen) {
        return c.json(
          { success: false, error: { code: "NO_TARGET", message: "no version given and no last_good recorded" } },
          409,
        );
      }
      c.get("logger").info({ version: chosen }, "lode: rollback requested via admin");
      return c.json(ok({ requested: chosen }));
    },
  );

  // ── Request a restart of the current version ───────────────────────────────
  router.post(
    "/restart",
    describeRoute({
      tags: [TAGS.System],
      summary: "Request a lode restart of the current version",
      responses: { ...jsonOk(z.any(), "Requested"), ...errors(401, 409) },
    }),
    (c) => {
      if (!isUnderLode())
        return notManaged(c);
      requestRestart();
      c.get("logger").info("lode: restart requested via admin");
      return c.json(ok({ requested: "restart" }));
    },
  );

  return router;
}
