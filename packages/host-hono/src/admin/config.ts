// M8 — `/admin/config` control API (mounted under `/admin`, behind the channel
// token via the parent router). The config/skills provisioning surface (§7):
//   POST /config/artifacts   upload/version a skill dir | instruction file
//                            (raw body = bytes; ?name & ?kind & ?content_type metadata)
//   PUT  /config/entries     set the DESIRED set for a scope ('global' | <task_id>)
//                            ({scope, engine?, entries:[{name, sha256, kind, target?}]})
//
// The host is the SINGLE SOURCE OF TRUTH: artifacts land content-addressed in the
// unified store (dedup by sha256); entries declare the per-scope desired set with
// host-resolved per-engine `target`. The host NEVER materializes on an agent — it
// only declares + serves; the agent reconciles. Responses use `{ success, data }`.
import type { HostEnv } from "../env";
import { broadcastGlobalConfig, DEFAULT_ENGINE, getGlobalConfigVersion, getTask, GLOBAL_SCOPE, ingestArtifact, readBodyCapped, resolveEntries, setEntries } from "@gino/host-core/domain";
import { Hono } from "hono";
import { z } from "zod";
import { ok } from "../api-response";
import { describeRoute, errors, jsonCreated, jsonOk, TAGS, validator } from "../openapi";

const entriesBody = z.object({
  scope: z.string().min(1),
  /** Engine for host-side `target` resolution (defaults to the task's engine, then claude). */
  engine: z.string().min(1).optional(),
  entries: z.array(z.object({
    name: z.string().min(1),
    sha256: z.string().min(1),
    kind: z.string().min(1),
    target: z.string().min(1).optional(),
  })),
});

export function adminConfigRoutes() {
  const router = new Hono<HostEnv>();

  // ── Upload / version an artifact (skill dir | instruction file) ────────────
  router.post(
    "/artifacts",
    describeRoute({
      tags: [TAGS.Config],
      summary: "Upload/version a config artifact (raw bytes; ?name & ?kind)",
      responses: { ...jsonCreated(z.any(), "Stored"), ...errors(400, 401, 413) },
    }),
    async (c) => {
      const name = c.req.query("name");
      const kind = c.req.query("kind");
      if (!name || !kind) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: "name and kind query params are required" } }, 400);
      }
      if (kind !== "file" && kind !== "dir") {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: "kind must be 'file' or 'dir'" } }, 400);
      }
      const contentType = c.req.query("content_type") ?? c.req.header("content-type");
      // Bounded ingest — declared Content-Length AND actual
      // streamed size are capped (413 via PayloadTooLargeError) before hashing.
      const host = c.get("host");
      const bytes = await readBodyCapped(c.req.raw, host.options.maxUploadBytes);

      try {
        const artifact = ingestArtifact(host.storage, {
          name,
          kind,
          bytes,
          ...(contentType ? { contentType } : {}),
        });
        c.get("logger").info({ name: artifact.name, sha256: artifact.sha256, kind, size: artifact.size }, "config artifact ingested");
        return c.json(ok(artifact), 201);
      }
      catch (err) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: err instanceof Error ? err.message : "store rejected the artifact" } }, 400);
      }
    },
  );

  // ── Set the desired set for a scope ('global' | <task_id>) ─────────────────
  router.put(
    "/entries",
    describeRoute({
      tags: [TAGS.Config],
      summary: "Set the desired config set for a scope (host resolves target)",
      responses: { ...jsonOk(z.any(), "Set"), ...errors(401, 422) },
    }),
    validator("json", entriesBody),
    (c) => {
      const storage = c.get("host").storage;
      const b = c.req.valid("json");
      // Resolve the engine: explicit > the task's engine (for a task scope) > claude.
      const taskEngine = b.scope !== GLOBAL_SCOPE ? getTask(storage, b.scope)?.engine : undefined;
      const engine = b.engine ?? taskEngine ?? DEFAULT_ENGINE;
      const inputs = b.entries.map(e => ({
        name: e.name,
        sha256: e.sha256,
        kind: e.kind,
        ...(e.target != null ? { target: e.target } : {}),
      }));
      const resolved = resolveEntries(engine, inputs);
      const persisted = setEntries(storage, b.scope, resolved);
      if (b.scope === GLOBAL_SCOPE) {
        // M8 delivery: a global change bumps the monotonic version (inside
        // setEntries) and is re-pushed live to every online agent; offline
        // agents converge on their next versioned config pull.
        const version = getGlobalConfigVersion(storage);
        const pushed = broadcastGlobalConfig(storage, c.get("host").sessions);
        c.get("logger").info({ scope: b.scope, engine, count: persisted.length, version, pushed: pushed.length }, "config entries set");
        return c.json(ok({ scope: b.scope, engine, entries: persisted, version }));
      }
      c.get("logger").info({ scope: b.scope, engine, count: persisted.length }, "config entries set");
      return c.json(ok({ scope: b.scope, engine, entries: persisted }));
    },
  );

  return router;
}
