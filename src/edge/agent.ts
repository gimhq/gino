import type { Context } from "hono";
import type { AgentCapabilities } from "@/agents/registry";
import type { AppDatabase } from "@/db";
import type { Agent } from "@/db/schema";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { upgradeWebSocket } from "hono/bun";
import { correlateLaunch, enrollAgent, setAgentStatus, touchLiveness, verifyConnectToken } from "@/agents/registry";
import { sessions } from "@/agents/sessions";
import {
  attachmentsBySha,
  createStore,
  DEFAULT_ENGINE,
  getArtifactBySha,
  getAttachmentById,
  getGlobalConfigVersion,
  GLOBAL_SCOPE,
  isShaInAgentDesiredSet,
  resolveBlobRef,
  resolveEntriesForEngine,
  sha256Hex,
  ShaMismatchError,
} from "@/configstore";
import { writeAttachmentDescriptors } from "@/msgstore";
import { generateId } from "@/platform";
import { handleInboundFrame, ingestHttpFrame, MalformedFrameError } from "@/protocol/inbound";
import { replayUnacked } from "@/protocol/outbox";
import { HEARTBEAT_INTERVAL_MS, isSupportedProtocol, PROTOCOL_VERSION, registerError } from "@/protocol/version";
import { ForbiddenError, NotFoundError, UnauthorizedError } from "@/shared/lib/errors";
import { describeRoute, errors, TAGS } from "@/shared/lib/openapi";
import { connectTokenAuth, registerTokenAuth } from "@/shared/middleware/auth";
import { rateLimit } from "@/shared/middleware/rate-limit";
import { getTask } from "@/tasks";

/**
 * M1/M2/M3 — `/agent/*` data plane (mounted at `/agent`). Enrollment (M3) +
 * the WebSocket frame pipe (M2/M3) + the per-agent HTTP fallbacks. `POST
 * /register` carries the shared register token; every other endpoint carries
 * the per-agent connect token (`x-internal-token`, verified against
 * `agents.connect_token_hash`). `/attachments` is the M8/B6 byte channel —
 * out-of-band, sha256-verified bytes from the unified config/attachment store.
 *
 * Explicit routes only — no catch-all. An unknown `/agent/...` path 404s.
 */

/** The host-authoritative agent config object (protocol §3) — register + config mirror it. */
function agentConfig(agentId: string) {
  return {
    agentId,
    wsUrl: `/agent/ws/${agentId}`,
    configUrl: `/agent/config/${agentId}`,
    outputUrl: `/agent/output/${agentId}`,
    heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
    protocolVersion: PROTOCOL_VERSION,
  };
}

/** An agent's primary engine (the first declared capability), defaulting to claude. */
function primaryEngine(agent: Agent): string {
  return agent.capabilities.engines[0] ?? DEFAULT_ENGINE;
}

/**
 * M8 — the resolved GLOBAL desired set for the agent's engine plus the
 * host-assigned monotonic `version`, delivered on register / `GET
 * /agent/config` (additive; the agent reconciles only when the version
 * advances — agent-architecture §9b). Empty entries when nothing is declared,
 * so the response stays backward-compatible.
 */
function globalConfig(db: AppDatabase, agent: Agent) {
  return {
    entries: resolveEntriesForEngine(db, GLOBAL_SCOPE, primaryEngine(agent)),
    version: getGlobalConfigVersion(db),
  };
}

/**
 * Byte-channel authorization (B6/IDOR fix). The requesting agent identity is the
 * connect-token-resolved `c.get("agentId")` (set by `connectTokenAuth`), NEVER a
 * client-supplied param. These helpers gate every read/write against it.
 *
 * `403`-vs-`404` policy (consistent across the byte channel): an UNKNOWN resource
 * (no such attachment id/sha, no such config artifact) → `404`; a KNOWN resource
 * the requester is not authorized for → `403`.
 */

/** True iff `agentId` is the agent currently PLACED on `taskId` (set-once, §3). */
function isAgentPlacedOnTask(db: AppDatabase, agentId: string, taskId: string): boolean {
  const task = getTask(db, taskId);
  return task != null && task.agentId === agentId;
}

/**
 * Authorize + serve an attachment by descriptor `id`: the requester must be the
 * agent placed on the attachment's `task_id`. Unknown id → 404; wrong agent → 403.
 */
function serveAttachmentById(c: Context<AppEnv>, requesterId: string, id: string): Response {
  const db = c.get("db");
  const att = getAttachmentById(db, id);
  if (!att) {
    throw new NotFoundError("attachment", id);
  }
  if (!isAgentPlacedOnTask(db, requesterId, att.taskId)) {
    throw new ForbiddenError("attachment belongs to another agent's task");
  }
  return serveBlob(c, { id });
}

/**
 * Authorize + serve a blob by content `sha256`. Authorized when EITHER an
 * attachment carrying this sha is on a task placed on the requester, OR the sha
 * is in the requester's desired set (a config/skill artifact the host declared
 * for it). The bytes are content-addressed, so any authorized path serves the
 * same verified content. Unknown sha → 404; known-but-unauthorized → 403.
 */
function serveBySha(c: Context<AppEnv>, requesterId: string, sha256: string): Response {
  const db = c.get("db");
  const atts = attachmentsBySha(db, sha256);
  const artifact = getArtifactBySha(db, sha256);
  if (atts.length === 0 && !artifact) {
    throw new NotFoundError("attachment", sha256);
  }
  const placed = atts.some(a => isAgentPlacedOnTask(db, requesterId, a.taskId));
  const inDesiredSet = artifact != null && isShaInAgentDesiredSet(db, requesterId, sha256);
  if (!placed && !inDesiredSet) {
    throw new ForbiddenError("sha256 is neither on a task placed on this agent nor in its desired set");
  }
  return serveBlob(c, { sha256 });
}

/**
 * Serve a resolved blob from the unified store, integrity-checked against the
 * full sha256 (B6 byte channel). A missing blob 404s; a stored-bytes/sha256
 * mismatch is a 409 (the host never serves corrupt/tampered bytes).
 */
function serveBlob(c: Context<AppEnv>, sel: { id?: string; sha256?: string }): Response {
  const db = c.get("db");
  const resolved = resolveBlobRef(db, sel);
  if (!resolved) {
    throw new NotFoundError("attachment", sel.id ?? sel.sha256 ?? "");
  }
  const store = createStore(c.get("config"));
  try {
    const bytes = store.readVerified(resolved.ref, resolved.sha256);
    // Copy out an exact ArrayBuffer (fresh-buffer slice) for the Response body.
    const body = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
    return new Response(body, {
      status: 200,
      headers: {
        "content-type": resolved.contentType,
        "content-length": String(bytes.byteLength),
        "x-content-sha256": resolved.sha256,
      },
    });
  }
  catch (err) {
    if (err instanceof ShaMismatchError) {
      c.get("logger").error({ err, ref: resolved.ref }, "attachment serve: sha256 integrity failure");
      return c.json({ success: false, error: { code: "BLOB_INTEGRITY", message: "stored bytes failed sha256 verification" } }, 409);
    }
    throw err;
  }
}

/**
 * Normalise the register body's `capabilities` into the stored shape. Protocol
 * §3 sends an engine-name array (`["claude", …]`); the design stores
 * `{ engines, max_threads? }` (DP3 — `max_threads` host-overridable). Both the
 * array form and an object form are accepted.
 */
function normalizeCapabilities(raw: unknown, maxThreads?: unknown): AgentCapabilities {
  const mt = typeof maxThreads === "number" ? maxThreads : undefined;
  if (Array.isArray(raw)) {
    const engines = raw.filter((x): x is string => typeof x === "string");
    return { engines, ...(mt != null ? { max_threads: mt } : {}) };
  }
  if (raw != null && typeof raw === "object") {
    const obj = raw as { engines?: unknown; max_threads?: unknown };
    const engines = Array.isArray(obj.engines) ? obj.engines.filter((x): x is string => typeof x === "string") : [];
    const objMt = typeof obj.max_threads === "number" ? obj.max_threads : mt;
    return { engines, ...(objMt != null ? { max_threads: objMt } : {}) };
  }
  return { engines: [], ...(mt != null ? { max_threads: mt } : {}) };
}

export function agentRoutes() {
  const router = new Hono<AppEnv>();

  // ── Enrollment (M3) — register token; version-gated; mints the connect token.
  router.post(
    "/register",
    // HOST-M1M2-10 / HOST-M9-02: throttle enrollment per client IP BEFORE the
    // token check, so shared-secret brute force and register-retry loops are
    // bounded. Over-limit renders the protocol §3 `register_error{rate_limited}`
    // envelope (Retry-After is set by the middleware).
    rateLimit({
      bucket: "agent-register",
      limits: config => ({ windowMs: config.RATE_LIMIT_REGISTER_WINDOW_MS, max: config.RATE_LIMIT_REGISTER_MAX }),
      onLimit: c => c.json(registerError("rate_limited"), 429),
    }),
    registerTokenAuth,
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Enroll an agent (returns agentId + connect token + config)",
      responses: { 200: { description: "Enrolled" }, ...errors(400, 401), 429: { description: "Rate limited (`register_error{rate_limited}`, protocol §3)" } },
    }),
    async (c) => {
      const body = await c.req.json().catch(() => null) as Record<string, unknown> | null;

      // Version gate (protocol §1): reject anything but the current major.
      if (!body || !isSupportedProtocol(body.protocolVersion)) {
        return c.json(registerError("unsupported_protocol"), 400);
      }

      const db = c.get("db");
      const { agent, connectToken } = enrollAgent(db, {
        ...(typeof body.name === "string" ? { name: body.name } : {}),
        source: typeof body.source === "string" ? body.source : "manual",
        ...(typeof body.access === "string" ? { access: body.access } : {}),
        capabilities: normalizeCapabilities(body.capabilities, body.maxThreads),
      });

      // OD17 launch↔register correlation (OPTIONAL, backward-compatible): when
      // the agent echoes the per-launch nonce it was launched with, bind it to
      // that launch (launcher_kind + launch_ref). No nonce → unchanged: a manual
      // enroll never touches the pending-launch set.
      if (typeof body.launchNonce === "string" && body.launchNonce.length > 0) {
        const correlated = correlateLaunch(db, agent.id, body.launchNonce);
        if (correlated) {
          c.get("logger").info({ agentId: agent.id, launcher: correlated.launcherKind, ref: correlated.launchRef }, "agent registration correlated to launch");
        }
      }

      c.get("logger").info({ agentId: agent.id, source: agent.source }, "agent enrolled");
      // M8 (B6): seed the resolved GLOBAL desired set + version on register (additive).
      const config = { ...agentConfig(agent.id), ...globalConfig(db, agent) };
      return c.json({ agentId: agent.id, connectToken, config });
    },
  );

  // ── WS session + frame pipe (M2/M3) — connect token verified per-agent. The
  // upgrade is handled by the hono/bun adapter; the `websocket` handler is wired
  // into Bun.serve in src/index.ts (and the integration test).
  router.get(
    "/ws/:agentId",
    connectTokenAuth,
    upgradeWebSocket((c) => {
      const agentId = c.req.param("agentId");
      if (!agentId) {
        throw new UnauthorizedError();
      }
      const token = c.req.header("x-internal-token") ?? "";
      const db = c.get("db");
      const logger = c.get("logger");

      // Per-agent auth (M3): verify against the stored hash. Throwing here
      // rejects the upgrade with the standard 401 envelope (no socket opens).
      const agent = verifyConnectToken(db, agentId, token);
      if (!agent) {
        throw new UnauthorizedError();
      }

      // M6 (HOST-M5M6-04): a stored thread_message triggers the webhook pump.
      const pump = c.get("webhookPump");
      const deps = { db, sender: sessions, agentId, logger, ...(pump ? { onTranscriptWrite: pump.notifyTask } : {}) };
      return {
        onOpen(_evt, ws) {
          // Attach BEFORE replay so the replayed frames find the live socket.
          sessions.attach(agentId, ws, agent.connectTokenHash);
          setAgentStatus(db, agentId, "online");
          // HOST-M3-08 / HOST-M1M2-05: stamp `last_seen` at attach so a fresh
          // connection starts inside the sweep's grace window (never insta-swept
          // on a stale or NULL `last_seen` before its first heartbeat).
          touchLiveness(db, agentId);
          const replayed = replayUnacked(db, sessions, agentId, { logger });
          logger.info({ agentId, replayed }, "agent ws attached");
        },
        onMessage(evt, _ws) {
          const data = evt.data;
          if (typeof data === "string") {
            handleInboundFrame(deps, data);
          }
          else if (data instanceof ArrayBuffer) {
            handleInboundFrame(deps, data);
          }
          // Blob payloads are not produced by the bun adapter — ignored.
        },
        onClose(_evt, ws) {
          // HOST-M1M2-05 / HOST-M3-02: only the CURRENT socket's close marks the
          // agent offline — a superseded socket's late close (dirty reconnect)
          // must not clobber the live session's `online` status.
          const evicted = sessions.detach(agentId, ws);
          if (evicted) {
            setAgentStatus(db, agentId, "offline");
          }
          logger.info({ agentId, evicted }, "agent ws detached");
        },
      };
    }),
  );

  // ── Config pull (mirror of the register config) — connect token.
  router.get(
    "/config/:agentId",
    connectTokenAuth,
    describeRoute({
      tags: [TAGS.Config],
      summary: "Pull agent config (mirror of the register response)",
      responses: { 200: { description: "Config" }, ...errors(401) },
    }),
    (c) => {
      const agentId = c.req.param("agentId");
      const token = c.req.header("x-internal-token") ?? "";
      const agent = agentId ? verifyConnectToken(c.get("db"), agentId, token) : null;
      if (!agent || !agentId) {
        throw new UnauthorizedError();
      }
      // M8 (B6): mirror the resolved GLOBAL desired set + version alongside the config.
      return c.json({ ...agentConfig(agentId), ...globalConfig(c.get("db"), agent) });
    },
  );

  // ── Pre-WS thread_result fallback (protocol §2) — connect token → M5 store.
  router.post(
    "/output/:agentId",
    connectTokenAuth,
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Post agent output (pre-WS fallback) — routed through M2 inbound",
      responses: { 200: { description: "Stored (returns the ack)" }, ...errors(400, 401, 500) },
    }),
    async (c) => {
      const agentId = c.req.param("agentId");
      const token = c.req.header("x-internal-token") ?? "";
      const agent = agentId ? verifyConnectToken(c.get("db"), agentId, token) : null;
      if (!agent || !agentId) {
        throw new UnauthorizedError();
      }
      const raw = await c.req.text();
      // HOST-M1M2-09: only an undecodable frame is 400 (permanently bad — the
      // agent must drop it); a storage failure propagates to the error handler
      // as a 5xx so the agent retries the frame.
      try {
        // M6 (HOST-M5M6-04): a stored thread_message triggers the webhook pump.
        const pump = c.get("webhookPump");
        return c.json(ingestHttpFrame(
          { db: c.get("db"), agentId, logger: c.get("logger"), ...(pump ? { onTranscriptWrite: pump.notifyTask } : {}) },
          raw,
        ));
      }
      catch (err) {
        if (err instanceof MalformedFrameError) {
          c.get("logger").warn({ err, agentId }, "agent output fallback: malformed frame rejected");
          return c.json({ success: false, error: { code: "BAD_FRAME", message: "malformed frame" } }, 400);
        }
        c.get("logger").error({ err, agentId }, "agent output fallback: storage failure (agent should retry)");
        throw err;
      }
    },
  );

  // ── Attachment byte channel (M8/B6) — connect token; bytes ride out-of-band
  // here (never inline in frames, protocol §5), verified against the full
  // sha256. The unified store backs BOTH attachments and config/skill artifacts,
  // so a skill bundle is fetched the same way (no dedicated `/agent/skills`).

  // Download by content `sha256` or attachment `id` via query (`?sha256` | `?id`).
  router.get(
    "/attachments",
    connectTokenAuth,
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Download a blob by ?sha256 or ?id (verified bytes)",
      responses: { 200: { description: "Bytes" }, ...errors(400, 401, 404, 409) },
    }),
    (c) => {
      const requesterId = c.get("agentId");
      const sha256 = c.req.query("sha256");
      const id = c.req.query("id");
      if (!sha256 && !id) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: "id or sha256 query param required" } }, 400);
      }
      // `id` addresses an attachment (authorize by task placement); `sha256`
      // addresses an attachment OR a config artifact (authorize by placement or
      // desired-set membership). Never serve arbitrary store bytes by sha.
      return id ? serveAttachmentById(c, requesterId, id) : serveBySha(c, requesterId, sha256!);
    },
  );

  // Download by attachment `id` (path form — protocol §3 `GET /agent/attachments/:id`).
  router.get(
    "/attachments/:id",
    connectTokenAuth,
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Download an attachment by id (verified bytes)",
      responses: { 200: { description: "Bytes" }, ...errors(401, 404, 409) },
    }),
    (c) => {
      return serveAttachmentById(c, c.get("agentId"), c.req.param("id"));
    },
  );

  // Ingest bytes → unified store + an `attachments` descriptor. Metadata rides
  // query params; the raw request body is the bytes. A declared `?sha256` that
  // does not match the computed digest is rejected (the host stores nothing).
  router.post(
    "/attachments",
    connectTokenAuth,
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Ingest attachment bytes → store + descriptor",
      responses: { 201: { description: "Stored (returns the descriptor)" }, ...errors(400, 401) },
    }),
    async (c) => {
      const requesterId = c.get("agentId");
      const name = c.req.query("name");
      const taskId = c.req.query("task_id");
      const threadId = c.req.query("thread_id");
      if (!name || !taskId || !threadId) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: "name, task_id, thread_id query params are required" } }, 400);
      }
      // An agent may write attachments ONLY for its OWN placed tasks — authorize
      // BEFORE reading/storing any bytes (a foreign task_id never touches the store).
      if (!isAgentPlacedOnTask(c.get("db"), requesterId, taskId)) {
        throw new ForbiddenError("cannot write an attachment for a task placed on another agent");
      }
      const contentType = c.req.query("content_type") ?? c.req.header("content-type") ?? "application/octet-stream";
      const directionRaw = c.req.query("direction");
      const direction: "inbound" | "outbound" = directionRaw === "inbound" ? "inbound" : "outbound";
      const declaredSha = c.req.query("sha256");

      const bytes = new Uint8Array(await c.req.arrayBuffer());
      const sha256 = sha256Hex(bytes);
      if (declaredSha && declaredSha !== sha256) {
        return c.json({ success: false, error: { code: "SHA_MISMATCH", message: `computed ${sha256} != declared ${declaredSha}` } }, 400);
      }

      const store = createStore(c.get("config"));
      let blob;
      try {
        blob = store.put(name, bytes);
      }
      catch (err) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: err instanceof Error ? err.message : "store rejected the blob" } }, 400);
      }

      const descriptor = {
        id: generateId("att"),
        name: blob.name,
        content_type: contentType,
        size: blob.size,
        sha256: blob.sha256,
        ref: blob.ref,
        direction,
      };
      writeAttachmentDescriptors(c.get("db"), { taskId, threadId, descriptors: [descriptor] });
      c.get("logger").info({ id: descriptor.id, taskId, threadId, sha256: blob.sha256, size: blob.size }, "attachment ingested");
      return c.json({ success: true, data: descriptor }, 201);
    },
  );

  return router;
}
