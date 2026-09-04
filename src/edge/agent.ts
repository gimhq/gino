import type { Context } from "hono";
import type { AgentCapabilities } from "@/agents/registry";
import type { AppDatabase } from "@/db";
import type { Agent } from "@/db/schema";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { upgradeWebSocket } from "hono/bun";
import { correlateLaunch, enrollAgent, reEnrollByLaunchNonce, setAgentStatus, touchLiveness, verifyConnectToken } from "@/agents/registry";
import {
  attachmentsBySha,
  getArtifactBySha,
  getAttachmentById,
  getGlobalConfigVersion,
  GLOBAL_SCOPE,
  isShaInAgentDesiredSet,
  MissingBlobError,
  resolveBlobRef,
  resolveEntriesForEngines,
  sanitizeAttachmentName,
  sha256Hex,
  ShaMismatchError,
} from "@/configstore";
import { wireAttachmentRef, writeAttachmentDescriptors } from "@/msgstore";
import { generateId, hashToken, safeEqual } from "@/platform";
import { handleInboundFrame, ingestHttpFrame, MalformedFrameError } from "@/protocol/inbound";
import { replayUnacked } from "@/protocol/outbox";
import { isSupportedProtocol, PROTOCOL_VERSION, registerError } from "@/protocol/version";
import { readBodyCapped } from "@/shared/lib/body-limit";
import { ForbiddenError, NotFoundError, UnauthorizedError } from "@/shared/lib/errors";
import { describeRoute, errors, TAGS } from "@/shared/lib/openapi";
import { bearerToken, connectTokenAuth } from "@/shared/middleware/auth";
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
function agentConfig(agentId: string, heartbeatIntervalMs: number) {
  return {
    agentId,
    wsUrl: `/agent/ws/${agentId}`,
    configUrl: `/agent/config/${agentId}`,
    outputUrl: `/agent/output/${agentId}`,
    heartbeatIntervalMs,
    protocolVersion: PROTOCOL_VERSION,
  };
}

/**
 * M8 — the resolved GLOBAL desired set covering ALL the agent's declared
 * engines (HOST-M8-03 — a multi-engine agent lands every engine's targets, not
 * just its first capability's) plus the host-assigned monotonic `version`,
 * delivered on register / `GET /agent/config` (additive; the agent reconciles
 * only when the version advances — agent-architecture §9b). Empty entries when
 * nothing is declared, so the response stays backward-compatible.
 */
function globalConfig(db: AppDatabase, agent: Agent) {
  return {
    entries: resolveEntriesForEngines(db, GLOBAL_SCOPE, agent.capabilities.engines),
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

/** True iff a request `If-None-Match` header matches the blob's strong ETag. */
function ifNoneMatchHits(header: string | undefined, etag: string): boolean {
  if (!header) {
    return false;
  }
  return header
    .split(",")
    .map(v => v.trim())
    .some(v => v === "*" || v === etag || v === `W/${etag}`);
}

/**
 * Serve a resolved blob from the unified store, integrity-checked against the
 * full sha256 (B6 byte channel). A missing blob 404s — including a dangling
 * registry/descriptor row whose bytes are gone (HOST-M8-12), never a 500; a
 * stored-bytes/sha256 mismatch is a 409 (the host never serves corrupt/
 * tampered bytes). Responses are content-addressed and immutable, so they
 * carry `Cache-Control: private, immutable` + a strong sha256 `ETag`, and a
 * matching `If-None-Match` short-circuits to a bodyless 304 (HOST-M8-14).
 */
function serveBlob(c: Context<AppEnv>, sel: { id?: string; sha256?: string }): Response {
  const db = c.get("db");
  const resolved = resolveBlobRef(db, sel);
  if (!resolved) {
    throw new NotFoundError("attachment", sel.id ?? sel.sha256 ?? "");
  }
  const etag = `"${resolved.sha256}"`;
  const cacheHeaders = {
    // `private` keeps shared caches out (auth rides the non-standard
    // x-internal-token header, which does not inhibit them); the bytes behind
    // a sha256 never change, hence `immutable`.
    "cache-control": "private, max-age=31536000, immutable",
    "etag": etag,
    "x-content-sha256": resolved.sha256,
  };
  if (ifNoneMatchHits(c.req.header("if-none-match"), etag)) {
    return new Response(null, { status: 304, headers: cacheHeaders });
  }
  const store = c.get("host").store;
  try {
    const bytes = store.readVerified(resolved.ref, resolved.sha256);
    // Copy out an exact ArrayBuffer (fresh-buffer slice) for the Response body.
    const body = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
    return new Response(body, {
      status: 200,
      headers: {
        "content-type": resolved.contentType,
        "content-length": String(bytes.byteLength),
        ...cacheHeaders,
      },
    });
  }
  catch (err) {
    if (err instanceof ShaMismatchError) {
      c.get("logger").error({ err, ref: resolved.ref }, "attachment serve: sha256 integrity failure");
      return c.json({ success: false, error: { code: "BLOB_INTEGRITY", message: "stored bytes failed sha256 verification" } }, 409);
    }
    if (err instanceof MissingBlobError) {
      c.get("logger").error({ err, ref: resolved.ref, sha256: resolved.sha256 }, "attachment serve: registry row points at a missing blob (dangling row — HOST-M8-12)");
      throw new NotFoundError("attachment", sel.id ?? sel.sha256 ?? "");
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
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Enroll an agent (returns agentId + connect token + config)",
      responses: {
        200: { description: "Enrolled" },
        400: { description: "`register_error{invalid_body|unsupported_protocol}` (protocol §3)" },
        401: { description: "`register_error{invalid_register_token}` (protocol §3)" },
        403: { description: "`register_error{registration_disabled}` (protocol §3)" },
        429: { description: "Rate limited (`register_error{rate_limited}`, protocol §3)" },
      },
    }),
    async (c) => {
      // HOST-M1M2-14: EVERY register failure renders the protocol §3
      // `register_error` envelope (an agent parses `reason` to decide between
      // "exit permanently" and "retry") — token validation lives here, in the
      // handler, so a bad token never falls through to the scaffold's generic
      // 401 envelope. The shared register token rides in the body
      // (`{ registerToken }`) with an `Authorization: Bearer` fallback; the
      // comparison is constant-time and fails CLOSED on an unconfigured secret.
      const body = await c.req.json().catch(() => null) as Record<string, unknown> | null;
      const host = c.get("host");
      const expected = host.options.registerToken;
      const bodyToken = body && typeof body.registerToken === "string" ? body.registerToken : null;
      const provided = bearerToken(c) ?? bodyToken;
      if (!expected || !provided || !safeEqual(provided, expected)) {
        return c.json(registerError("invalid_register_token"), 401);
      }
      if (!host.options.registrationEnabled) {
        return c.json(registerError("registration_disabled"), 403);
      }
      if (body == null || typeof body !== "object" || Array.isArray(body)) {
        return c.json(registerError("invalid_body"), 400);
      }

      // Version gate (protocol §1): reject anything but the current major.
      if (!isSupportedProtocol(body.protocolVersion)) {
        return c.json(registerError("unsupported_protocol"), 400);
      }

      const db = c.get("db");
      const launchNonce = typeof body.launchNonce === "string" && body.launchNonce.length > 0 ? body.launchNonce : null;

      // HOST-M3-06: registration is IDEMPOTENT per launch. A retry carrying a
      // nonce that already correlated (the response was lost — the agent never
      // saw its token) reuses the existing row with a ROTATED connect token
      // instead of minting a duplicate whose launch binding lands on a ghost.
      if (launchNonce) {
        const existing = reEnrollByLaunchNonce(db, launchNonce);
        if (existing) {
          c.get("logger").info({ agentId: existing.agent.id, source: existing.agent.source }, "agent register retry — existing enrollment reused (token rotated)");
          const retryConfig = { ...agentConfig(existing.agent.id, host.options.heartbeatIntervalMs), ...globalConfig(db, existing.agent) };
          return c.json({ agentId: existing.agent.id, connectToken: existing.connectToken, config: retryConfig });
        }
      }

      // HOST-M3-09: the body's `access` is IGNORED — a register-token holder
      // never self-declares `public`. Enrollment always lands `dedicated`; a
      // host launch's admin allowlist applies at correlate, and flipping an
      // agent `public` later is the admin-only access mutation.
      const { agent, connectToken } = enrollAgent(db, {
        ...(typeof body.name === "string" ? { name: body.name } : {}),
        source: typeof body.source === "string" ? body.source : "manual",
        capabilities: normalizeCapabilities(body.capabilities, body.maxThreads),
        // HOST-M3-15 provenance: which register token enrolled it — the
        // sha-256 at-rest form (the shared plaintext is never stored).
        registerTokenRef: hashToken(host.options.registerToken),
      });

      // OD17 launch↔register correlation (OPTIONAL, backward-compatible): when
      // the agent echoes the per-launch nonce it was launched with, bind it to
      // that launch (launcher_kind + launch_ref). No nonce → unchanged: a manual
      // enroll never touches the pending-launch set.
      if (launchNonce) {
        const correlated = correlateLaunch(db, agent.id, launchNonce);
        if (correlated) {
          c.get("logger").info({ agentId: agent.id, launcher: correlated.launcherKind, ref: correlated.launchRef }, "agent registration correlated to launch");
        }
      }

      c.get("logger").info({ agentId: agent.id, source: agent.source }, "agent enrolled");
      host.events.emit("agent.enrolled", { agentId: agent.id });
      // M8 (B6): seed the resolved GLOBAL desired set + version on register (additive).
      const config = { ...agentConfig(agent.id, host.options.heartbeatIntervalMs), ...globalConfig(db, agent) };
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
      const host = c.get("host");

      // Per-agent auth (M3): verify against the stored hash. Throwing here
      // rejects the upgrade with the standard 401 envelope (no socket opens).
      const agent = verifyConnectToken(db, agentId, token);
      if (!agent) {
        throw new UnauthorizedError();
      }

      // Host events (M6 HOST-M5M6-04: a stored thread_message triggers the webhook pump via `transcript.message`).
      const deps = { db, sender: host.sessions, agentId, logger, metrics: host.metrics, events: host.events };
      return {
        onOpen(_evt, ws) {
          // Attach BEFORE replay so the replayed frames find the live socket.
          host.sessions.attach(agentId, ws);
          setAgentStatus(db, agentId, "online");
          host.events.emit("agent.online", { agentId });
          // HOST-M3-08 / HOST-M1M2-05: stamp `last_seen` at attach so a fresh
          // connection starts inside the sweep's grace window (never insta-swept
          // on a stale or NULL `last_seen` before its first heartbeat).
          touchLiveness(db, agentId);
          const replayed = replayUnacked(db, host.sessions, agentId, { logger });
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
          const evicted = host.sessions.detach(agentId, ws);
          if (evicted) {
            setAgentStatus(db, agentId, "offline");
            host.events.emit("agent.offline", { agentId, reason: "close" });
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
      return c.json({ ...agentConfig(agentId, c.get("host").options.heartbeatIntervalMs), ...globalConfig(c.get("db"), agent) });
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
        // Host events (M6 HOST-M5M6-04: a stored thread_message triggers the webhook pump via `transcript.message`).
        const host = c.get("host");
        return c.json(ingestHttpFrame(
          { db: c.get("db"), agentId, logger: c.get("logger"), metrics: host.metrics, events: host.events },
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
      responses: { 201: { description: "Stored (returns the descriptor)" }, ...errors(400, 401, 413) },
    }),
    async (c) => {
      const requesterId = c.get("agentId");
      // Metadata rides query params (host-native form) OR headers (the runner's
      // form — agent-architecture §14 A1: `x-attachment-name` +
      // `x-attachment-sha256`, bytes as the raw body).
      const name = c.req.query("name") ?? c.req.header("x-attachment-name");
      const taskId = c.req.query("task_id");
      const threadId = c.req.query("thread_id");
      if (!name) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: "name query param or x-attachment-name header is required" } }, 400);
      }
      // The runner's upload carries NO task/thread context — the binding
      // arrives later on `thread_result.data.attachments` (protocol §5). When
      // a caller DOES bind at upload time it must name both ids, and an agent
      // may write attachments ONLY for its OWN placed tasks — authorize BEFORE
      // reading/storing any bytes (a foreign task_id never touches the store).
      if ((taskId == null) !== (threadId == null)) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: "task_id and thread_id must be provided together" } }, 400);
      }
      if (taskId != null && !isAgentPlacedOnTask(c.get("db"), requesterId, taskId)) {
        throw new ForbiddenError("cannot write an attachment for a task placed on another agent");
      }
      const contentType = c.req.query("content_type") ?? c.req.header("content-type") ?? "application/octet-stream";
      const directionRaw = c.req.query("direction");
      const direction: "inbound" | "outbound" = directionRaw === "inbound" ? "inbound" : "outbound";
      const declaredSha = c.req.query("sha256") ?? c.req.header("x-attachment-sha256");

      // HOST-M1M2-11: bounded ingest — declared Content-Length AND actual
      // streamed size are capped (413 via PayloadTooLargeError) before hashing.
      const host = c.get("host");
      const bytes = await readBodyCapped(c.req.raw, host.options.maxUploadBytes);
      const sha256 = sha256Hex(bytes);
      if (declaredSha && declaredSha !== sha256) {
        return c.json({ success: false, error: { code: "SHA_MISMATCH", message: `computed ${sha256} != declared ${declaredSha}` } }, 400);
      }

      const store = host.store;
      let blob;
      try {
        // The runner names an outbound file by its `output/`-relative path
        // (separators included) — flatten to a store segment with the same
        // function the descriptor registry uses, so the later
        // `thread_result` registration derives this exact store ref.
        blob = store.put(sanitizeAttachmentName(name), bytes);
      }
      catch (err) {
        return c.json({ success: false, error: { code: "BAD_REQUEST", message: err instanceof Error ? err.message : "store rejected the blob" } }, 400);
      }

      // The wire descriptor carries the RESOLVABLE byte-channel ref (the
      // runner parses the flat `{id, ref, …}` body and later fetches `ref`
      // joined to its host base — agent-architecture §14 A1); the store ref
      // stays host-private (derived again at registry-write time).
      const id = generateId("att");
      const descriptor = {
        id,
        name: blob.name,
        content_type: contentType,
        size: blob.size,
        sha256: blob.sha256,
        ref: wireAttachmentRef(id),
        direction,
      };
      if (taskId != null && threadId != null) {
        writeAttachmentDescriptors(c.get("db"), { taskId, threadId, descriptors: [descriptor] });
      }
      c.get("logger").info({ id: descriptor.id, taskId, threadId, sha256: blob.sha256, size: blob.size }, "attachment ingested");
      return c.json(descriptor, 201);
    },
  );

  return router;
}
