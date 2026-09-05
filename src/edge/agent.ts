import type { Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import type { HttpResult } from "@/host";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { upgradeWebSocket } from "hono/bun";
import { registerError } from "@/protocol/version";
import { readBodyCapped } from "@/shared/lib/body-limit";
import { UnauthorizedError } from "@/shared/lib/errors";
import { describeRoute, errors, TAGS } from "@/shared/lib/openapi";
import { bearerToken, connectTokenAuth } from "@/shared/middleware/auth";
import { rateLimit } from "@/shared/middleware/rate-limit";

/**
 * M1/M2/M3 — `/agent/*` data plane (mounted at `/agent`): a thin Hono mount over
 * `host.wire` (src/host/wire — host-sdk-design §3.2). The protocol logic lives
 * there; this shim owns only rate limiting, OpenAPI metadata, the connect-token
 * middleware, the body cap and the WebSocket upgrade. `POST /register` carries
 * the shared register token; everything else the per-agent `x-internal-token`.
 * Explicit routes only — no catch-all. An unknown `/agent/...` path 404s.
 */

/** Map a framework-free `HttpResult` onto a hono `Response`. */
function toResponse(c: Context<AppEnv>, result: HttpResult): Response {
  const status = result.status as ContentfulStatusCode;
  const headers = result.headers ?? {};
  if (result.body instanceof Uint8Array) {
    // Copy out an exact ArrayBuffer (fresh-buffer slice) for the Response body.
    const body = result.body.buffer.slice(result.body.byteOffset, result.body.byteOffset + result.body.byteLength) as ArrayBuffer;
    return new Response(body, { status, headers });
  }
  if (result.body === null) {
    return new Response(null, { status, headers });
  }
  return c.json(result.body, status, headers);
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
      const body: unknown = await c.req.json().catch(() => null);
      return toResponse(c, c.get("host").wire.register(body, { bearer: bearerToken(c) }));
    },
  );

  // ── WS session + frame pipe (M2/M3) — connect token verified per-agent (upgrade by hono/bun).
  router.get(
    "/ws/:agentId",
    connectTokenAuth,
    upgradeWebSocket((c) => {
      const agentId = c.req.param("agentId");
      const wire = c.get("host").wire;
      // Per-agent auth (M3): verify against the stored hash. Throwing here
      // rejects the upgrade with the standard 401 envelope (no socket opens).
      if (!agentId || !wire.authorizeAgent(agentId, c.req.header("x-internal-token") ?? "")) {
        throw new UnauthorizedError();
      }
      return {
        onOpen: (_evt, ws) => wire.attachSocket(agentId, ws),
        onMessage: (evt) => {
          const data = evt.data;
          if (typeof data === "string" || data instanceof ArrayBuffer) {
            wire.onSocketMessage(agentId, data);
          }
          // Blob payloads are not produced by the bun adapter — ignored.
        },
        onClose: (_evt, ws) => wire.detachSocket(agentId, ws),
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
      const wire = c.get("host").wire;
      if (!agentId || !wire.authorizeAgent(agentId, c.req.header("x-internal-token") ?? "")) {
        throw new UnauthorizedError();
      }
      return toResponse(c, wire.configFor(agentId));
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
      const wire = c.get("host").wire;
      if (!agentId || !wire.authorizeAgent(agentId, c.req.header("x-internal-token") ?? "")) {
        throw new UnauthorizedError();
      }
      return toResponse(c, wire.ingestHttpFrame(agentId, await c.req.text()));
    },
  );

  // ── Attachment byte channel (M8/B6) — connect token; the requester identity
  // is the token-resolved `agentId`. Download by `?sha256` | `?id` query.
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
      const { attachments } = c.get("host").wire;
      const opts = { ifNoneMatch: c.req.header("if-none-match") };
      return toResponse(c, id ? attachments.serveById(requesterId, id, opts) : attachments.serveBySha(requesterId, sha256!, opts));
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
      return toResponse(c, c.get("host").wire.attachments.serveById(c.get("agentId"), c.req.param("id"), { ifNoneMatch: c.req.header("if-none-match") }));
    },
  );

  // Ingest bytes → store + descriptor. Metadata rides query params (host-native)
  // OR headers (the runner's form — agent-architecture §14 A1); the body is the bytes.
  router.post(
    "/attachments",
    connectTokenAuth,
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Ingest attachment bytes → store + descriptor",
      responses: { 201: { description: "Stored (returns the descriptor)" }, ...errors(400, 401, 413) },
    }),
    async (c) => {
      const host = c.get("host");
      // HOST-M1M2-11: bounded ingest — declared Content-Length AND actual
      // streamed size are capped (413 via PayloadTooLargeError) before hashing.
      const bytes = await readBodyCapped(c.req.raw, host.options.maxUploadBytes);
      return toResponse(c, host.wire.attachments.ingest(c.get("agentId"), {
        name: c.req.query("name") ?? c.req.header("x-attachment-name"),
        taskId: c.req.query("task_id"),
        threadId: c.req.query("thread_id"),
        contentType: c.req.query("content_type") ?? c.req.header("content-type") ?? "application/octet-stream",
        direction: c.req.query("direction") === "inbound" ? "inbound" : "outbound",
        declaredSha: c.req.query("sha256") ?? c.req.header("x-attachment-sha256"),
        bytes,
      }));
    },
  );

  return router;
}
