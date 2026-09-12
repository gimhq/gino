import type { Host, HttpResult } from "@gino/host-core";
import { AppError, ForbiddenError, UnauthorizedError } from "@gino/host-core";
import { readBodyCapped } from "@gino/host-core/domain";

/**
 * The `/agent` data plane over `host.wire` with no HTTP framework: one function
 * from a web-standard `Request` to a `Response`, plus the WebSocket handshake
 * decision. The protocol logic lives in host-core; this module owns only the
 * path matching, the connect-token check and the `HttpResult → Response` map.
 * `./bun` and `./node` put a listener in front of it; any other runtime can do
 * the same in a few lines.
 *
 * Routes (relative to `basePath`, default `/agent`), mirroring `@gino/host-hono`'s
 * `agentRouter`: `POST /register`, `GET /ws/:agentId` (handshake only — see
 * {@link AgentHandler.socket}), `GET /config/:agentId`, `POST /output/:agentId`,
 * `GET /attachments?id|sha256`, `GET /attachments/:id`, `POST /attachments`.
 */

export interface AgentHandlerOptions {
  /** Path prefix the agent dials (`config.wsUrl` etc. are relative to it). Default `/agent`. */
  readonly basePath?: string;
  /** Byte cap for `POST /attachments` (default: `host.options.maxUploadBytes`). */
  readonly maxUploadBytes?: number;
}

/** The authorized outcome of a WebSocket handshake: the agent the connect token resolved to. */
export interface SocketAccept {
  readonly agentId: string;
}

export interface AgentHandler {
  /**
   * Route one HTTP request. `null` when the path is outside the agent data
   * plane (serve your own routes then); a `Response` otherwise — including the
   * protocol error envelope for a rejected request.
   */
  fetch: (req: Request) => Promise<Response | null>;
  /**
   * Decide a WebSocket handshake. `null` when `req` is not the `/ws/:agentId`
   * path; a 401/403 `Response` to send instead of upgrading; or the accepted
   * agent. After upgrading, drive `host.wire.attachSocket` /
   * `onSocketMessage` / `detachSocket` with that `agentId`.
   */
  socket: (req: Request) => SocketAccept | Response | null;
}

const BEARER_PREFIX = "bearer ";

function bearerToken(req: Request): string | null {
  const header = req.headers.get("authorization")?.trim();
  if (!header || !header.toLowerCase().startsWith(BEARER_PREFIX)) {
    return null;
  }
  const token = header.slice(BEARER_PREFIX.length).trim();
  return token.length > 0 ? token : null;
}

/** Map a framework-free `HttpResult` onto a web-standard `Response`. */
export function toResponse(result: HttpResult): Response {
  const init = { status: result.status, headers: result.headers ?? {} };
  if (result.body instanceof Uint8Array) {
    const body = result.body.buffer.slice(result.body.byteOffset, result.body.byteOffset + result.body.byteLength) as ArrayBuffer;
    return new Response(body, init);
  }
  if (result.body === null) {
    return new Response(null, init);
  }
  return Response.json(result.body, init);
}

/** The protocol error envelope for a thrown error (`AppError` → its status; anything else → a logged 500). */
export function errorResponse(host: Host, err: unknown): Response {
  if (err instanceof AppError) {
    return Response.json(err.toJSON(), { status: err.statusCode });
  }
  host.logger.error({ err }, "unhandled error");
  return Response.json({ success: false, error: { code: "INTERNAL_ERROR", message: "Internal server error" } }, { status: 500 });
}

function notFound(): Response {
  return Response.json({ success: false, error: { code: "NOT_FOUND", message: "Not found" } }, { status: 404 });
}

/** Strip `basePath` and split the remainder into segments; `null` when the request is outside it. */
function agentPath(url: URL, basePath: string): string[] | null {
  if (!url.pathname.startsWith(`${basePath}/`)) {
    return null;
  }
  return url.pathname.slice(basePath.length + 1).split("/").filter(Boolean);
}

export function createAgentHandler(host: Host, opts: AgentHandlerOptions = {}): AgentHandler {
  const basePath = (opts.basePath ?? "/agent").replace(/\/+$/, "");
  const maxUploadBytes = opts.maxUploadBytes ?? host.options.maxUploadBytes;
  const { wire } = host;

  /**
   * The per-agent connect token (`x-internal-token`) is the identity: resolve
   * it, and refuse a route whose `:agentId` names a different agent.
   */
  function authenticate(req: Request, pathAgentId?: string): string {
    const token = req.headers.get("x-internal-token");
    if (!token || token.trim().length === 0) {
      throw new UnauthorizedError();
    }
    const agent = wire.resolveAgent(token);
    if (!agent) {
      throw new UnauthorizedError();
    }
    if (pathAgentId !== undefined && pathAgentId !== agent.agentId) {
      throw new ForbiddenError();
    }
    return agent.agentId;
  }

  async function route(req: Request, url: URL, segments: string[]): Promise<Response> {
    const [head, second, third] = segments;
    const { method } = req;

    if (head === "register" && second === undefined && method === "POST") {
      const body: unknown = await req.json().catch(() => null);
      return toResponse(wire.register(body, { bearer: bearerToken(req) }));
    }
    if (head === "ws" && second !== undefined && third === undefined && method === "GET") {
      // Reached over plain HTTP (no upgrade): same auth as the handshake, then 426.
      authenticate(req, second);
      return Response.json({ success: false, error: { code: "UPGRADE_REQUIRED", message: "WebSocket upgrade required" } }, { status: 426 });
    }
    if (head === "config" && second !== undefined && third === undefined && method === "GET") {
      const agentId = authenticate(req, second);
      return toResponse(wire.configFor(agentId));
    }
    if (head === "output" && second !== undefined && third === undefined && method === "POST") {
      const agentId = authenticate(req, second);
      return toResponse(wire.ingestHttpFrame(agentId, await req.text()));
    }
    if (head === "attachments" && third === undefined) {
      const ifNoneMatch = req.headers.get("if-none-match") ?? undefined;
      if (second !== undefined && method === "GET") {
        return toResponse(wire.attachments.serveById(authenticate(req), second, { ifNoneMatch }));
      }
      if (second === undefined && method === "GET") {
        const requesterId = authenticate(req);
        const sha256 = url.searchParams.get("sha256");
        const id = url.searchParams.get("id");
        if (!sha256 && !id) {
          return Response.json({ success: false, error: { code: "BAD_REQUEST", message: "id or sha256 query param required" } }, { status: 400 });
        }
        return toResponse(id ? wire.attachments.serveById(requesterId, id, { ifNoneMatch }) : wire.attachments.serveBySha(requesterId, sha256!, { ifNoneMatch }));
      }
      if (second === undefined && method === "POST") {
        const requesterId = authenticate(req);
        const q = url.searchParams;
        const bytes = await readBodyCapped(req, maxUploadBytes);
        return toResponse(wire.attachments.ingest(requesterId, {
          name: q.get("name") ?? req.headers.get("x-attachment-name") ?? undefined,
          taskId: q.get("task_id") ?? undefined,
          threadId: q.get("thread_id") ?? undefined,
          contentType: q.get("content_type") ?? req.headers.get("content-type") ?? "application/octet-stream",
          direction: q.get("direction") === "inbound" ? "inbound" : "outbound",
          declaredSha: q.get("sha256") ?? req.headers.get("x-attachment-sha256") ?? undefined,
          bytes,
        }));
      }
    }
    return notFound();
  }

  return {
    async fetch(req) {
      const url = new URL(req.url);
      const segments = agentPath(url, basePath);
      if (segments === null) {
        return null;
      }
      try {
        return await route(req, url, segments);
      }
      catch (err) {
        return errorResponse(host, err);
      }
    },
    socket(req) {
      const segments = agentPath(new URL(req.url), basePath);
      if (segments === null || segments.length !== 2 || segments[0] !== "ws" || req.method !== "GET") {
        return null;
      }
      const pathAgentId = segments[1]!;
      try {
        const agentId = authenticate(req, pathAgentId);
        if (!wire.authorizeAgent(agentId, req.headers.get("x-internal-token") ?? "")) {
          throw new UnauthorizedError();
        }
        return { agentId };
      }
      catch (err) {
        return errorResponse(host, err);
      }
    },
  };
}

/** Listener options shared by `./bun` and `./node`. */
export interface ServeOptions extends AgentHandlerOptions {
  /** Port to listen on (`0` picks a free port). Default `0`. */
  readonly port?: number;
  /** Bind address. Default `127.0.0.1`. */
  readonly hostname?: string;
  /** PEM key + cert to terminate TLS in-process; omit to serve plain HTTP (a reverse proxy usually terminates). */
  readonly tls?: { readonly key: string; readonly cert: string };
  /** Serve requests outside the agent data plane (your own routes). Default: a 404 envelope. */
  readonly fallback?: (req: Request) => Response | Promise<Response>;
}

/** A running listener. */
export interface ServedHost {
  /** `http(s)://hostname:port` as bound. */
  readonly url: string;
  readonly port: number;
  /** Close the listener and every agent socket; resolves once closed. */
  stop: () => Promise<void>;
}

/** The default `fallback`: the same 404 envelope an unknown `/agent/...` path gets. */
export function notFoundFallback(): Response {
  return notFound();
}
