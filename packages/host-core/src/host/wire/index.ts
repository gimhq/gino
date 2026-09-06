// HOST WIRE — the `/agent/*` data plane without a framework (host-sdk-design
// §3.2, §5 step 4). Every handler here is a plain function over the host
// context: it returns an `HttpResult` (or a typed outcome) and throws the
// existing `AppError` subclasses. The Hono adapter (`src/edge/agent.ts`) is a
// 1:1 mount — rate limiting, OpenAPI metadata, connect-token middleware, body
// caps and the WebSocket upgrade stay there.
import type { HostContext } from "../context";
import type { WsLike } from "../ports/transport";
import { getAgentByConnectToken, verifyConnectToken } from "../../agents/registry";
import { attachmentsWire } from "./attachments";
import { configFor } from "./config";
import { ingestHttpFrame } from "./output";
import { register } from "./register";
import { attachSocket, detachSocket, onSocketMessage } from "./socket";

/** A framework-agnostic HTTP response: `body` is a JSON value, raw bytes, or `null` (bodyless). */
export interface HttpResult {
  status: number;
  headers?: Record<string, string>;
  body: unknown;
}

export interface AttachmentIngestInput {
  name: string | undefined;
  taskId?: string | undefined;
  threadId?: string | undefined;
  contentType: string;
  direction: "inbound" | "outbound";
  declaredSha?: string | undefined;
  bytes: Uint8Array;
}

export interface AttachmentsWire {
  /** `200` bytes | `304` | `404` unknown id | `403` foreign task | `409` BLOB_INTEGRITY. */
  serveById: (requesterId: string, id: string, opts: { ifNoneMatch?: string | undefined }) => HttpResult;
  /** Same as `serveById`, addressed by content sha256 (attachment OR config artifact). */
  serveBySha: (requesterId: string, sha256: string, opts: { ifNoneMatch?: string | undefined }) => HttpResult;
  /** `201` descriptor | `400` | throws `ForbiddenError` on a foreign task. */
  ingest: (requesterId: string, input: AttachmentIngestInput) => HttpResult;
}

export interface Wire {
  /** `POST /agent/register` — every failure is a protocol §3 `register_error` envelope. */
  register: (body: unknown, auth: { bearer: string | null }) => HttpResult;
  /** Resolve a connect token to its owning (non-removed) agent — `connectTokenAuth`. */
  resolveAgent: (connectToken: string) => { agentId: string } | null;
  /** Per-agent connect-token check against the stored hash (ws / config / output). */
  authorizeAgent: (agentId: string, connectToken: string) => boolean;
  /** `GET /agent/config/:agentId` — the caller has already authorized. */
  configFor: (agentId: string) => HttpResult;
  /** `POST /agent/output/:agentId` — `200` ack | `400` BAD_FRAME; a storage failure THROWS. */
  ingestHttpFrame: (agentId: string, raw: string | Uint8Array) => HttpResult;
  /** WS onOpen: attach → status online → events → touchLiveness → replay. */
  attachSocket: (agentId: string, ws: WsLike) => void;
  /** WS onMessage: the M2 inbound frame pipe. */
  onSocketMessage: (agentId: string, data: string | ArrayBuffer) => void;
  /** WS onClose: status offline ONLY when the current socket was evicted. */
  detachSocket: (agentId: string, ws: WsLike) => void;
  attachments: AttachmentsWire;
}

/** The slice of the context the wire closes over (a full `HostContext` satisfies it). */
export type WireContext = Pick<HostContext, "storage" | "sessions" | "logger" | "metrics" | "events" | "options">;

export function createWire(ctx: WireContext): Wire {
  return {
    register: (body, auth) => register(ctx, body, auth),
    resolveAgent: (connectToken) => {
      const agent = getAgentByConnectToken(ctx.storage, connectToken);
      return agent ? { agentId: agent.id } : null;
    },
    authorizeAgent: (agentId, connectToken) => verifyConnectToken(ctx.storage, agentId, connectToken) != null,
    configFor: agentId => configFor(ctx, agentId),
    ingestHttpFrame: (agentId, raw) => ingestHttpFrame(ctx, agentId, raw),
    attachSocket: (agentId, ws) => attachSocket(ctx, agentId, ws),
    onSocketMessage: (agentId, data) => onSocketMessage(ctx, agentId, data),
    detachSocket: (agentId, ws) => detachSocket(ctx, agentId, ws),
    attachments: attachmentsWire(ctx),
  };
}
