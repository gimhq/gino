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

/**
 * One upload as the adapter hands it over: the already-read `bytes`, the
 * declared metadata, and the task/thread the attachment belongs to. Pass
 * `declaredSha` when the request carried one — the wire rejects a mismatch
 * rather than storing bytes under a lie.
 */
export interface AttachmentIngestInput {
  name: string | undefined;
  taskId?: string | undefined;
  threadId?: string | undefined;
  contentType: string;
  direction: "inbound" | "outbound";
  declaredSha?: string | undefined;
  bytes: Uint8Array;
}

/**
 * The byte channel. Bytes never ride inside frames: an agent uploads here and
 * fetches here, and every transfer is verified against the full sha256, so a
 * corrupt or tampered blob is never served. `requesterId` MUST be the identity
 * resolved from the connect token, never a client-supplied parameter — it is
 * the whole authorization input.
 */
export interface AttachmentsWire {
  /**
   * Serve an attachment by descriptor id to the agent placed on its task.
   * `200` with the bytes, `304` when `ifNoneMatch` matches the sha256 ETag,
   * or `409` `BLOB_INTEGRITY` when the stored bytes fail verification.
   * @throws NotFoundError unknown id, or a descriptor whose bytes are gone;
   * `ForbiddenError` when the attachment belongs to another agent's task.
   */
  serveById: (requesterId: string, id: string, opts: { ifNoneMatch?: string | undefined }) => HttpResult;
  /**
   * Serve by content sha256 — an attachment on one of the requester's tasks,
   * or a config artifact in its desired set. Same status codes as `serveById`.
   * This is also how a skill bundle is fetched; there is no separate endpoint.
   * @throws NotFoundError no attachment and no artifact carries the sha;
   * `ForbiddenError` when it exists but the requester is authorized for neither.
   */
  serveBySha: (requesterId: string, sha256: string, opts: { ifNoneMatch?: string | undefined }) => HttpResult;
  /**
   * Store bytes and mint a descriptor. `201` with the wire descriptor whose
   * `ref` the agent resolves against its host base URL; `400` for a missing
   * name, a `taskId`/`threadId` supplied without the other, or a `declaredSha`
   * that does not match the computed digest. Binding at upload time is
   * optional — the runner binds later via `thread_result`.
   * @throws ForbiddenError writing for a task placed on another agent.
   */
  ingest: (requesterId: string, input: AttachmentIngestInput) => HttpResult;
}

/**
 * The agent-facing data plane with no framework in it. Each handler is a plain
 * function returning `{ status, headers?, body }` or a typed outcome, and
 * throwing the `AppError` family; an adapter mounts them 1:1 and adds its own
 * routing, body caps, rate limiting and WebSocket upgrade.
 *
 * The socket triple is the entire transport contract. Ordering inside it is
 * protocol-visible and must not be re-implemented in an adapter: call
 * `attachSocket` once on open, `onSocketMessage` for every inbound message,
 * and `detachSocket` on close, always with the same socket handle.
 */
export interface Wire {
  /**
   * Enroll an agent. Verifies the protocol version, the shared register token
   * from `auth.bearer` and the registration switch, correlates an optional
   * launch nonce, and returns the agent id, its one-time connect token and the
   * config payload. IDEMPOTENT per launch: a retry carrying a nonce that
   * already correlated reuses the existing agent with a rotated connect token
   * rather than minting a duplicate. Never throws for a client error — every
   * failure is a `register_error` envelope with the right status, so the agent
   * gets a protocol answer rather than the adapter's error page. The body's
   * `access` is ignored; enrollment always lands `dedicated`.
   */
  register: (body: unknown, auth: { bearer: string | null }) => HttpResult;
  /**
   * Resolve a presented connect token to its owning agent, or `null`. Removed
   * agents never resolve. Use it as the adapter's authentication middleware.
   */
  resolveAgent: (connectToken: string) => { agentId: string } | null;
  /**
   * Check a connect token against ONE agent's stored hash — the per-route
   * check for handlers that take the agent id from the path.
   */
  authorizeAgent: (agentId: string, connectToken: string) => boolean;
  /**
   * The agent's current config: heartbeat cadence, the global config version
   * and the desired set resolved for the engines it advertises. Authorize
   * before calling.
   * @throws NotFoundError unknown agent.
   */
  configFor: (agentId: string) => HttpResult;
  /**
   * The HTTP fallback for one inbound frame, for agents that cannot hold a
   * socket. `200` with the ack body once the frame is durably handled, `400`
   * `BAD_FRAME` for something unparseable — which the agent must not retry. A
   * STORAGE failure throws instead of answering, so the agent retries; do not
   * convert that into a 4xx.
   */
  ingestHttpFrame: (agentId: string, raw: string | Uint8Array) => HttpResult;
  /**
   * Adopt a newly opened socket: register it as the agent's current session,
   * superseding and closing any previous one, mark the agent online, touch
   * liveness, then REPLAY every unacked outbox row in ascending id order.
   * Replay is why at-least-once holds across a reconnect.
   */
  attachSocket: (agentId: string, ws: WsLike) => void;
  /**
   * Feed one inbound message through the frame pipe: parse, dedup, write
   * durably, then emit the matching event. An unparseable or unknown frame is
   * logged and dropped rather than thrown, per the protocol's additive rule.
   */
  onSocketMessage: (agentId: string, data: string | ArrayBuffer) => void;
  /**
   * Release a closed socket. The agent is marked offline ONLY when the handle
   * being detached is still the current session — a superseded socket closing
   * late must not knock the live one offline. Pass the same handle you passed
   * to `attachSocket`.
   */
  detachSocket: (agentId: string, ws: WsLike) => void;
  /** The out-of-band byte channel for attachments and config artifacts. */
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
