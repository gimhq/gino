// HOST SDK PORT — AGENT TRANSPORT (docs/host-sdk-design.md §8.4).
//
// The host→agent PUSH surface. Durability, dedup and replay live in core (the
// M2 `outbox` rows + `msg_ack`), so the transport only needs a notion of
// connection plus a best-effort send — which is what makes a non-WebSocket
// link a legitimate adapter. The inbound direction is core's wire (the adapter
// calls attach/detach and feeds messages in); the socket handle only has to
// satisfy `WsLike`.

/**
 * The minimal socket surface the host needs — satisfied by the hono/bun
 * `WSContext`. Kept structural so unit tests can attach a fake socket.
 */
export interface WsLike {
  send: (data: string) => void;
  readyState?: number;
  close: (code?: number, reason?: string) => void;
  /**
   * The underlying transport socket, when the handle is a wrapper. The hono/bun
   * adapter constructs a FRESH `WSContext` per event around the same raw Bun
   * socket, so current-socket checks must compare `raw` — wrapper identity
   * differs between `onOpen` and `onClose` for the same connection.
   */
  raw?: unknown;
}

/** §8.4 — the host→agent push surface (implemented by M3 `SessionRegistry`). */
export interface AgentTransport {
  isOnline: (agentId: string) => boolean;
  /**
   * Best-effort push of one already-encoded frame. `false` → the frame stays
   * unacked in the outbox and replays on the next connect.
   */
  send: (agentId: string, frame: Record<string, unknown>) => boolean;
  /**
   * Supersede / remove / sweep: detach the agent's current socket and
   * best-effort close it. Returns whether a session existed.
   */
  closeAgent: (agentId: string, code: number, reason: string) => boolean;
}
