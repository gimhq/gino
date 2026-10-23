import type { AgentTransport, WsLike } from "../host/ports/transport";
// M3 — SESSIONS (live connection registry; the host→agent push surface).
//
// An agent always DIALS OUT (protocol §2): the host never connects in. This
// registry maps `agentId → { ws }` for the currently-attached sockets and is
// the one place that turns a host→agent frame into bytes on the wire. Durability is NOT here — the durable host→agent queue is the M2 `outbox`
// (it persists before send and replays on reconnect). Sessions only knows
// "is this agent online, and how do I push a frame to it right now".
//
// Layering: sessions implements the `AgentTransport` port (docs/host-sdk-design
// §8.4) so the outbox can deliver/replay without importing transport details,
// and it never parses `data` — it ships an already-encoded frame object as JSON.

import { Buffer } from "node:buffer";

export type { WsLike };

/** Same connection: identical handle, or both wrap the same raw socket. */
function sameSocket(a: WsLike, b: WsLike): boolean {
  return a === b || (a.raw != null && a.raw === b.raw);
}

interface Session {
  agentId: string;
  ws: WsLike;
}

const WS_OPEN = 1;

/**
 * In-memory registry of attached agent sockets. One per `HostContext` (not per
 * process): the WS pipe + outbox share the context's instance. Tests can
 * construct an isolated `new SessionRegistry()`.
 */
export class SessionRegistry implements AgentTransport {
  private readonly sessions = new Map<string, Session>();

  /** Called whenever an agent's current socket is dropped or superseded. */
  onDrop?: (agentId: string) => void;

  /**
   * Attach (or replace) the socket for an agent. A reconnect supersedes any
   * stale socket: the previous one is closed so a single agent never has two
   * live host→agent pipes. The caller flips the agent `online` in the registry
   * and then replays the unacked outbox.
   */
  attach(agentId: string, ws: WsLike): void {
    const existing = this.sessions.get(agentId);
    if (existing && !sameSocket(existing.ws, ws)) {
      try {
        existing.ws.close(1012, "superseded by reconnect");
      }
      catch {
        // The stale socket may already be gone — detaching is best-effort.
      }
      this.onDrop?.(agentId);
    }
    this.sessions.set(agentId, { agentId, ws });
  }

  /**
   * Detach the agent's socket. When `ws` is given, only detaches if it is still
   * the current socket — so a late `close` from a superseded socket cannot evict
   * the live reconnect. Returns whether the CURRENT session was evicted, so the
   * caller can skip status bookkeeping for a superseded socket's close.
   */
  detach(agentId: string, ws?: WsLike): boolean {
    const existing = this.sessions.get(agentId);
    if (!existing) {
      return false;
    }
    if (ws && !sameSocket(existing.ws, ws)) {
      return false;
    }
    this.sessions.delete(agentId);
    this.onDrop?.(agentId);
    return true;
  }

  get(agentId: string): Session | undefined {
    return this.sessions.get(agentId);
  }

  isOnline(agentId: string): boolean {
    return this.sessions.has(agentId);
  }

  /**
   * Detach every session and best-effort `close(code, reason)` each socket
   * (host shutdown). A throwing socket is skipped, like `attach`. Returns the
   * number of sessions closed.
   */
  closeAll(code: number, reason: string): number {
    let closed = 0;
    for (const [agentId, session] of this.sessions) {
      this.sessions.delete(agentId);
      this.onDrop?.(agentId);
      closed += 1;
      try {
        session.ws.close(code, reason);
      }
      catch {
        // The socket may already be gone — closing is best-effort.
      }
    }
    return closed;
  }

  /**
   * Detach the agent's socket and best-effort `close(code, reason)` it
   * (supersede / remove / sweep). A throwing socket is swallowed, like
   * `attach`. Returns whether a session existed.
   */
  closeAgent(agentId: string, code: number, reason: string): boolean {
    const session = this.sessions.get(agentId);
    if (!session) {
      return false;
    }
    this.sessions.delete(agentId);
    this.onDrop?.(agentId);
    try {
      session.ws.close(code, reason);
    }
    catch {
      // The socket may already be gone — closing is best-effort.
    }
    return true;
  }

  /**
   * Push one already-encoded frame to the agent as JSON. Returns `false` when
   * the agent is offline or the socket throws — the frame stays in the outbox
   * (unacked) and replays on the next connect, so a failed push is never lost.
   */
  send(agentId: string, frame: Record<string, unknown>): boolean {
    const session = this.sessions.get(agentId);
    if (!session) {
      return false;
    }
    if (session.ws.readyState != null && session.ws.readyState !== WS_OPEN) {
      return false;
    }
    try {
      const text = JSON.stringify(frame);
      if (typeof frame.type === "string" && frame.type.startsWith("pty_") && frame.type !== "pty_close") {
        const raw = (session.ws.raw ?? session.ws) as { bufferedAmount?: number; getBufferedAmount?: () => number } | undefined;
        const queued = raw?.getBufferedAmount?.() ?? raw?.bufferedAmount ?? 0;
        if (queued + Buffer.byteLength(text) > 256 * 1024)
          return false;
      }
      session.ws.send(text);
      return true;
    }
    catch {
      return false;
    }
  }
}
