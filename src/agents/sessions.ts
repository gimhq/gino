// M3 — SESSIONS (live connection registry; the host→agent push surface).
//
// An agent always DIALS OUT (protocol §2): the host never connects in. This
// registry maps `agentId → { ws, connectTokenHash }` for the currently-attached
// sockets and is the one place that turns a host→agent frame into bytes on the
// wire. Durability is NOT here — the durable host→agent queue is the M2 `outbox`
// (it persists before send and replays on reconnect). Sessions only knows
// "is this agent online, and how do I push a frame to it right now".
//
// Layering: sessions implements the M2 `FrameSender` seam (`isOnline` + `send`)
// so the outbox can deliver/replay without importing transport details, and it
// never parses `data` — it ships an already-encoded frame object as JSON.

/**
 * The minimal socket surface sessions needs — satisfied by the hono/bun
 * `WSContext`. Kept structural so unit tests can attach a fake socket.
 */
export interface WsLike {
  send: (data: string) => void;
  readyState?: number;
  close: (code?: number, reason?: string) => void;
}

interface Session {
  agentId: string;
  ws: WsLike;
  connectTokenHash: string;
}

const WS_OPEN = 1;

/**
 * In-memory registry of attached agent sockets. One per host process; the
 * exported `sessions` singleton is what the WS pipe + outbox share. Tests can
 * construct an isolated `new SessionRegistry()`.
 */
export class SessionRegistry {
  private readonly sessions = new Map<string, Session>();

  /**
   * Attach (or replace) the socket for an agent. A reconnect supersedes any
   * stale socket: the previous one is closed so a single agent never has two
   * live host→agent pipes. The caller flips the agent `online` in the registry
   * and then replays the unacked outbox.
   */
  attach(agentId: string, ws: WsLike, connectTokenHash: string): void {
    const existing = this.sessions.get(agentId);
    if (existing && existing.ws !== ws) {
      try {
        existing.ws.close(1012, "superseded by reconnect");
      }
      catch {
        // The stale socket may already be gone — detaching is best-effort.
      }
    }
    this.sessions.set(agentId, { agentId, ws, connectTokenHash });
  }

  /**
   * Detach the agent's socket. When `ws` is given, only detaches if it is still
   * the current socket — so a late `close` from a superseded socket cannot evict
   * the live reconnect.
   */
  detach(agentId: string, ws?: WsLike): void {
    const existing = this.sessions.get(agentId);
    if (!existing) {
      return;
    }
    if (ws && existing.ws !== ws) {
      return;
    }
    this.sessions.delete(agentId);
  }

  get(agentId: string): Session | undefined {
    return this.sessions.get(agentId);
  }

  isOnline(agentId: string): boolean {
    return this.sessions.has(agentId);
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
      session.ws.send(JSON.stringify(frame));
      return true;
    }
    catch {
      return false;
    }
  }
}

/** Process-wide session registry shared by the WS pipe (M3) and the outbox (M2). */
export const sessions = new SessionRegistry();
