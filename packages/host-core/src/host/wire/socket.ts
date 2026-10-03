import type { InboundDeps } from "../../protocol/inbound";
import type { WsLike } from "../ports/transport";
import type { WireContext } from "./index";
import { setAgentStatus, touchLiveness } from "../../agents/registry";
import { handleInboundFrame } from "../../protocol/inbound";
import { replayUnacked } from "../../protocol/outbox";

// WS session + frame pipe (M2/M3). The lifecycle ordering below is
// protocol-visible — the adapter must call
// these three in its socket callbacks and never re-implement them.

/** Host events (M6: a stored thread_message triggers the webhook pump via `transcript.message`). */
function inboundDeps(ctx: WireContext, agentId: string): InboundDeps {
  return { storage: ctx.storage, sender: ctx.sessions, agentId, logger: ctx.logger, metrics: ctx.metrics, events: ctx.events };
}

export function attachSocket(ctx: WireContext, agentId: string, ws: WsLike): void {
  // Attach BEFORE replay so the replayed frames find the live socket.
  ctx.sessions.attach(agentId, ws);
  setAgentStatus(ctx.storage, agentId, "online");
  ctx.events.emit("agent.online", { agentId });
  // Stamp `last_seen` at attach so a fresh
  // connection starts inside the sweep's grace window (never insta-swept
  // on a stale or NULL `last_seen` before its first heartbeat).
  touchLiveness(ctx.storage, agentId);
  const replayed = replayUnacked(ctx.storage, ctx.sessions, agentId, { logger: ctx.logger });
  ctx.logger.info({ agentId, replayed }, "agent ws attached");
}

export function onSocketMessage(ctx: WireContext, agentId: string, data: string | ArrayBuffer): void {
  handleInboundFrame(inboundDeps(ctx, agentId), data);
}

export function detachSocket(ctx: WireContext, agentId: string, ws: WsLike): void {
  // Only the CURRENT socket's close marks the
  // agent offline — a superseded socket's late close (dirty reconnect)
  // must not clobber the live session's `online` status.
  const evicted = ctx.sessions.detach(agentId, ws);
  if (evicted) {
    setAgentStatus(ctx.storage, agentId, "offline");
    ctx.events.emit("agent.offline", { agentId, reason: "close" });
  }
  ctx.logger.info({ agentId, evicted }, "agent ws detached");
}
