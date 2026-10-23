// M2 — INBOUND (frame → demux by threadId → dispatch by type → M5 writer → ack).
//
// The receive half of the only wire-aware module. Each agent→host frame is:
//   1. decoded to its routing envelope (data stays opaque) — `decodeFrame`;
//   2. demuxed by `threadId` (each thread is an independent stream, protocol
//      §6) — realised by the M5 writers, which key + dedupe on the thread
//      (`(thread_id, seq)`), so an at-least-once REPLAY is a safe no-op;
//   3. dispatched by `type` to the matching M5 writer (storage) or plumbing
//      handler (`ack` → outbox, `msg_ack` → message delivery, `heartbeat` →
//      liveness);
//   4. acked — but ONLY after the write succeeds, so a failed store is not
//      acked and the agent replays it.
//
// Failure policy — the ack decision separates two classes:
//   • PERMANENT (schema-invalid `data`, missing correlation ids, authorize
//     denial): replaying can never succeed, so the frame is POISON — logged
//     loudly, counted (`inbound_poison_frames_total`), and ACKED-AND-DROPPED
//     so the agent stops retransmitting it forever.
//   • TRANSIENT (a storage write throws, e.g. SQLITE_BUSY): NOT acked — the
//     agent replays and the idempotent store makes the replay a safe no-op.
//
// Unknown `type`s and unknown fields are ignored (protocol §4.2). Business
// `data` is validated here (still M2) against the `frames.ts` schemas right
// before it reaches a writer; the writers themselves stay transport-free.
import type { RemoteOps } from "../agents/remote";
import type { HostEvents } from "../host/events";
import type { Metrics } from "../host/metrics";
import type { Storage } from "../host/ports/storage";
import type { AgentTransport } from "../host/ports/transport";
import type { Logger } from "../logger";
import type { DecodedFrame } from "./envelope";
import { touchLiveness } from "../agents/registry";
import {
  checkTranscriptCompleteness,
  getCron,
  getThreadTaskId,
  writeAttachmentDescriptors,
  writeCronState,
  writeThreadMessage,
  writeThreadResult,
} from "../msgstore";
import { decodeFrame } from "./envelope";
import { CronStateData, HeartbeatData, RECOGNIZED_MESSAGE_KINDS, ThreadMessageData, ThreadResultData } from "./frames";
import { ackMessage, ackOutbox } from "./outbox";

export interface InboundDeps {
  storage: Storage;
  /** Live socket to ack back over (absent for the HTTP `/agent/output` fallback). */
  sender?: AgentTransport;
  agentId: string;
  logger?: Logger;
  /**
   * Host event emitter: each durable write (and each outbox ack) is announced
   * AFTER it lands — e.g. `transcript.message` is what the webhook pump
   * subscribes to. Purely a scheduling hint — delivery itself
   * stays cursor-driven (consumers re-read the store), so a missed/failed
   * notification loses nothing.
   */
  events?: HostEvents;
  /** Metrics sink (the host context's); absent → counters are not recorded. */
  metrics?: Metrics;
  /** Pending engine-free requests; absent → `fs_response` frames are ignored. */
  remote?: RemoteOps;
}

/** Build the `ack{id}` for a frame that carried its own `outboxId`, else null. */
function ackFor(decoded: DecodedFrame): Record<string, unknown> | null {
  return decoded.outboxId != null ? { type: "ack", id: decoded.outboxId } : null;
}

/**
 * PERMANENT-failure sink: a frame that can never be processed —
 * schema-invalid `data`, missing correlation ids, or an authorize denial — is
 * acked-and-dropped so the agent's at-least-once outbox stops replaying it
 * forever. Logged at error level + counted so poison is loud, never silent.
 */
function poisonDrop(deps: InboundDeps, decoded: DecodedFrame, reason: string, detail?: unknown): Record<string, unknown> | null {
  deps.logger?.error(
    { agentId: deps.agentId, type: decoded.type, taskId: decoded.taskId, threadId: decoded.threadId, cronId: decoded.cronId, outboxId: decoded.outboxId, reason, detail },
    "inbound frame PERMANENTLY unprocessable — acked and dropped (poison policy)",
  );
  deps.metrics?.counterAdd("inbound_poison_frames_total", "Permanently unprocessable inbound frames acked and dropped", 1, { type: decoded.type, reason });
  return ackFor(decoded);
}

/**
 * Authorize a STORE-WRITE frame (`thread_message` / `thread_result` /
 * `cron_state`) against the connect-token-resolved agent identity — the IDOR
 * gate the byte channel already enforces (B6s), applied to the M2/M5 write path.
 *
 * The agent that opened this stream is `deps.agentId` (set by
 * `connectTokenAuth` / `verifyConnectToken` from the per-agent connect token —
 * NEVER a frame field). The frame's `taskId` / `threadId` / `cronId` are, by
 * contrast, fully caller-supplied envelope ids. A write is authorized only when:
 *   1. the frame's `taskId` is a task PLACED on this agent (`tasks.agent_id`, the
 *      set-once placement) — so an agent can only write its OWN tasks; and
 *   2. the supplied `threadId` / `cronId` is not already bound to a DIFFERENT
 *      task — a thread/cron belongs to exactly one task, so this stops a placed
 *      agent from poisoning another task's transcript via a forged `(thread,seq)`
 *      or clobbering another task's cron mirror via a forged cron id.
 *
 * Returns `true` to proceed with the write; `false` to DROP it (no write) after
 * logging the denial. A denial is PERMANENT — replaying can never make it pass —
 * so the caller acks-and-drops it under the poison policy: the
 * frame is still never written, and a compromised agent can neither inject into
 * nor overwrite another agent's task state.
 */
function authorizeWrite(
  deps: InboundDeps,
  storage: Storage,
  taskId: string,
  binding: { threadId?: string; cronId?: string },
): boolean {
  const deny = (reason: string): false => {
    deps.logger?.warn(
      { agentId: deps.agentId, taskId, ...binding, reason },
      "inbound write frame DENIED (cross-agent/cross-task write blocked)",
    );
    return false;
  };

  const task = deps.storage.tasks.get(taskId);
  if (task == null || task.agentId !== deps.agentId) {
    return deny("task not placed on this agent");
  }
  if (binding.threadId != null) {
    const owner = getThreadTaskId(storage, binding.threadId);
    if (owner != null && owner !== taskId) {
      return deny("thread belongs to another task");
    }
  }
  if (binding.cronId != null) {
    const cron = getCron(storage, binding.cronId);
    if (cron != null && cron.taskId !== taskId) {
      return deny("cron belongs to another task");
    }
  }
  return true;
}

/**
 * Derive the dispatch context of a thread from its id.
 * The `thread_result` frame does not carry `source`/`cronId`
 * (protocol §4.1), but the agent mints cron-fired thread ids deterministically
 * (agent scheduler, agent-architecture §4): `<cronId>@<scheduled_epoch_millis>`
 * for isolated fires and `<cronId>@shared` for shared continuity. When the
 * prefix names a known cron of the SAME task, the run is cron-originated;
 * anything else — no `@`, a non-cron suffix, an unknown prefix, or a cron
 * belonging to another task — stays `chat` with no cron link.
 */
function deriveDispatchContext(storage: Storage, taskId: string, threadId: string): { source: string; cronId?: string } {
  const at = threadId.lastIndexOf("@");
  if (at <= 0) {
    return { source: "chat" };
  }
  const suffix = threadId.slice(at + 1);
  if (suffix !== "shared" && !/^\d+$/.test(suffix)) {
    return { source: "chat" };
  }
  const cronId = threadId.slice(0, at);
  const cron = getCron(storage, cronId);
  if (cron == null || cron.taskId !== taskId) {
    return { source: "chat" };
  }
  return { source: "cron", cronId };
}

/**
 * Dispatch one decoded frame: write it to M5 (or apply the plumbing effect) and
 * return the ack frame the agent expects, or `null` for control frames that are
 * not acked. THROWS only if a storage write fails (TRANSIENT) — the caller must
 * then NOT ack so the agent replays; the store is idempotent, so a replay after
 * a partial failure re-runs cleanly. PERMANENT failures never throw: they are
 * acked-and-dropped inside (`poisonDrop`).
 */
export function applyFrame(deps: InboundDeps, decoded: DecodedFrame): Record<string, unknown> | null {
  const { storage, agentId } = deps;

  switch (decoded.type) {
    case "thread_message": {
      if (decoded.taskId == null || decoded.threadId == null) {
        return poisonDrop(deps, decoded, "missing_ids");
      }
      if (!authorizeWrite(deps, storage, decoded.taskId, { threadId: decoded.threadId })) {
        return poisonDrop(deps, decoded, "authorize_denied");
      }
      const parsed = ThreadMessageData.safeParse(decoded.data);
      if (!parsed.success) {
        return poisonDrop(deps, decoded, "schema_invalid", parsed.error.issues);
      }
      const data = parsed.data;
      // `kind` is an open string (additive protocol, §1): an
      // unrecognized value is stored opaquely, never refused, but flagged so
      // version drift is visible to operators.
      if (!RECOGNIZED_MESSAGE_KINDS.has(data.kind)) {
        deps.logger?.warn({ agentId, taskId: decoded.taskId, threadId: decoded.threadId, kind: data.kind }, "thread_message with unrecognized kind — stored opaquely");
        deps.metrics?.counterAdd("inbound_unrecognized_kinds_total", "thread_message frames whose kind is outside the recognized set", 1, { kind: data.kind });
      }
      writeThreadMessage(storage, { taskId: decoded.taskId, threadId: decoded.threadId, data });
      deps.events?.emit("transcript.message", { agentId, taskId: decoded.taskId, threadId: decoded.threadId, seq: data.seq, data });
      return ackFor(decoded);
    }

    case "thread_result": {
      if (decoded.taskId == null || decoded.threadId == null) {
        return poisonDrop(deps, decoded, "missing_ids");
      }
      if (!authorizeWrite(deps, storage, decoded.taskId, { threadId: decoded.threadId })) {
        return poisonDrop(deps, decoded, "authorize_denied");
      }
      const parsed = ThreadResultData.safeParse(decoded.data);
      if (!parsed.success) {
        return poisonDrop(deps, decoded, "schema_invalid", parsed.error.issues);
      }
      const data = parsed.data;
      // `source`/`cronId` are dispatch context the frame does not carry
      // (protocol §4.1) — derived here from the cron-minted thread-id shape.
      const dispatch = deriveDispatchContext(storage, decoded.taskId, decoded.threadId);
      writeThreadResult(storage, { taskId: decoded.taskId, threadId: decoded.threadId, data, ...dispatch });
      // Register the run's outbound attachment descriptors (protocol §5 /
      // agent-architecture §9: "descriptors returned on thread_result"). The
      // agent's byte upload carries no task/thread context, so THIS is where
      // the descriptor gains its task/thread binding (idempotent on `id`).
      if (data.attachments != null && data.attachments.length > 0) {
        writeAttachmentDescriptors(storage, { taskId: decoded.taskId, threadId: decoded.threadId, descriptors: data.attachments });
      }
      deps.events?.emit("thread.result", { agentId, taskId: decoded.taskId, threadId: decoded.threadId, ...dispatch, data });
      // Verify the transcript is exactly `1..=final_seq`
      // (protocol §6). Diagnostic only — a gap is flagged, never blocks the ack.
      const completeness = checkTranscriptCompleteness(storage, decoded.threadId, data.final_seq);
      if (!completeness.complete) {
        deps.logger?.warn(
          { agentId, taskId: decoded.taskId, threadId: decoded.threadId, ...completeness },
          "thread_result transcript gap — stored messages do not cover 1..=final_seq",
        );
        deps.metrics?.counterAdd("inbound_transcript_gaps_total", "thread_result ingests whose stored transcript does not cover 1..=final_seq", 1);
      }
      return ackFor(decoded);
    }

    case "cron_state": {
      if (decoded.taskId == null || decoded.cronId == null) {
        return poisonDrop(deps, decoded, "missing_ids");
      }
      if (!authorizeWrite(deps, storage, decoded.taskId, { cronId: decoded.cronId })) {
        return poisonDrop(deps, decoded, "authorize_denied");
      }
      const parsed = CronStateData.safeParse(decoded.data);
      if (!parsed.success) {
        return poisonDrop(deps, decoded, "schema_invalid", parsed.error.issues);
      }
      writeCronState(storage, { taskId: decoded.taskId, cronId: decoded.cronId, data: parsed.data });
      deps.events?.emit("cron.state", { agentId, taskId: decoded.taskId, cronId: decoded.cronId, data: parsed.data });
      return ackFor(decoded);
    }

    case "heartbeat": {
      const data = HeartbeatData.optional().safeParse(decoded.data);
      touchLiveness(storage, agentId, data.success ? data.data?.activeThreads : undefined);
      // Heartbeats are liveness, not durable work; ack only if one was requested.
      return ackFor(decoded);
    }

    case "msg_ack": {
      if (decoded.msgId != null) {
        ackMessage(storage, agentId, decoded.msgId);
        deps.events?.emit("outbox.acked", { agentId, id: decoded.msgId, type: "msg_ack" });
      }
      return null;
    }

    case "ack": {
      if (decoded.id != null) {
        ackOutbox(storage, agentId, decoded.id);
        deps.events?.emit("outbox.acked", { agentId, id: decoded.id, type: "ack" });
      }
      return null;
    }

    case "pty_ready":
    case "pty_output":
    case "pty_exit": {
      if (decoded.ptyId != null)
        deps.remote?.receivePty(agentId, decoded.type, decoded.ptyId, decoded.data);
      return null;
    }

    case "fs_response": {
      // Live-only: correlated by `reqId`, never acked.
      if (decoded.reqId != null) {
        deps.remote?.resolve(agentId, decoded.reqId, decoded.data);
      }
      return null;
    }

    default:
      // Unknown type — ignore (protocol §4.2).
      return null;
  }
}

/**
 * WS entry point: decode → dispatch → send the ack back over the live socket.
 * An undecodable/unknown frame is ignored; a TRANSIENT storage failure is
 * logged and NOT acked (so the agent replays); a PERMANENT failure is
 * acked-and-dropped by `applyFrame` (poison policy). Never
 * throws to the socket loop.
 */
export function handleInboundFrame(deps: InboundDeps, raw: string | ArrayBuffer | Uint8Array): void {
  const decoded = decodeFrame(raw);
  if (!decoded) {
    return;
  }

  let ack: Record<string, unknown> | null;
  try {
    ack = applyFrame(deps, decoded);
  }
  catch (err) {
    deps.logger?.warn({ err, type: decoded.type, agentId: deps.agentId }, "inbound frame dropped (not acked → agent will replay)");
    return;
  }

  if (ack && deps.sender) {
    deps.sender.send(deps.agentId, ack);
  }
}

/**
 * The one error class the `/agent/output` route maps to `400 BAD_FRAME`:
 * the request body is not a decodable frame envelope, so a
 * retry can never succeed and a conforming agent must drop it. Every OTHER
 * throw out of `ingestHttpFrame` is a transient storage failure and must
 * surface as a 5xx so the agent retries.
 */
export class MalformedFrameError extends Error {
  constructor() {
    super("malformed frame");
    this.name = "MalformedFrameError";
  }
}

/**
 * HTTP `/agent/output` fallback (protocol §2): decode + dispatch a single frame
 * and RETURN the ack body instead of sending it over a socket. Throws
 * `MalformedFrameError` on an undecodable frame (→ 400, do not retry) and lets
 * a TRANSIENT storage failure propagate untagged (→ 5xx, the agent retries).
 * PERMANENT failures return the poison ack (2xx) like the WS path.
 * The deps carry the optional `events` emitter (the webhook pump's
 * `transcript.message` trigger) exactly like the WS path.
 */
export function ingestHttpFrame(deps: Omit<InboundDeps, "sender">, raw: string | ArrayBuffer | Uint8Array): Record<string, unknown> {
  const decoded = decodeFrame(raw);
  if (!decoded) {
    throw new MalformedFrameError();
  }
  const ack = applyFrame(deps, decoded);
  return ack ?? { ok: true };
}
