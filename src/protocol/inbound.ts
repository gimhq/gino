import type { DecodedFrame } from "./envelope";
import type { FrameSender } from "./outbox";
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
// Unknown `type`s and unknown fields are ignored (protocol §4.2). Business
// `data` is validated here (still M2) against the `frames.ts` schemas right
// before it reaches a writer; the writers themselves stay transport-free.
import type { AppDatabase } from "@/db";
import type { Logger } from "@/shared/lib/logger";
import { touchLiveness } from "@/agents/registry";
import {
  getCron,
  getThreadTaskId,
  writeCronState,
  writeThreadMessage,
  writeThreadResult,
} from "@/msgstore";
import { CronStateData, HeartbeatData, ThreadMessageData, ThreadResultData } from "@/protocol/frames";
import { getTask } from "@/tasks/store";
import { decodeFrame } from "./envelope";
import { ackMessage, ackOutbox } from "./outbox";

export interface InboundDeps {
  db: AppDatabase;
  /** Live socket to ack back over (absent for the HTTP `/agent/output` fallback). */
  sender?: FrameSender;
  agentId: string;
  logger?: Logger;
  /**
   * M6 webhook egress trigger (HOST-M5M6-04): fired AFTER a `thread_message`
   * is durably stored, so the webhook pump pushes the new row promptly. Purely
   * a scheduling hint — delivery itself stays cursor-driven (egress re-reads
   * the store), so a missed/failed notification loses nothing.
   */
  onTranscriptWrite?: (taskId: string) => void;
}

/** Build the `ack{id}` for a frame that carried its own `outboxId`, else null. */
function ackFor(decoded: DecodedFrame): Record<string, unknown> | null {
  return decoded.outboxId != null ? { type: "ack", id: decoded.outboxId } : null;
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
 * Returns `true` to proceed with the write; `false` to DROP it (no write, no
 * ack — like the §4.2 "ignore" policy) after logging the denial. So a compromised
 * agent can neither inject into nor overwrite another agent's task state.
 */
function authorizeWrite(
  deps: InboundDeps,
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

  const task = getTask(deps.db, taskId);
  if (task == null || task.agentId !== deps.agentId) {
    return deny("task not placed on this agent");
  }
  if (binding.threadId != null) {
    const owner = getThreadTaskId(deps.db, binding.threadId);
    if (owner != null && owner !== taskId) {
      return deny("thread belongs to another task");
    }
  }
  if (binding.cronId != null) {
    const cron = getCron(deps.db, binding.cronId);
    if (cron != null && cron.taskId !== taskId) {
      return deny("cron belongs to another task");
    }
  }
  return true;
}

/**
 * Dispatch one decoded frame: write it to M5 (or apply the plumbing effect) and
 * return the ack frame the agent expects, or `null` for control frames that are
 * not acked. THROWS if a storage write fails — the caller must then NOT ack so
 * the agent replays. The store is idempotent, so a replay after a partial
 * failure re-runs cleanly.
 */
export function applyFrame(deps: InboundDeps, decoded: DecodedFrame): Record<string, unknown> | null {
  const { db, agentId } = deps;

  switch (decoded.type) {
    case "thread_message": {
      if (decoded.taskId == null || decoded.threadId == null) {
        return null;
      }
      if (!authorizeWrite(deps, decoded.taskId, { threadId: decoded.threadId })) {
        return null;
      }
      const data = ThreadMessageData.parse(decoded.data);
      writeThreadMessage(db, { taskId: decoded.taskId, threadId: decoded.threadId, data });
      deps.onTranscriptWrite?.(decoded.taskId);
      return ackFor(decoded);
    }

    case "thread_result": {
      if (decoded.taskId == null || decoded.threadId == null) {
        return null;
      }
      if (!authorizeWrite(deps, decoded.taskId, { threadId: decoded.threadId })) {
        return null;
      }
      const data = ThreadResultData.parse(decoded.data);
      // `source` is dispatch context the frame does not carry (protocol §4.1).
      // B1b default = 'chat'; M4/B3 supplies real source/cronId from dispatch.
      writeThreadResult(db, { taskId: decoded.taskId, threadId: decoded.threadId, data, source: "chat" });
      return ackFor(decoded);
    }

    case "cron_state": {
      if (decoded.taskId == null || decoded.cronId == null) {
        return null;
      }
      if (!authorizeWrite(deps, decoded.taskId, { cronId: decoded.cronId })) {
        return null;
      }
      const data = CronStateData.parse(decoded.data);
      writeCronState(db, { taskId: decoded.taskId, cronId: decoded.cronId, data });
      return ackFor(decoded);
    }

    case "heartbeat": {
      const data = HeartbeatData.optional().safeParse(decoded.data);
      touchLiveness(db, agentId, data.success ? data.data?.activeThreads : undefined);
      // Heartbeats are liveness, not durable work; ack only if one was requested.
      return ackFor(decoded);
    }

    case "msg_ack": {
      if (decoded.msgId != null) {
        ackMessage(db, agentId, decoded.msgId);
      }
      return null;
    }

    case "ack": {
      if (decoded.id != null) {
        ackOutbox(db, agentId, decoded.id);
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
 * A malformed/unknown frame is ignored; a storage failure is logged and NOT
 * acked (so the agent replays). Never throws to the socket loop.
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
 * HTTP `/agent/output` fallback (protocol §2): decode + dispatch a single frame
 * and RETURN the ack body instead of sending it over a socket. Throws on a
 * storage failure so the route surfaces a non-2xx and the agent retries; throws
 * a tagged error on a malformed frame.
 */
export function ingestHttpFrame(
  db: AppDatabase,
  agentId: string,
  raw: string | ArrayBuffer | Uint8Array,
  onTranscriptWrite?: (taskId: string) => void,
): Record<string, unknown> {
  const decoded = decodeFrame(raw);
  if (!decoded) {
    throw new Error("malformed frame");
  }
  const ack = applyFrame({ db, agentId, ...(onTranscriptWrite ? { onTranscriptWrite } : {}) }, decoded);
  return ack ?? { ok: true };
}
