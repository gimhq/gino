// M2 — OUTBOX (the SOLE host→agent path; at-least-once delivery).
//
// Every host→agent frame is PERSISTED (the `outbox` table) BEFORE it is sent
// (protocol §4.2). If the agent is online the frame is also pushed immediately;
// if not — or if the push is lost — it stays unacked and is REPLAYED in id
// order on the next connect. Every frame carries `msgId` — the row's ULID —
// and the agent acks durable storage with `msg_ack{msgId}`, which stops replay.
//
// Layering: the outbox talks to the live socket only through the abstract
// `AgentTransport` port (sessions implements it), and rebuilds the wire frame
// via the M2 `encodeOutbox` codec — it never hand-rolls the envelope.
import type { OutboxRow, Storage } from "../host/ports/storage";
import type { AgentTransport } from "../host/ports/transport";
import type { Logger } from "../logger";
import { ulid } from "../platform";
import { encodeOutbox } from "./envelope";

export interface EnqueueInput {
  agentId: string;
  type: string;
  taskId?: string;
  threadId?: string;
  cronId?: string;
  /** Business payload (envelope ids never live here — protocol §4.1). */
  data?: Record<string, unknown>;
}

/** An enqueued outbox row; a `message` row also surfaces its `msgId` (= row id). */
export type EnqueuedOutbox = OutboxRow & { msgId?: string };

/**
 * Persist a host→agent frame and, if the agent is online, send it. The row is
 * written FIRST (durability before delivery); the live push is best-effort —
 * a miss is recovered by `replayUnacked` on reconnect.
 */
export function enqueue(storage: Storage, sender: AgentTransport, input: EnqueueInput): EnqueuedOutbox {
  const inserted = storage.outbox.append({
    id: ulid(),
    agentId: input.agentId,
    type: input.type,
    taskId: input.taskId ?? null,
    threadId: input.threadId ?? null,
    cronId: input.cronId ?? null,
    dataJson: input.data ?? {},
  });

  const row = input.type === "message" ? { ...inserted, msgId: inserted.id } : inserted;

  if (sender.isOnline(input.agentId)) {
    sender.send(input.agentId, encodeOutbox(row));
  }
  return row;
}

/**
 * Persist + push a LIVE-ONLY control frame (at-most-once). The row rides the
 * normal outbox path (uniform encode + audit trail) but is stamped acked
 * immediately, so it is NEVER replayed on reconnect: a cancel signal is only
 * meaningful against the turn that is running right now — replaying a stale
 * one would kill an arbitrary future turn. Delivery is the live
 * push or nothing.
 */
export function enqueueLiveOnly(storage: Storage, sender: AgentTransport, input: EnqueueInput): OutboxRow {
  const row = enqueue(storage, sender, input);
  const ackedAt = Date.now();
  storage.outbox.markAcked(input.agentId, row.id, ackedAt);
  return { ...row, ackedAt };
}

/** Agent durably stored a host→agent frame (`msg_ack{msgId}`) → stop replaying it, scoped to the agent's own rows. */
export function ackMessage(storage: Storage, agentId: string, msgId: string): void {
  storage.outbox.markAcked(agentId, msgId, Date.now());
}

/**
 * TTL for durable `close` control frames — 15 minutes. A `close`
 * queued by a stop/restart fallback while the agent was offline expresses a
 * NOW intent, not a standing order: replaying it weeks later would kill a
 * deliberate later start. Past the TTL the frame is skipped (and retired) on
 * replay instead of delivered.
 */
export const CLOSE_FRAME_TTL_MS = 15 * 60 * 1000;

export interface ReplayOptions {
  /** TTL for `close` frames in ms. Defaults to `CLOSE_FRAME_TTL_MS`. */
  readonly closeTtlMs?: number;
  /** Clock seam (tests inject a fake clock). Defaults to `Date.now`. */
  readonly now?: () => number;
  readonly logger?: Logger;
}

/**
 * Replay every unacked frame for an agent, in id order, on (re)connect. The
 * agent dedupes by id, so re-sending an already-processed-but-not-yet-acked
 * frame is safe. Returns the number of frames pushed.
 *
 * A `close` frame older than its TTL is STALE — it is retired
 * (marked acked so it never rescans) and logged instead of sent, so a stop
 * issued while the agent was offline cannot kill a deliberate later start.
 */
export function replayUnacked(storage: Storage, sender: AgentTransport, agentId: string, opts: ReplayOptions = {}): number {
  const rows = storage.outbox.listUnacked(agentId);

  const now = opts.now?.() ?? Date.now();
  const closeCutoff = now - (opts.closeTtlMs ?? CLOSE_FRAME_TTL_MS);

  let sent = 0;
  for (const row of rows) {
    if (row.type === "close" && row.createdAt < closeCutoff) {
      storage.outbox.markAcked(agentId, row.id, now);
      opts.logger?.warn(
        { agentId, outboxId: row.id, ageMs: now - row.createdAt },
        "stale close frame skipped on replay (past TTL) — retired without delivery",
      );
      continue;
    }
    if (sender.send(agentId, encodeOutbox(row))) {
      sent += 1;
    }
  }
  return sent;
}

/**
 * Purge an agent's UNACKED outbox rows. Called on remove: the
 * agent's connect token is revoked, so it can never reconnect to replay or ack
 * — the rows are undeliverable by construction and would otherwise be immortal
 * (retention GC only touches acked rows). Returns the number of rows deleted.
 */
export function purgeAgentOutbox(storage: Storage, agentId: string): number {
  return storage.outbox.purge(agentId);
}

/** Default retention window for ACKED outbox rows (ms) — 7 days. */
export const OUTBOX_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

/** Default cadence of the periodic retention sweep (ms) — hourly. */
export const OUTBOX_GC_INTERVAL_MS = 60 * 60 * 1000;

export interface OutboxGcOptions {
  /** Retention window for acked rows in ms. Defaults to `OUTBOX_RETENTION_MS`. */
  readonly retentionMs?: number;
  /** Clock seam (tests inject a fake clock). Defaults to `Date.now`. */
  readonly now?: () => number;
}

/**
 * Retention GC: delete ACKED rows whose `acked_at` is older than the retention
 * window. Unacked rows are NEVER deleted — they are still owed to the agent
 * (at-least-once, §4.2) and stay until acked, however old. Seed state is not
 * inferred from outbox history (it lives on `tasks.seeded_at`), so dropping
 * acked frames is safe. Returns the number of rows deleted.
 */
export function gcOutbox(storage: Storage, opts: OutboxGcOptions = {}): number {
  const now = opts.now?.() ?? Date.now();
  const cutoff = now - (opts.retentionMs ?? OUTBOX_RETENTION_MS);
  return storage.outbox.gc(cutoff);
}

export interface OutboxGcDeps extends OutboxGcOptions {
  readonly storage: Storage;
  readonly logger?: Logger;
  /** Sweep cadence in ms. Defaults to `OUTBOX_GC_INTERVAL_MS`. */
  readonly intervalMs?: number;
}

export interface OutboxGcHandle {
  /** Stop the periodic GC (call on shutdown). Idempotent. */
  readonly stop: () => void;
  /** Run a GC pass immediately; returns the number of rows deleted. */
  readonly gc: () => number;
}

/**
 * Start the periodic outbox retention GC. The timer is `unref`'d so it never
 * keeps the process alive on its own; `stop()` clears it. A failing pass is
 * logged and swallowed so the loop survives a transient DB error.
 */
export function startOutboxGc(deps: OutboxGcDeps): OutboxGcHandle {
  const tick = () => {
    try {
      const deleted = gcOutbox(deps.storage, deps);
      if (deleted > 0) {
        deps.logger?.info({ deleted }, "outbox gc: deleted acked rows past retention");
      }
    }
    catch (err) {
      deps.logger?.error({ err }, "outbox gc failed");
    }
  };

  const timer = setInterval(tick, deps.intervalMs ?? OUTBOX_GC_INTERVAL_MS);
  timer.unref?.();

  return {
    stop: () => clearInterval(timer),
    gc: () => gcOutbox(deps.storage, deps),
  };
}
