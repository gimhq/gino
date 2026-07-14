// M2 — OUTBOX (the SOLE host→agent path; at-least-once delivery).
//
// Every host→agent frame is PERSISTED (the `outbox` table) BEFORE it is sent
// (protocol §4.2). If the agent is online the frame is also pushed immediately;
// if not — or if the push is lost — it stays unacked and is REPLAYED in id
// order on the next connect. The agent acks delivery and the host stops
// replaying:
//   • generic host→agent frames carry `outboxId` and are acked by `ack{id}`;
//   • the `message` frame carries `msgId` and is acked by `msg_ack{msgId}` so
//     M4 (B3) can advance its delivery cursor only after the agent has DURABLY
//     stored the input. The message's `msgId` IS the row id — derived at encode
//     time, never stored (there is no `msg_id` column) — so a `message` row is
//     atomically born with its delivery id and the ack just marks the row
//     delivered (the cursor advance is a B3 seam).
//
// Layering: the outbox talks to the live socket only through the abstract
// `FrameSender` (sessions implements it), and rebuilds the wire frame via the
// M2 `encodeOutbox` codec — it never hand-rolls the envelope.
import type { AppDatabase } from "@/db";
import type { Outbox } from "@/db/schema";
import type { Logger } from "@/shared/lib/logger";
import { and, asc, eq, isNotNull, isNull, lt, ne } from "drizzle-orm";
import { outbox } from "@/db/schema";
import { encodeOutbox } from "./envelope";

/** The push surface the outbox needs (implemented by M3 `SessionRegistry`). */
export interface FrameSender {
  isOnline: (agentId: string) => boolean;
  send: (agentId: string, frame: Record<string, unknown>) => boolean;
}

export interface EnqueueInput {
  agentId: string;
  type: string;
  taskId?: string;
  threadId?: string;
  cronId?: string;
  /** Business payload (envelope ids never live here — protocol §4.1). */
  data?: Record<string, unknown>;
}

/**
 * An enqueued outbox row. `msgId` is NOT a column — for a `message` row it is
 * the derived delivery id (= row id), attached so callers can surface it.
 */
export type EnqueuedOutbox = Outbox & { msgId?: number };

/**
 * Persist a host→agent frame and, if the agent is online, send it. The row is
 * written FIRST (durability before delivery); the live push is best-effort —
 * a miss is recovered by `replayUnacked` on reconnect. A `message` frame's
 * delivery id (`msgId`) IS the row id: it exists the instant the single INSERT
 * commits, so there is no crash window in which a durable `message` row lacks
 * an ackable id. The returned row carries the derived `msgId` for callers that
 * surface it.
 */
export function enqueue(db: AppDatabase, sender: FrameSender, input: EnqueueInput): EnqueuedOutbox {
  const inserted = db
    .insert(outbox)
    .values({
      agentId: input.agentId,
      type: input.type,
      taskId: input.taskId ?? null,
      threadId: input.threadId ?? null,
      cronId: input.cronId ?? null,
      dataJson: input.data ?? {},
    })
    .returning()
    .get();

  const row = input.type === "message" ? { ...inserted, msgId: inserted.id } : inserted;

  if (sender.isOnline(input.agentId)) {
    sender.send(input.agentId, encodeOutbox(row));
  }
  return row;
}

/**
 * Agent acked a generic host→agent frame (`ack{id}`) → stop replaying it.
 * Scoped to the authenticated agent's own rows (outbox ids are a global
 * autoincrement, so a foreign id is trivially guessable), and to non-`message`
 * rows: a `message` is delivered only by the durable-storage `msg_ack` (§4.2).
 */
export function ackOutbox(db: AppDatabase, agentId: string, id: number): void {
  db
    .update(outbox)
    .set({ ackedAt: Date.now() })
    .where(and(eq(outbox.id, id), eq(outbox.agentId, agentId), ne(outbox.type, "message")))
    .run();
}

/**
 * Agent durably stored a `message` (`msg_ack{msgId}`) → mark it delivered.
 * A message's `msgId` is its row id, so the match keys on `id` +
 * `type = 'message'`, scoped to the authenticated agent's own rows.
 * TODO(M4/B3): advance the per-task delivery cursor here; for B1b marking the
 * row acked is enough to stop replay.
 */
export function ackMessage(db: AppDatabase, agentId: string, msgId: number): void {
  db
    .update(outbox)
    .set({ ackedAt: Date.now() })
    .where(and(eq(outbox.id, msgId), eq(outbox.agentId, agentId), eq(outbox.type, "message")))
    .run();
}

/**
 * Replay every unacked frame for an agent, in id order, on (re)connect. The
 * agent dedupes by id, so re-sending an already-processed-but-not-yet-acked
 * frame is safe. Returns the number of frames pushed.
 */
export function replayUnacked(db: AppDatabase, sender: FrameSender, agentId: string): number {
  const rows = db
    .select()
    .from(outbox)
    .where(and(eq(outbox.agentId, agentId), isNull(outbox.ackedAt)))
    .orderBy(asc(outbox.id))
    .all();

  let sent = 0;
  for (const row of rows) {
    if (sender.send(agentId, encodeOutbox(row))) {
      sent += 1;
    }
  }
  return sent;
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
export function gcOutbox(db: AppDatabase, opts: OutboxGcOptions = {}): number {
  const now = opts.now?.() ?? Date.now();
  const cutoff = now - (opts.retentionMs ?? OUTBOX_RETENTION_MS);
  return db
    .delete(outbox)
    .where(and(isNotNull(outbox.ackedAt), lt(outbox.ackedAt, cutoff)))
    .returning({ id: outbox.id })
    .all()
    .length;
}

export interface OutboxGcDeps extends OutboxGcOptions {
  readonly db: AppDatabase;
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
      const deleted = gcOutbox(deps.db, deps);
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
    gc: () => gcOutbox(deps.db, deps),
  };
}
