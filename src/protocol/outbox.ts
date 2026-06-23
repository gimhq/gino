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
//     stored the input. For B1b the message's `msgId` reuses the row id and the
//     ack just marks the row delivered (the cursor advance is a B3 seam).
//
// Layering: the outbox talks to the live socket only through the abstract
// `FrameSender` (sessions implements it), and rebuilds the wire frame via the
// M2 `encodeOutbox` codec — it never hand-rolls the envelope.
import type { AppDatabase } from "@/db";
import type { Outbox } from "@/db/schema";
import { and, asc, eq, isNull } from "drizzle-orm";
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
 * Persist a host→agent frame and, if the agent is online, send it. The row is
 * written FIRST (durability before delivery); the live push is best-effort —
 * a miss is recovered by `replayUnacked` on reconnect. For a `message` frame
 * the row's `msgId` is set to its own id so the frame carries a stable delivery
 * id the agent echoes in `msg_ack`.
 */
export function enqueue(db: AppDatabase, sender: FrameSender, input: EnqueueInput): Outbox {
  const inserted = db
    .insert(outbox)
    .values({
      agentId: input.agentId,
      type: input.type,
      taskId: input.taskId ?? null,
      threadId: input.threadId ?? null,
      cronId: input.cronId ?? null,
      msgId: null,
      dataJson: input.data ?? {},
    })
    .returning()
    .get();

  let row = inserted;
  if (input.type === "message") {
    db.update(outbox).set({ msgId: inserted.id }).where(eq(outbox.id, inserted.id)).run();
    row = { ...inserted, msgId: inserted.id };
  }

  if (sender.isOnline(input.agentId)) {
    sender.send(input.agentId, encodeOutbox(row));
  }
  return row;
}

/** Agent acked a generic host→agent frame (`ack{id}`) → stop replaying it. */
export function ackOutbox(db: AppDatabase, id: number): void {
  db.update(outbox).set({ ackedAt: Date.now() }).where(eq(outbox.id, id)).run();
}

/**
 * Agent durably stored a `message` (`msg_ack{msgId}`) → mark it delivered.
 * TODO(M4/B3): advance the per-task delivery cursor here; for B1b marking the
 * row acked is enough to stop replay.
 */
export function ackMessage(db: AppDatabase, msgId: number): void {
  db
    .update(outbox)
    .set({ ackedAt: Date.now() })
    .where(and(eq(outbox.msgId, msgId), eq(outbox.type, "message")))
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
