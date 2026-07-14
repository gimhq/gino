// M5 — `thread_message` → `messages` (the durable transcript) + transcript reads.
//
// The transcript is the centerpiece of the store and the source M6 distributes
// from. Writes come ONLY from inbound `thread_message` frames; the writer is
// idempotent on the `(thread_id, seq)` unique key, so an at-least-once replay
// of a frame never duplicates a row and never errors. Order is by `seq`
// (per-thread monotonic) — NEVER by arrival or `outboxId` (protocol §6).
//
// Retention (HOST-M5M6-08): transcript rows are append-only here — there is
// deliberately NO delete path in the store module. Reclamation belongs to the
// unified store GC alongside attachments/config artifacts (a documented no-op
// today); what this module DOES enforce is that every single-query read is
// bounded (`TRANSCRIPT_READ_LIMIT` below, batch caps in `channels/egress.ts`).
import type { AppDatabase } from "@/db";
import type { Message } from "@/db/schema";
import type { ThreadMessageData } from "@/protocol/frames";
import { and, eq, gt, sql } from "drizzle-orm";
import { messages } from "@/db/schema";

export interface ThreadMessageWrite {
  taskId: string;
  threadId: string;
  data: ThreadMessageData;
}

/**
 * Persist one transcript event into `messages`. Idempotent on
 * `(thread_id, seq)`: a replayed frame is silently ignored (ON CONFLICT DO
 * NOTHING), so the FIRST write of a given `(thread_id, seq)` wins and a later
 * duplicate can neither overwrite nor raise. Nothing is synthesized — only the
 * fields the frame carries are stored.
 */
export function writeThreadMessage(db: AppDatabase, { taskId, threadId, data }: ThreadMessageWrite): void {
  db.insert(messages)
    .values({
      taskId,
      threadId,
      seq: data.seq,
      kind: data.kind,
      role: data.role ?? null,
      content: data.content ?? null,
      tool: data.tool ?? null,
      input: data.input ?? null,
      output: data.output ?? null,
    })
    .onConflictDoNothing({ target: [messages.threadId, messages.seq] })
    .run();
}

/** Per-query row cap on transcript reads (HOST-M5M6-08) — page with `afterSeq` for longer threads. */
export const TRANSCRIPT_READ_LIMIT = 1000;

export interface TranscriptReadOptions {
  /** Read messages with `seq > afterSeq` (pagination cursor). */
  afterSeq?: number;
  /** Row cap for this read (defaults to and is capped at `TRANSCRIPT_READ_LIMIT`). */
  limit?: number;
}

/** Transcript for a thread, `ORDER BY seq` (protocol §6 — never arrival), bounded to at most `TRANSCRIPT_READ_LIMIT` rows. */
export function getTranscript(db: AppDatabase, threadId: string, opts: TranscriptReadOptions = {}): Message[] {
  const limit = Math.min(Math.max(opts.limit ?? TRANSCRIPT_READ_LIMIT, 1), TRANSCRIPT_READ_LIMIT);
  return db
    .select()
    .from(messages)
    .where(and(
      eq(messages.threadId, threadId),
      ...(opts.afterSeq != null ? [gt(messages.seq, opts.afterSeq)] : []),
    ))
    .orderBy(messages.seq)
    .limit(limit)
    .all();
}

export interface TranscriptStats {
  messageCount: number;
  /** Highest stored `seq`, or null when the thread has no transcript rows. */
  lastSeq: number | null;
}

/** Per-thread transcript summary (count + last seq) for bounded list views. */
export function getTranscriptStats(db: AppDatabase, threadId: string): TranscriptStats {
  const row = db
    .select({
      messageCount: sql<number>`count(*)`,
      lastSeq: sql<number | null>`max(${messages.seq})`,
    })
    .from(messages)
    .where(eq(messages.threadId, threadId))
    .get();
  return { messageCount: row?.messageCount ?? 0, lastSeq: row?.lastSeq ?? null };
}
