// M5 — `thread_message` → `messages` (the durable transcript) + transcript reads.
//
// The transcript is the centerpiece of the store and the source M6 distributes
// from. Writes come ONLY from inbound `thread_message` frames; the writer is
// idempotent on the `(thread_id, seq)` primary key, so an at-least-once replay
// of a frame never duplicates a row and never errors. Order is by `seq`
// (per-thread monotonic) — NEVER by arrival or `outboxId` (protocol §6).
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

/** Full transcript for a thread, `ORDER BY seq` (protocol §6 — never arrival). */
export function getTranscript(db: AppDatabase, threadId: string): Message[] {
  return db
    .select()
    .from(messages)
    .where(eq(messages.threadId, threadId))
    .orderBy(messages.seq)
    .all();
}

export interface TranscriptPageOptions {
  /** Cursor: return only rows with `seq > after` (0/absent → from the start). */
  after?: number;
  /** Page size (rows). */
  limit: number;
}

/**
 * One transcript page, `seq > after ORDER BY seq LIMIT n` — the same
 * seq-cursor read shape the channel poll uses, so an admin client pages a
 * long thread instead of loading it whole (HOST-M1M2-16).
 */
export function getTranscriptPage(db: AppDatabase, threadId: string, opts: TranscriptPageOptions): Message[] {
  return db
    .select()
    .from(messages)
    .where(and(eq(messages.threadId, threadId), gt(messages.seq, opts.after ?? 0)))
    .orderBy(messages.seq)
    .limit(opts.limit)
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
