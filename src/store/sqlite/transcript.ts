// SQLITE STORE — `TranscriptRepo` on drizzle + bun:sqlite (messages). Each
// method is the one statement the domain issues today (`msgstore/messages.ts`,
// `msgstore/reads.ts`, `channels/egress.ts`), unchanged.
import type { AppDatabase } from "@/db";
import type { NewMessageRow, TranscriptRepo } from "@/host/ports/storage";
import { and, asc, eq, gt, sql } from "drizzle-orm";
import { messages } from "@/db/schema";

export function sqliteTranscriptRepo(db: AppDatabase): TranscriptRepo {
  return {
    insertIfAbsent(row: NewMessageRow) {
      // Idempotent on `(thread_id, seq)` (protocol §6): ON CONFLICT DO NOTHING,
      // so the FIRST write wins and a replay neither overwrites nor raises.
      return db
        .insert(messages)
        .values(row)
        .onConflictDoNothing({ target: [messages.threadId, messages.seq] })
        .returning({ id: messages.id })
        .get() != null;
    },
    read(threadId, opts) {
      return db
        .select()
        .from(messages)
        .where(and(
          eq(messages.threadId, threadId),
          ...(opts.afterSeq != null ? [gt(messages.seq, opts.afterSeq)] : []),
        ))
        .orderBy(messages.seq)
        .limit(opts.limit)
        .all();
    },
    stats(threadId) {
      const row = db
        .select({
          messageCount: sql<number>`count(*)`,
          lastSeq: sql<number | null>`max(${messages.seq})`,
        })
        .from(messages)
        .where(eq(messages.threadId, threadId))
        .get();
      return { messageCount: row?.messageCount ?? 0, lastSeq: row?.lastSeq ?? null };
    },
    threads(taskId) {
      return db
        .select({ threadId: messages.threadId, firstAt: sql<number>`min(${messages.createdAt})` })
        .from(messages)
        .where(eq(messages.taskId, taskId))
        .groupBy(messages.threadId)
        .orderBy(sql`min(${messages.createdAt})`, sql`min(${messages.id})`)
        .all();
    },
    taskOf(threadId) {
      return db
        .select({ taskId: messages.taskId })
        .from(messages)
        .where(eq(messages.threadId, threadId))
        .limit(1)
        .get()
        ?.taskId;
    },
    readAfterId(taskId, afterId, limit) {
      // The tail WINDOW is selected by `id` so a capped batch is contiguous
      // (HOST-M5M6-03); the caller re-sorts for delivery.
      return db
        .select()
        .from(messages)
        .where(and(eq(messages.taskId, taskId), gt(messages.id, afterId)))
        .orderBy(asc(messages.id))
        .limit(limit)
        .all();
    },
  };
}
