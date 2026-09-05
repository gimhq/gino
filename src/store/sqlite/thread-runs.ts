// SQLITE STORE — `ThreadRunRepo` on drizzle + bun:sqlite (thread_runs). Each
// method is the one statement the domain issues today
// (`msgstore/thread-runs.ts`), unchanged.
import type { AppDatabase } from "@/db";
import type { NewThreadRunRow, ThreadRunRepo } from "@/host/ports/storage";
import { asc, eq, sql } from "drizzle-orm";
import { threadRuns } from "@/db/schema";

export function sqliteThreadRunRepo(db: AppDatabase): ThreadRunRepo {
  return {
    upsert(row: NewThreadRunRow) {
      // UPSERT keyed by `thread_id`: on conflict only the terminal mirror
      // fields move; context + timestamps stay as first written.
      db
        .insert(threadRuns)
        .values(row)
        .onConflictDoUpdate({
          target: threadRuns.threadId,
          set: {
            status: row.status,
            failureReason: row.failureReason ?? null,
            engineSessionId: row.engineSessionId ?? null,
            durationMs: row.durationMs ?? null,
            finalSeq: row.finalSeq ?? null,
          },
        })
        .run();
    },
    get(threadId) {
      return db
        .select()
        .from(threadRuns)
        .where(eq(threadRuns.threadId, threadId))
        .get();
    },
    listByTask(taskId) {
      return db
        .select()
        .from(threadRuns)
        .where(eq(threadRuns.taskId, taskId))
        .orderBy(asc(sql`rowid`))
        .all();
    },
  };
}
