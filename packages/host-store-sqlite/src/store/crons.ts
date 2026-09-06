import type { CronMirrorRepo, CronPromptUpsert, CronStateUpsert } from "@gino/host-core/domain";
// SQLITE STORE — `CronMirrorRepo` on drizzle + bun:sqlite (crons). Each method
// is the one statement the domain issues today (`msgstore/crons.ts`,
// `tasks/store.listTaskCrons`), unchanged.
import type { AppDatabase } from "../db";
import { asc, desc, eq, sql } from "drizzle-orm";
import { crons } from "../schema";

export function sqliteCronMirrorRepo(db: AppDatabase): CronMirrorRepo {
  return {
    upsertState(input: CronStateUpsert) {
      const { id, taskId, continuity, ...mirror } = input;
      db.insert(crons)
        .values({
          id,
          taskId,
          // `prompt` is the dispatch-side work command — not carried by `cron_state`.
          // Seed empty on first mirror; preserved (never in the conflict set) after.
          prompt: "",
          continuity: continuity ?? "",
          ...mirror,
        })
        .onConflictDoUpdate({
          target: crons.id,
          set: {
            ...mirror,
            ...(continuity !== undefined ? { continuity } : {}),
          },
        })
        .run();
    },
    upsertPrompt(input: CronPromptUpsert) {
      // HOST-M5M6-09: INSERT a stub row when the command precedes the first
      // `cron_state`; on conflict update ONLY `prompt`, leaving every mirror
      // field to `upsertState`.
      const { id, taskId, prompt, updatedAt } = input;
      db.insert(crons)
        .values({ id, taskId, kind: "", prompt, continuity: "", updatedAt })
        .onConflictDoUpdate({ target: crons.id, set: { prompt, updatedAt } })
        .run();
    },
    get(cronId) {
      return db
        .select()
        .from(crons)
        .where(eq(crons.id, cronId))
        .get();
    },
    listByTask(taskId) {
      return db
        .select()
        .from(crons)
        .where(eq(crons.taskId, taskId))
        .orderBy(desc(crons.updatedAt), asc(sql`rowid`))
        .all();
    },
  };
}
