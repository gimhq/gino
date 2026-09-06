import type { NewTaskRow, TaskRepo, TaskRow } from "@gino/host-core/domain";
// SQLITE STORE — `TaskRepo` on drizzle + bun:sqlite (tasks). Each method is the
// one statement the domain issues today (`tasks/store.ts`, `agents/registry.ts`
// `removeAgent`), unchanged.
import type { AppDatabase } from "../db";
import { and, asc, desc, eq, isNull, sql } from "drizzle-orm";
import { tasks } from "../schema";

export function sqliteTaskRepo(db: AppDatabase): TaskRepo {
  return {
    insert(row: NewTaskRow): TaskRow {
      return db.insert(tasks).values(row).returning().get();
    },
    get(taskId) {
      return db.select().from(tasks).where(eq(tasks.id, taskId)).get();
    },
    list(filter = {}) {
      const conds = [
        filter.ownerId != null ? eq(tasks.ownerId, filter.ownerId) : undefined,
        filter.agentId != null ? eq(tasks.agentId, filter.agentId) : undefined,
        filter.status != null ? eq(tasks.status, filter.status) : undefined,
      ].filter(c => c != null);

      const base = db.select().from(tasks);
      const filtered = conds.length > 0 ? base.where(and(...conds)) : base;
      // `rowid` tiebreak: equal `createdAt` keeps insertion order (port ordering rule).
      return filtered.orderBy(desc(tasks.createdAt), asc(sql`rowid`)).all();
    },
    setPlacementOnce(taskId, agentId, now) {
      // SET ONCE (§3 no-migration): the `agentId IS NULL` guard is the CAS.
      return db
        .update(tasks)
        .set({ agentId, assignedAt: now, seededAt: null, currentThreadId: null, updatedAt: now })
        .where(and(eq(tasks.id, taskId), isNull(tasks.agentId)))
        .returning({ id: tasks.id })
        .get() != null;
    },
    setStatus(taskId, status, now) {
      return db
        .update(tasks)
        .set({ status, updatedAt: now })
        .where(eq(tasks.id, taskId))
        .returning()
        .get();
    },
    setCurrentThread(taskId, threadId, now) {
      return db
        .update(tasks)
        .set({ currentThreadId: threadId, updatedAt: now })
        .where(eq(tasks.id, taskId))
        .returning()
        .get();
    },
    markSeeded(taskId, now) {
      db
        .update(tasks)
        .set({ seededAt: now })
        .where(and(eq(tasks.id, taskId), isNull(tasks.seededAt)))
        .run();
    },
    orphanByAgent(agentId, now) {
      return db
        .update(tasks)
        .set({ agentId: null, updatedAt: now })
        .where(eq(tasks.agentId, agentId))
        .returning({ id: tasks.id })
        .all()
        .map(row => row.id);
    },
  };
}
