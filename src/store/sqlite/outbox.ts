// SQLITE STORE — `OutboxRepo` on drizzle + bun:sqlite (outbox). Each method is
// the one statement the domain issues today (`protocol/outbox.ts`), unchanged.
// The AUTOINCREMENT `id` IS the monotonic delivery id (`outboxId` / `msgId`).
import type { AppDatabase } from "@/db";
import type { NewOutboxRow, OutboxRepo, OutboxRow } from "@/host/ports/storage";
import { and, asc, eq, isNotNull, isNull, lt, ne } from "drizzle-orm";
import { outbox } from "@/db/schema";

export function sqliteOutboxRepo(db: AppDatabase): OutboxRepo {
  return {
    append(row: NewOutboxRow): OutboxRow {
      return db.insert(outbox).values(row).returning().get();
    },
    listUnacked(agentId) {
      return db
        .select()
        .from(outbox)
        .where(and(eq(outbox.agentId, agentId), isNull(outbox.ackedAt)))
        .orderBy(asc(outbox.id))
        .all();
    },
    listAllUnacked() {
      return db
        .select()
        .from(outbox)
        .where(isNull(outbox.ackedAt))
        .orderBy(asc(outbox.id))
        .all();
    },
    markAcked(agentId, id, now, opts = {}) {
      // Scoped to the agent's own rows (ids are a global sequence); with
      // `excludeType: "message"` this is `ack{id}` — a `message` row is only
      // acked by the durable-storage `msg_ack` (protocol §4.2).
      return db
        .update(outbox)
        .set({ ackedAt: now })
        .where(and(
          eq(outbox.id, id),
          eq(outbox.agentId, agentId),
          ...(opts.excludeType !== undefined ? [ne(outbox.type, opts.excludeType)] : []),
        ))
        .returning({ id: outbox.id })
        .get() != null;
    },
    purge(agentId) {
      // HOST-M3-12: unacked rows only — acked history is retention GC's business.
      return db
        .delete(outbox)
        .where(and(eq(outbox.agentId, agentId), isNull(outbox.ackedAt)))
        .returning({ id: outbox.id })
        .all()
        .length;
    },
    gc(before) {
      // Unacked rows are NEVER deleted — still owed to the agent (at-least-once, §4.2).
      return db
        .delete(outbox)
        .where(and(isNotNull(outbox.ackedAt), lt(outbox.ackedAt, before)))
        .returning({ id: outbox.id })
        .all()
        .length;
    },
  };
}
