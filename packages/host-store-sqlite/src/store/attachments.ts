import type { AttachmentRepo, NewAttachmentRow } from "@gino/host-core/domain";
// SQLITE STORE — `AttachmentRepo` on drizzle + sqlite (attachments). Each
// method is the one statement the domain issues today
// (`msgstore/attachments.ts`, `configstore/serve.ts`, `configstore/gc.ts`),
// unchanged.
import type { AppDatabase } from "../driver";
import { asc, eq, sql } from "drizzle-orm";
import { attachments } from "../schema";

export function sqliteAttachmentRepo(db: AppDatabase): AttachmentRepo {
  return {
    insertIfAbsent(row: NewAttachmentRow): boolean {
      // Idempotent on `id` (ON CONFLICT DO NOTHING) so a replayed carrier frame
      // neither duplicates nor overwrites a descriptor.
      return db
        .insert(attachments)
        .values(row)
        .onConflictDoNothing({ target: attachments.id })
        .returning({ id: attachments.id })
        .get() != null;
    },
    getById(id) {
      return db.select().from(attachments).where(eq(attachments.id, id)).get();
    },
    listBySha(sha256) {
      return db.select().from(attachments).where(eq(attachments.sha256, sha256)).orderBy(asc(sql`rowid`)).all();
    },
    listAll() {
      return db.select().from(attachments).orderBy(asc(sql`rowid`)).all();
    },
  };
}
