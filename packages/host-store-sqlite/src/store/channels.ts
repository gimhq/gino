import type { ChannelRepo, ChannelRow, NewChannelBindingRow, NewChannelRow } from "@gino/host-core/domain";
// SQLITE STORE — `ChannelRepo` on drizzle + sqlite (channels /
// channel_bindings / channel_cursors). Each method is the one statement the
// domain issues today (`channels/registry.ts`, `channels/ingress.ts`,
// `channels/egress.ts`), unchanged.
import type { AppDatabase } from "../driver";
import { and, asc, desc, eq, lt, sql } from "drizzle-orm";
import { channelBindings, channelCursors, channels } from "../schema";

export function sqliteChannelRepo(db: AppDatabase): ChannelRepo {
  return {
    insert(row: NewChannelRow): ChannelRow {
      return db.insert(channels).values(row).returning().get();
    },
    get(channelId) {
      return db.select().from(channels).where(eq(channels.id, channelId)).get();
    },
    getByName(name) {
      // Names are not unique — the FIRST registered wins (`resolveChannel`).
      return db.select().from(channels).where(eq(channels.name, name)).orderBy(asc(sql`rowid`)).get();
    },
    list() {
      return db.select().from(channels).orderBy(desc(channels.createdAt), asc(sql`rowid`)).all();
    },
    setConfig(channelId, config) {
      return db.update(channels).set({ config }).where(eq(channels.id, channelId)).returning().get();
    },

    bindIfAbsent(row: NewChannelBindingRow): boolean {
      return db
        .insert(channelBindings)
        .values(row)
        .onConflictDoNothing({ target: [channelBindings.channelId, channelBindings.conversationRef] })
        .returning({ channelId: channelBindings.channelId })
        .get() != null;
    },
    getBinding(channelId, conversationRef) {
      return db
        .select()
        .from(channelBindings)
        .where(and(eq(channelBindings.channelId, channelId), eq(channelBindings.conversationRef, conversationRef)))
        .get();
    },
    listBindings(channelId) {
      return db
        .select()
        .from(channelBindings)
        .where(eq(channelBindings.channelId, channelId))
        .orderBy(desc(channelBindings.createdAt), asc(sql`rowid`))
        .all();
    },
    isTaskBound(channelId, taskId) {
      return db
        .select({ taskId: channelBindings.taskId })
        .from(channelBindings)
        .where(and(eq(channelBindings.channelId, channelId), eq(channelBindings.taskId, taskId)))
        .get() != null;
    },

    readCursor(channelId, taskId) {
      const row = db
        .select({ lastSeq: channelCursors.lastSeq })
        .from(channelCursors)
        .where(and(eq(channelCursors.channelId, channelId), eq(channelCursors.taskId, taskId)))
        .get();
      return row?.lastSeq ?? 0;
    },
    advanceCursor(channelId, taskId, position, now) {
      // Forward only: an absent row reads as `0`, so a non-positive position
      // never creates one; an existing row moves only past its stored mark.
      if (position <= 0) {
        return false;
      }
      return db
        .insert(channelCursors)
        .values({ channelId, taskId, lastSeq: position, updatedAt: now })
        .onConflictDoUpdate({
          target: [channelCursors.channelId, channelCursors.taskId],
          set: { lastSeq: position, updatedAt: now },
          setWhere: lt(channelCursors.lastSeq, position),
        })
        .returning({ channelId: channelCursors.channelId })
        .get() != null;
    },
  };
}
