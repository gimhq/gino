import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { ackMessage, replayUnacked } from "@gino/host-core/domain";
import { Database } from "bun:sqlite";
import { afterAll, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { createDb } from "../../src/db";
import { sqliteStorage } from "../../src/store";

// Migration 0003 drops the vestigial `outbox.msg_id` column (HOST-M1M2-07
// follow-up: the delivery id is derived from the row id, never stored) and the
// `outbox_msg_ack_idx` partial index. This test proves the drop is SAFE on a
// legacy-shaped DB: rows written under the old schema — msg_id populated
// (pre-fix enqueue back-filled it) or NULL (the pre-fix crash window) —
// survive the migration intact and stay deliverable + ackable.

const LEGACY_MIGRATION_COUNT = 3; // 0000..0002 — the last schema WITH msg_id

const scratch = resolve(tmpdir(), `gino-msgid-migration-${process.pid}-${Date.now()}`);
const dbPath = resolve(scratch, "gino.db");

afterAll(() => {
  rmSync(scratch, { recursive: true, force: true });
});

/** Materialise a migrations folder truncated to the first N journal entries. */
function truncatedMigrations(count: number): string {
  const src = resolve(import.meta.dir, "../../drizzle");
  const journal = JSON.parse(readFileSync(resolve(src, "meta/_journal.json"), "utf8")) as {
    entries: { idx: number; tag: string }[];
  };
  const entries = journal.entries.filter(e => e.idx < count);
  expect(entries).toHaveLength(count); // the repo must still carry the legacy migrations

  const out = resolve(scratch, `migrations-${count}`);
  mkdirSync(resolve(out, "meta"), { recursive: true });
  writeFileSync(resolve(out, "meta/_journal.json"), JSON.stringify({ ...journal, entries }));
  for (const e of entries) {
    writeFileSync(resolve(out, `${e.tag}.sql`), readFileSync(resolve(src, `${e.tag}.sql`)));
  }
  return out;
}

describe("migration 0003 — drop outbox.msg_id on a legacy DB", () => {
  it("preserves legacy rows (msg_id populated and NULL) and drops column + index", () => {
    // 1. Build a LEGACY-shaped DB: migrate only up to 0002, then write rows the
    //    way the pre-fix host did.
    mkdirSync(scratch, { recursive: true });
    const legacy = new Database(dbPath, { create: true });
    legacy.exec("PRAGMA journal_mode = WAL");
    migrate(drizzle(legacy), { migrationsFolder: truncatedMigrations(LEGACY_MIGRATION_COUNT) });

    const cols = legacy.query("PRAGMA table_info(outbox)").all() as { name: string }[];
    expect(cols.map(c => c.name)).toContain("msg_id"); // legacy shape confirmed

    legacy.exec(`
      INSERT INTO agents (id, source, access, connect_token_hash, capabilities, status, created_at)
      VALUES ('agent-legacy', 'manual', 'dedicated', 'hash', '{"engines":["claude"]}', 'enrolled', 1000);
    `);
    // Pre-fix enqueue: msg_id back-filled with the row id.
    legacy.exec(`
      INSERT INTO outbox (id, agent_id, type, task_id, thread_id, msg_id, data_json, created_at, acked_at)
      VALUES (1, 'agent-legacy', 'message', 't-1', 'c-1', 1, '{"text":"backfilled"}', 1000, NULL);
    `);
    // Pre-fix crash window: durable message row, msg_id never back-filled.
    legacy.exec(`
      INSERT INTO outbox (id, agent_id, type, task_id, msg_id, data_json, created_at, acked_at)
      VALUES (2, 'agent-legacy', 'message', 't-1', NULL, '{"text":"orphan"}', 1001, NULL);
    `);
    // A non-message frame, already acked.
    legacy.exec(`
      INSERT INTO outbox (id, agent_id, type, task_id, msg_id, data_json, created_at, acked_at)
      VALUES (3, 'agent-legacy', 'task_upsert', 't-1', NULL, '{"name":"job"}', 1002, 2000);
    `);
    legacy.close();

    // 2. Reopen through the real bootstrap — createDb runs the remaining
    //    migrations (0003 drops the index, then the column).
    const db = createDb(dbPath);
    const storage = sqliteStorage(db);
    try {
      const names = (db.all(
        sql`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'outbox'`,
      ) as { name: string }[]).map(r => r.name);
      expect(names).not.toContain("outbox_msg_ack_idx");
      expect(names).toContain("outbox_agent_unacked_idx");

      const shape = (db.all(sql`PRAGMA table_info(outbox)`) as { name: string }[]).map(c => c.name);
      expect(shape).not.toContain("msg_id");

      // Every legacy row survived with its data intact.
      const rows = db.all(
        sql`SELECT id, agent_id AS agentId, type, task_id AS taskId, thread_id AS threadId, data_json AS dataJson, created_at AS createdAt, acked_at AS ackedAt FROM outbox ORDER BY id`,
      ) as { id: number; agentId: string; type: string; taskId: string; threadId: string | null; dataJson: string; createdAt: number; ackedAt: number | null }[];
      expect(rows).toHaveLength(3);
      expect(rows[0]).toMatchObject({ id: 1, type: "message", taskId: "t-1", threadId: "c-1", dataJson: "{\"text\":\"backfilled\"}", createdAt: 1000, ackedAt: null });
      expect(rows[1]).toMatchObject({ id: 2, type: "message", dataJson: "{\"text\":\"orphan\"}", ackedAt: null });
      expect(rows[2]).toMatchObject({ id: 3, type: "task_upsert", ackedAt: 2000 });

      // 3. The migrated rows still work end-to-end: both legacy messages replay
      //    with a derived msgId (= row id) and are ackable by msg_ack.
      const sent: Record<string, unknown>[] = [];
      const sender = {
        isOnline: () => true,
        send: (_: string, frame: Record<string, unknown>) => {
          sent.push(frame);
          return true;
        },
        closeAgent: () => false,
      };
      expect(replayUnacked(storage, sender, "agent-legacy")).toBe(2);
      expect(sent[0]).toMatchObject({ type: "message", msgId: 1 });
      expect(sent[1]).toMatchObject({ type: "message", msgId: 2 });

      ackMessage(storage, "agent-legacy", 1);
      ackMessage(storage, "agent-legacy", 2);
      sent.length = 0;
      expect(replayUnacked(storage, sender, "agent-legacy")).toBe(0);
    }
    finally {
      db.close();
    }
  });
});
