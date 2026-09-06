/* eslint-disable no-console */
// Node proof for @gino/host-store-sqlite: the package resolves through its own
// `exports` map under plain Node (self-reference → `dist/`), the better-sqlite3
// driver opens + migrates a file database, and the repo API round-trips a row.
// Run by `bun run check` after `build`; exits non-zero on any failure. Needs
// @gino/host-core built first (its dist/ is consumed the same way).
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { embeddedMigrations, openDatabase, sqliteStorage } from "@gino/host-store-sqlite";
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node";

const dir = mkdtempSync(join(tmpdir(), "gino-node-smoke-"));
try {
  const db = openDatabase({ driver: nodeSqliteDriver, path: join(dir, "gino.db") });
  try {
    const journal = JSON.parse(embeddedMigrations.get("meta/_journal.json"));
    const applied = db.all("SELECT count(*) AS c FROM __drizzle_migrations");
    assert.equal(applied[0].c, journal.entries.length, "every embedded migration applied");

    const storage = sqliteStorage(db, { storeDir: join(dir, "store") });
    const inserted = storage.agents.insert({
      id: "a-node",
      name: "node smoke",
      source: "manual",
      access: "dedicated",
      connectTokenHash: "hash",
      capabilities: { engines: ["claude"] },
      status: "enrolled",
      createdAt: 1000,
    });
    assert.equal(inserted.id, "a-node");
    const read = storage.agents.get("a-node");
    assert.ok(read, "agent row readable");
    assert.equal(read.name, "node smoke");
    assert.equal(storage.agents.list().length, 1);
    storage.ping();
    db.checkpoint();
  }
  finally {
    db.close();
  }
  console.log(`[smoke] node ${process.version} + ${nodeSqliteDriver.name}: ok`);
}
finally {
  rmSync(dir, { recursive: true, force: true });
}
