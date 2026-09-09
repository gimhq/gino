import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { listPendingMigrations } from "@/cli";

const ENTRIES = [
  { tag: "0000_first", when: 1000, sql: "CREATE TABLE first_t (id integer primary key);" },
  { tag: "0001_second", when: 2000, sql: "CREATE TABLE second_t (id integer primary key);" },
];

/**
 * The embedded-migrations map for the first `count` entries — the shape
 * `runMigrations` and `listPendingMigrations` both consume.
 */
function migrationSet(count: number): Map<string, string> {
  const entries = ENTRIES.slice(0, count);
  const map = new Map<string, string>([["meta/_journal.json", JSON.stringify({
    version: "7",
    dialect: "sqlite",
    entries: entries.map((e, idx) => ({ idx, version: "6", when: e.when, tag: e.tag, breakpoints: true })),
  })]]);
  for (const e of entries)
    map.set(`${e.tag}.sql`, e.sql);
  return map;
}

/** The same set on disk, so drizzle's own migrator can seed a fixture DB. */
function writeMigrationsFolder(root: string, count: number): string {
  const folder = join(root, `drizzle-${count}`);
  mkdirSync(join(folder, "meta"), { recursive: true });
  for (const [name, body] of migrationSet(count))
    writeFileSync(join(folder, name), body);
  return folder;
}

function applyMigrations(dbPath: string, migrationsFolder: string): void {
  const sqlite = new Database(dbPath, { create: true });
  try {
    migrate(drizzle(sqlite), { migrationsFolder });
  }
  finally {
    sqlite.close();
  }
}

// HOST-M9-10 — `migrate --check` must report what the next boot would apply,
// not "every bundled migration, forever".
describe("listPendingMigrations (HOST-M9-10)", () => {
  let root: string;
  let dbPath: string;
  let full: Map<string, string>;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "gino-micheck-"));
    dbPath = join(root, "gino.db");
    full = migrationSet(2);
  });

  afterEach(() => rmSync(root, { recursive: true, force: true }));

  it("reports every migration pending against a nonexistent DB", () => {
    expect(listPendingMigrations({ dbPath, migrations: full }))
      .toEqual(["0000_first", "0001_second"]);
  });

  it("reports no pending migrations on a fully migrated DB", () => {
    applyMigrations(dbPath, writeMigrationsFolder(root, 2));
    expect(listPendingMigrations({ dbPath, migrations: full })).toEqual([]);
  });

  it("reports only the not-yet-applied tail on a partially migrated DB", () => {
    applyMigrations(dbPath, writeMigrationsFolder(root, 1));
    expect(listPendingMigrations({ dbPath, migrations: full }))
      .toEqual(["0001_second"]);
  });

  it("treats a DB without the journal table as fully pending", () => {
    const sqlite = new Database(dbPath, { create: true });
    sqlite.exec("CREATE TABLE unrelated (id integer primary key)");
    sqlite.close();
    expect(listPendingMigrations({ dbPath, migrations: full }))
      .toEqual(["0000_first", "0001_second"]);
  });

  it("returns null when the migration set carries no journal", () => {
    expect(listPendingMigrations({ dbPath, migrations: new Map() })).toBeNull();
  });
});
