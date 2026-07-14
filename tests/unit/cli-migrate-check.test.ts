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

function writeMigrationsFolder(root: string, count: number): string {
  const folder = join(root, `drizzle-${count}`);
  mkdirSync(join(folder, "meta"), { recursive: true });
  const entries = ENTRIES.slice(0, count);
  writeFileSync(join(folder, "meta/_journal.json"), JSON.stringify({
    version: "7",
    dialect: "sqlite",
    entries: entries.map((e, idx) => ({ idx, version: "6", when: e.when, tag: e.tag, breakpoints: true })),
  }));
  for (const e of entries)
    writeFileSync(join(folder, `${e.tag}.sql`), e.sql);
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
  let fullFolder: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "gino-micheck-"));
    dbPath = join(root, "gino.db");
    fullFolder = writeMigrationsFolder(root, 2);
  });

  afterEach(() => rmSync(root, { recursive: true, force: true }));

  it("reports every migration pending against a nonexistent DB", () => {
    expect(listPendingMigrations({ dbPath, migrationsFolder: fullFolder }))
      .toEqual(["0000_first", "0001_second"]);
  });

  it("reports no pending migrations on a fully migrated DB", () => {
    applyMigrations(dbPath, fullFolder);
    expect(listPendingMigrations({ dbPath, migrationsFolder: fullFolder })).toEqual([]);
  });

  it("reports only the not-yet-applied tail on a partially migrated DB", () => {
    const partialFolder = writeMigrationsFolder(root, 1);
    applyMigrations(dbPath, partialFolder);
    expect(listPendingMigrations({ dbPath, migrationsFolder: fullFolder }))
      .toEqual(["0001_second"]);
  });

  it("treats a DB without the journal table as fully pending", () => {
    const sqlite = new Database(dbPath, { create: true });
    sqlite.exec("CREATE TABLE unrelated (id integer primary key)");
    sqlite.close();
    expect(listPendingMigrations({ dbPath, migrationsFolder: fullFolder }))
      .toEqual(["0000_first", "0001_second"]);
  });

  it("returns null when the migrations folder cannot be read", () => {
    expect(listPendingMigrations({ dbPath, migrationsFolder: join(root, "missing") })).toBeNull();
  });
});
