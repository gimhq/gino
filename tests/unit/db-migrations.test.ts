import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { acquireMigrationLock, createDb, materializeEmbeddedMigrations, runMigrations } from "@/db";

function journalJson(entries: { tag: string; when: number }[]): string {
  return JSON.stringify({
    version: "7",
    dialect: "sqlite",
    entries: entries.map((e, idx) => ({ idx, version: "6", when: e.when, tag: e.tag, breakpoints: true })),
  });
}

function embeddedMap(table: string): Map<string, string> {
  return new Map([
    ["meta/_journal.json", journalJson([{ tag: `0000_${table}`, when: 1000 }])],
    [`0000_${table}.sql`, `CREATE TABLE ${table} (id integer primary key);`],
  ]);
}

function writeFsMigrations(root: string, table: string): void {
  const folder = join(root, "drizzle");
  mkdirSync(join(folder, "meta"), { recursive: true });
  writeFileSync(join(folder, "meta/_journal.json"), journalJson([{ tag: `0000_${table}`, when: 1000 }]));
  writeFileSync(join(folder, `0000_${table}.sql`), `CREATE TABLE ${table} (id integer primary key);`);
}

function tableNames(sqlite: Database): string[] {
  return (sqlite.query("SELECT name FROM sqlite_master WHERE type = 'table'").all() as { name: string }[])
    .map(r => r.name);
}

describe("materializeEmbeddedMigrations (HOST-M9-06)", () => {
  const dirs: string[] = [];
  afterEach(() => {
    for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
  });

  it("writes the map into a fresh unpredictable owner-only temp dir", () => {
    const map = embeddedMap("m906");
    const dir1 = materializeEmbeddedMigrations(map);
    const dir2 = materializeEmbeddedMigrations(map);
    dirs.push(dir1, dir2);

    // mkdtemp: random suffix (two calls never collide) — not the old
    // predictable `gino-migrations-<pid>` name an attacker could pre-create.
    expect(dir1).not.toBe(dir2);
    expect(dirname(dir1)).toBe(tmpdir());
    expect(dir1).toContain("gino-migrations-");

    // Owner-only permissions: no other local user can read or swap contents.
    expect(statSync(dir1).mode & 0o777).toBe(0o700);

    // Contents materialized, including the nested meta/ journal.
    expect(readFileSync(join(dir1, "0000_m906.sql"), "utf8")).toContain("CREATE TABLE m906");
    expect(existsSync(join(dir1, "meta/_journal.json"))).toBe(true);
  });
});

describe("runMigrations source preference (HOST-M9-13)", () => {
  let root: string;
  let sqlite: Database;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "gino-migsrc-"));
    sqlite = new Database(":memory:");
  });

  afterEach(() => {
    sqlite.close();
    rmSync(root, { recursive: true, force: true });
  });

  it("compiled binary ignores an on-disk drizzle/ tree and applies the embedded set", () => {
    writeFsMigrations(root, "planted_fs");
    runMigrations(drizzle(sqlite), { compiled: true, embedded: embeddedMap("vetted"), rootDir: root });
    expect(tableNames(sqlite)).toContain("vetted");
    expect(tableNames(sqlite)).not.toContain("planted_fs");
  });

  it("a populated embedded map wins over the filesystem even when not compiled (packaged bundle)", () => {
    writeFsMigrations(root, "planted_fs");
    runMigrations(drizzle(sqlite), { compiled: false, embedded: embeddedMap("vetted"), rootDir: root });
    expect(tableNames(sqlite)).toContain("vetted");
    expect(tableNames(sqlite)).not.toContain("planted_fs");
  });

  it("dev run (empty embedded stub) applies the filesystem drizzle/ folder", () => {
    writeFsMigrations(root, "dev_fs");
    runMigrations(drizzle(sqlite), { compiled: false, embedded: new Map(), rootDir: root });
    expect(tableNames(sqlite)).toContain("dev_fs");
  });

  it("compiled binary with an empty embedded map fails fast instead of reading the filesystem", () => {
    writeFsMigrations(root, "planted_fs");
    expect(() =>
      runMigrations(drizzle(sqlite), { compiled: true, embedded: new Map(), rootDir: root }),
    ).toThrow(/No migrations available/);
    expect(tableNames(sqlite)).not.toContain("planted_fs");
  });

  it("embedded map without the journal sentinel is rejected as corrupt", () => {
    const map = embeddedMap("vetted");
    map.delete("meta/_journal.json");
    expect(() =>
      runMigrations(drizzle(sqlite), { compiled: true, embedded: map, rootDir: root }),
    ).toThrow(/meta\/_journal\.json missing/);
  });
});

describe("migration lock (HOST-M9-14)", () => {
  let dir: string;
  let dbPath: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "gino-miglock-"));
    dbPath = join(dir, "gino.db");
  });

  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  it("rejects a second acquirer while held and admits it after release", () => {
    const release = acquireMigrationLock(dbPath, 0);
    expect(() => acquireMigrationLock(dbPath, 0)).toThrow(/migration lock/);
    release();
    const release2 = acquireMigrationLock(dbPath, 0);
    release2();
  });

  it("reclaims a stale lock left behind by a dead process", () => {
    // Beyond the kernel's pid_max (default 4194304) — guaranteed not alive.
    writeFileSync(`${dbPath}.lock`, "999999999\n");
    const release = acquireMigrationLock(dbPath, 0);
    release();
    expect(existsSync(`${dbPath}.lock`)).toBe(false);
  });

  it("createDb refuses to migrate while another process holds the lock", () => {
    const release = acquireMigrationLock(dbPath, 0);
    try {
      expect(() => createDb(dbPath, { migrationLockTimeoutMs: 0 })).toThrow(/migration lock/);
      // The loser never migrated: the file (created on open) has no tables.
      const sqlite = new Database(dbPath, { readonly: true });
      try {
        expect(tableNames(sqlite)).toEqual([]);
      }
      finally {
        sqlite.close();
      }
    }
    finally {
      release();
    }
  });

  it("createDb releases the lock once migrations are done", () => {
    const db = createDb(dbPath, { migrationLockTimeoutMs: 0 });
    try {
      const release = acquireMigrationLock(dbPath, 0);
      release();
    }
    finally {
      db.close();
    }
  });
});
