import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { acquireMigrationLock } from "../../src/db";
import { bunSqliteDriver, createDb } from "../../src/drivers/bun";
import { embeddedMigrations } from "../../src/embedded-migrations";
import { runMigrations } from "../../src/migrate";
import { testDrivers } from "../helpers/drivers";

function journalJson(entries: { tag: string; when: number }[]): string {
  return JSON.stringify({
    version: "7",
    dialect: "sqlite",
    entries: entries.map((e, idx) => ({ idx, version: "6", when: e.when, tag: e.tag, breakpoints: true })),
  });
}

function embeddedMap(table: string, when = 1000): Map<string, string> {
  return new Map([
    ["meta/_journal.json", journalJson([{ tag: `0000_${table}`, when }])],
    [`0000_${table}.sql`, `CREATE TABLE ${table} (id integer primary key);`],
  ]);
}

interface JournalRow { id: number | null; hash: string; created_at: number }
interface MasterRow { name: string; sql: string | null }

function journalRows(sqlite: Database): JournalRow[] {
  return sqlite.query("SELECT id, hash, created_at FROM __drizzle_migrations ORDER BY created_at").all() as JournalRow[];
}

function schemaRows(sqlite: Database): MasterRow[] {
  return sqlite.query("SELECT name, sql FROM sqlite_master ORDER BY type, name").all() as MasterRow[];
}

function tableNames(sqlite: Database): string[] {
  return (sqlite.query("SELECT name FROM sqlite_master WHERE type = 'table'").all() as { name: string }[])
    .map(r => r.name);
}

/** Apply the real embedded set with drizzle's folder migrator (the reference implementation). */
function migrateWithDrizzle(sqlite: Database, migrations: ReadonlyMap<string, string> = embeddedMigrations): void {
  const dir = mkdtempSync(join(tmpdir(), "gino-drizzle-ref-"));
  try {
    mkdirSync(join(dir, "meta"), { recursive: true });
    for (const [name, content] of migrations) writeFileSync(join(dir, name), content);
    migrate(drizzle(sqlite), { migrationsFolder: dir });
  }
  finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("runMigrations (driver-agnostic, drizzle-journal compatible)", () => {
  const entryCount = (JSON.parse(embeddedMigrations.get("meta/_journal.json")!) as { entries: unknown[] }).entries.length;

  for (const driver of testDrivers) {
    describe(driver.name, () => {
      it("a fresh db gets every embedded migration; a second run is a no-op", () => {
        const handle = driver.open(":memory:");
        try {
          runMigrations(handle.db);
          const first = handle.db.all(sql`SELECT id, hash, created_at FROM "__drizzle_migrations" ORDER BY created_at`) as JournalRow[];
          expect(first).toHaveLength(entryCount);
          expect(handle.db.all(sql`SELECT name FROM sqlite_master WHERE name = 'agents'`)).toHaveLength(1);

          runMigrations(handle.db);
          const second = handle.db.all(sql`SELECT id, hash, created_at FROM "__drizzle_migrations" ORDER BY created_at`) as JournalRow[];
          expect(second).toEqual(first);
        }
        finally {
          handle.close();
        }
      });
    });
  }

  it("leaves a db pre-migrated by drizzle's own migrator untouched", () => {
    const sqlite = new Database(":memory:");
    try {
      migrateWithDrizzle(sqlite);
      const before = journalRows(sqlite);
      expect(before).toHaveLength(entryCount);

      runMigrations(drizzle(sqlite));
      expect(journalRows(sqlite)).toEqual(before);
    }
    finally {
      sqlite.close();
    }
  });

  it("produces the same __drizzle_migrations rows and sqlite_master as drizzle's migrator", () => {
    const reference = new Database(":memory:");
    const ours = new Database(":memory:");
    try {
      migrateWithDrizzle(reference);
      runMigrations(drizzle(ours));
      expect(journalRows(ours)).toEqual(journalRows(reference));
      expect(schemaRows(ours)).toEqual(schemaRows(reference));
    }
    finally {
      reference.close();
      ours.close();
    }
  });

  it("applies only the entries newer than the last recorded one", () => {
    const sqlite = new Database(":memory:");
    try {
      runMigrations(drizzle(sqlite), embeddedMap("first", 1000));
      const grown = new Map(embeddedMap("first", 1000));
      grown.set("meta/_journal.json", journalJson([{ tag: "0000_first", when: 1000 }, { tag: "0001_second", when: 2000 }]));
      grown.set("0001_second.sql", "CREATE TABLE second (id integer primary key);");
      runMigrations(drizzle(sqlite), grown);
      expect(tableNames(sqlite)).toEqual(expect.arrayContaining(["first", "second"]));
      expect(journalRows(sqlite).map(r => r.created_at)).toEqual([1000, 2000]);
    }
    finally {
      sqlite.close();
    }
  });

  it("splits a migration on statement-breakpoint markers and commits it atomically", () => {
    const sqlite = new Database(":memory:");
    try {
      const map = new Map([
        ["meta/_journal.json", journalJson([{ tag: "0000_multi", when: 1000 }])],
        ["0000_multi.sql", "CREATE TABLE a (id integer primary key);\n--> statement-breakpoint\nCREATE TABLE b (id integer primary key);"],
      ]);
      runMigrations(drizzle(sqlite), map);
      expect(tableNames(sqlite)).toEqual(expect.arrayContaining(["a", "b"]));

      // A failing statement rolls the whole entry back: no table, no journal row.
      const broken = new Map([
        ["meta/_journal.json", journalJson([{ tag: "0000_multi", when: 1000 }, { tag: "0001_broken", when: 2000 }])],
        ["0000_multi.sql", map.get("0000_multi.sql")!],
        ["0001_broken.sql", "CREATE TABLE c (id integer primary key);\n--> statement-breakpoint\nCREATE TABLE a (id integer primary key);"],
      ]);
      expect(() => runMigrations(drizzle(sqlite), broken)).toThrow();
      expect(tableNames(sqlite)).not.toContain("c");
      expect(journalRows(sqlite).map(r => r.created_at)).toEqual([1000]);
    }
    finally {
      sqlite.close();
    }
  });

  it("rejects a map without the journal sentinel as corrupt", () => {
    const sqlite = new Database(":memory:");
    try {
      const map = embeddedMap("vetted");
      map.delete("meta/_journal.json");
      expect(() => runMigrations(drizzle(sqlite), map)).toThrow(/meta\/_journal\.json missing/);
      expect(tableNames(sqlite)).not.toContain("vetted");
    }
    finally {
      sqlite.close();
    }
  });

  it("rejects a map whose journal lists a missing .sql file", () => {
    const sqlite = new Database(":memory:");
    try {
      const map = embeddedMap("vetted");
      map.delete("0000_vetted.sql");
      expect(() => runMigrations(drizzle(sqlite), map)).toThrow(/0000_vetted\.sql missing/);
    }
    finally {
      sqlite.close();
    }
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

  it("waits for the holder up to the timeout before giving up", () => {
    const release = acquireMigrationLock(dbPath, 0);
    const started = Date.now();
    expect(() => acquireMigrationLock(dbPath, 150)).toThrow(/migration lock/);
    expect(Date.now() - started).toBeGreaterThanOrEqual(100);
    release();
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

  it("createDb is openDatabase over the bun driver", () => {
    const db = createDb(":memory:");
    try {
      expect(bunSqliteDriver.name).toBe("bun:sqlite");
      expect(db.all(sql`SELECT count(*) as c FROM "__drizzle_migrations"`)).toEqual([{ c: (JSON.parse(embeddedMigrations.get("meta/_journal.json")!) as { entries: unknown[] }).entries.length }]);
    }
    finally {
      db.close();
    }
  });
});
