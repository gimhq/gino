import { describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { openDatabase } from "../../src/db";
import { embeddedMigrations } from "../../src/embedded-migrations";
import { testDrivers } from "../helpers/drivers";

for (const driver of testDrivers) {
  describe(`openDatabase (${driver.name})`, () => {
    it("opens an in-memory db, applies PRAGMAs, and runs the embedded migrations", () => {
      const db = openDatabase({ driver, path: ":memory:" });
      try {
        // `.all()` returns rows as objects (`.get()` returns value-arrays).
        const fk = db.all(sql`PRAGMA foreign_keys`) as { foreign_keys: number }[];
        expect(fk[0]?.foreign_keys).toBe(1);
        // Every journal entry was applied.
        const journal = JSON.parse(embeddedMigrations.get("meta/_journal.json")!) as { entries: unknown[] };
        const applied = db.all(sql`SELECT count(*) as c FROM "__drizzle_migrations"`) as { c: number }[];
        expect(applied[0]?.c).toBe(journal.entries.length);
        // Writes work end-to-end.
        db.run(sql`CREATE TABLE t (id integer primary key, v text)`);
        db.run(sql`INSERT INTO t (v) VALUES ('x')`);
        const rows = db.all(sql`SELECT count(*) as c FROM t`) as { c: number }[];
        expect(rows[0]?.c).toBe(1);
        db.checkpoint();
      }
      finally {
        db.close();
      }
    });
  });
}
