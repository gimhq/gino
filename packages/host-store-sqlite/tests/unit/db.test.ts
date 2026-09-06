import { describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { createDb } from "../../src/db";

describe("createDb (bun:sqlite)", () => {
  it("opens an in-memory db, applies PRAGMAs, and runs (empty) migrations", () => {
    const db = createDb(":memory:");
    try {
      // `.all()` returns rows as objects (`.get()` returns value-arrays).
      const fk = db.all(sql`PRAGMA foreign_keys`) as { foreign_keys: number }[];
      expect(fk[0]?.foreign_keys).toBe(1);
      // Migrations ran (empty journal) and writes work end-to-end.
      db.run(sql`CREATE TABLE t (id integer primary key, v text)`);
      db.run(sql`INSERT INTO t (v) VALUES ('x')`);
      const rows = db.all(sql`SELECT count(*) as c FROM t`) as { c: number }[];
      expect(rows[0]?.c).toBe(1);
    }
    finally {
      db.close();
    }
  });
});
