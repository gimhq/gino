import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { recordDbVersion, snapshotOnVersionChange } from "../../src/db";
import { testDrivers } from "../helpers/drivers";

// Pre-migration DB snapshot (docs/host-upgrade-design.md §7): on a version change
// the DB is copied to backups/<prev>.db before the new code migrates it.
for (const driver of testDrivers) {
  describe(`db pre-migration snapshot (§7, ${driver.name})`, () => {
    let dir: string;
    let dbPath: string;

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), "gino-dbsnap-"));
      dbPath = join(dir, "gino.db");
      const db = driver.open(dbPath);
      db.exec("PRAGMA journal_mode = WAL");
      db.exec("CREATE TABLE t (id INTEGER)");
      db.exec("INSERT INTO t VALUES (1)");
      db.close();
    });

    afterEach(() => rmSync(dir, { recursive: true, force: true }));

    it("snapshots to backups/<prev>.db on a version change", () => {
      recordDbVersion(dbPath, "1.0.0");
      const db = driver.open(dbPath);
      snapshotOnVersionChange(db, dbPath, "1.1.0");
      db.close();
      expect(existsSync(join(dir, "backups", "1.0.0.db"))).toBe(true);
    });

    it("does not snapshot when the version is unchanged", () => {
      recordDbVersion(dbPath, "1.0.0");
      const db = driver.open(dbPath);
      snapshotOnVersionChange(db, dbPath, "1.0.0");
      db.close();
      expect(existsSync(join(dir, "backups"))).toBe(false);
    });

    it("does not snapshot on a first run (no marker yet)", () => {
      const db = driver.open(dbPath);
      snapshotOnVersionChange(db, dbPath, "1.1.0");
      db.close();
      expect(existsSync(join(dir, "backups"))).toBe(false);
    });

    it("is a no-op for the dev placeholder version", () => {
      recordDbVersion(dbPath, "0.0.0-dev");
      expect(existsSync(join(dir, ".gino-version"))).toBe(false);
      const db = driver.open(dbPath);
      snapshotOnVersionChange(db, dbPath, "0.0.0-dev");
      db.close();
      expect(existsSync(join(dir, "backups"))).toBe(false);
    });

    it("records the version marker", () => {
      recordDbVersion(dbPath, "2.0.0");
      expect(readFileSync(join(dir, ".gino-version"), "utf8").trim()).toBe("2.0.0");
    });
  });
}
