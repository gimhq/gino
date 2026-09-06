// The port conformance suite against the better-sqlite3 driver under plain
// Node (`node --test`), since Bun 1.4.0 cannot load better-sqlite3 (see
// tests/helpers/drivers.ts). Mirrors tests/conformance/sqlite-bun.test.ts:
// `:memory:` per test, foreign keys off, filesystem BlobStore in a throwaway
// directory. Resolves the packages through their `exports` maps (`dist/`), so
// it runs after `build`.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, test } from "node:test";
import { describeStorageConformance } from "@gino/host-core/testing";
import { openDatabase, sqliteStorage } from "@gino/host-store-sqlite";
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node";
import { sql } from "drizzle-orm";
import { expect } from "expect";

describeStorageConformance({
  name: "sqlite-node",
  open: () => {
    const db = openDatabase({ driver: nodeSqliteDriver, path: ":memory:" });
    // The spec exercises each repo in isolation (an outbox row for an agent
    // that was never enrolled, a run for a task never created), so the
    // schema's foreign keys are switched off for the conformance database.
    db.run(sql`PRAGMA foreign_keys = OFF`);
    const storeDir = mkdtempSync(join(tmpdir(), "gino-conformance-store-"));
    const storage = sqliteStorage(db, { storeDir });
    return {
      storage,
      // A ref `<name>/<sha256[:12]>` is the file `<storeDir>/<name>/<sha256[:12]>`.
      tamperBlob: (ref: string) => writeFileSync(join(storeDir, ref), "tampered bytes"),
      close: () => {
        db.close();
        rmSync(storeDir, { recursive: true, force: true });
      },
    };
  },
}, { describe, test, beforeEach, afterEach, expect });
