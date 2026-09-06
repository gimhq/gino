import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describeStorageConformance } from "@gino/host-core/testing";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { sql } from "drizzle-orm";
import { openDatabase } from "../../src/db";
import { nodeSqliteDriver } from "../../src/drivers/node";
import { sqliteStorage } from "../../src/store";
import { nodeDriverEnabled } from "../helpers/drivers";

// The same adapter over the `better-sqlite3` driver. Opt-in under Bun (see
// tests/helpers/drivers.ts): better-sqlite3 aborts the Bun 1.4.0 process on
// `new Database`, so without GINO_TEST_NODE_DRIVER=1 this suite is skipped and
// the node driver is proven under `node` by `tests/node/smoke.mts` and
// `tests/node/conformance.mts` (the same suite through `node --test`).
const describeNode = nodeDriverEnabled ? describe : describe.skip;

describeNode("sqlite-node driver (set GINO_TEST_NODE_DRIVER=1 to run under Bun)", () => {
  describeStorageConformance({
    name: "sqlite-node",
    open: () => {
      const db = openDatabase({ driver: nodeSqliteDriver, path: ":memory:" });
      db.run(sql`PRAGMA foreign_keys = OFF`);
      const storeDir = mkdtempSync(join(tmpdir(), "gino-conformance-store-"));
      const storage = sqliteStorage(db, { storeDir });
      return {
        storage,
        tamperBlob: ref => writeFileSync(join(storeDir, ref), "tampered bytes"),
        close: () => {
          db.close();
          rmSync(storeDir, { recursive: true, force: true });
        },
      };
    },
  }, { describe, test, beforeEach, afterEach, expect });
});
