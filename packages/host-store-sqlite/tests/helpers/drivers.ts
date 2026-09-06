import type { SqliteDriver } from "../../src/driver";
import process from "node:process";
import { bunSqliteDriver } from "../../src/drivers/bun";
import { nodeSqliteDriver } from "../../src/drivers/node";

/**
 * The drivers the open / migrate / PRAGMA tests loop over. `better-sqlite3`
 * (N-API) currently aborts Bun 1.4.0 with a NAPI fatal error inside
 * `new Database(...)`, which would take the whole `bun test` process down, so
 * the node driver is opt-in here (`GINO_TEST_NODE_DRIVER=1`) until Bun can load
 * it; its Node-side proof is `tests/node/smoke.mjs`, run by `node` against
 * `dist/` as part of `check`.
 */
export const nodeDriverEnabled = process.env.GINO_TEST_NODE_DRIVER === "1";

export const testDrivers: readonly SqliteDriver[] = nodeDriverEnabled
  ? [bunSqliteDriver, nodeSqliteDriver]
  : [bunSqliteDriver];
