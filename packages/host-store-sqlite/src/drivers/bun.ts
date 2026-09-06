import type { AppDatabase, SqliteDriver } from "../driver";
import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { openDatabase } from "../db";
import * as schema from "../schema";

/** `bun:sqlite` via `drizzle-orm/bun-sqlite` — the driver the Bun host app runs on. */
export const bunSqliteDriver: SqliteDriver = {
  name: "bun:sqlite",
  open(path) {
    const sqlite = new Database(path, { create: true });
    return {
      db: drizzle(sqlite, { schema }),
      exec: sql => sqlite.exec(sql),
      close: () => sqlite.close(),
    };
  },
};

/**
 * Open (or create) the host database on `bun:sqlite`. Thin wrapper over
 * `openDatabase` for the Bun app and its tests; see `openDatabase` for the
 * semantics of `opts`.
 */
export function createDb(path: string, opts: { migrationLockTimeoutMs?: number; version?: string } = {}): AppDatabase {
  return openDatabase({ driver: bunSqliteDriver, path, ...opts });
}
