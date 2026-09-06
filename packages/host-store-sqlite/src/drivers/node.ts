import type { SqliteDriver } from "../driver";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import * as schema from "../schema";

/** `better-sqlite3` via `drizzle-orm/better-sqlite3` — the driver for Node consumers of the SDK. */
export const nodeSqliteDriver: SqliteDriver = {
  name: "better-sqlite3",
  open(path) {
    const sqlite = new Database(path);
    return {
      db: drizzle(sqlite, { schema }),
      exec: sql => sqlite.exec(sql),
      close: () => sqlite.close(),
    };
  },
};
