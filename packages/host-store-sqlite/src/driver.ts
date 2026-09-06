import type { BaseSQLiteDatabase } from "drizzle-orm/sqlite-core";
import type * as schema from "./schema";

/** The drizzle handle every driver yields: any synchronous sqlite driver over the host schema. */
export type DrizzleSqliteDatabase = BaseSQLiteDatabase<"sync", unknown, typeof schema>;

/** What `openDatabase` gets back from a driver: the drizzle handle plus the raw hooks it needs. */
export interface SqliteDriverHandle {
  readonly db: DrizzleSqliteDatabase;
  /** Run one or more raw SQL statements (PRAGMAs, checkpoints); no result. */
  readonly exec: (sql: string) => void;
  readonly close: () => void;
}

/**
 * The driver seam (docs/host-sdk-design.md §3.4): `openDatabase` is written
 * once against this interface, and a driver only knows how to open a path on
 * a concrete sqlite binding and wrap it in drizzle. Shipped drivers:
 * `bunSqliteDriver` (`@gino/host-store-sqlite/bun`) and `nodeSqliteDriver`
 * (`@gino/host-store-sqlite/node`).
 */
export interface SqliteDriver {
  readonly name: string;
  readonly open: (path: string) => SqliteDriverHandle;
}

/** The open host database: the drizzle handle augmented with `close()` and `checkpoint()`. */
export type AppDatabase = DrizzleSqliteDatabase & { close: () => void; checkpoint: () => void };
