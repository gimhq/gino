// @gino/host-store-sqlite — the drizzle + sqlite `Storage` adapter
// (docs/host-sdk-design.md §8.2): the database opener + migrator (driver
// agnostic; drivers live under `./bun` and `./node`), the schema, and the
// repositories over it.
export { acquireMigrationLock, openDatabase, recordDbVersion, snapshotOnVersionChange } from "./db";
export type { OpenDatabaseOptions } from "./db";
export type { AppDatabase, DrizzleSqliteDatabase, SqliteDriver, SqliteDriverHandle } from "./driver";
export { embeddedMigrations } from "./embedded-migrations";
export { readEmbeddedMigrations, runMigrations } from "./migrate";
export * as schema from "./schema";
export {
  sqliteAgentRepo,
  sqliteAttachmentRepo,
  sqliteChannelRepo,
  sqliteConfigRepo,
  sqliteCronMirrorRepo,
  sqliteOutboxRepo,
  sqliteStorage,
  sqliteTaskRepo,
  sqliteThreadRunRepo,
  sqliteTranscriptRepo,
} from "./store";
export type { SqliteStorageOptions } from "./store";
