// @gino/host-store-sqlite — the drizzle + bun:sqlite `Storage` adapter
// (docs/host-sdk-design.md §8.2): the database handle + migrator, the schema,
// and the repositories over it.
export { acquireMigrationLock, createDb, materializeEmbeddedMigrations, recordDbVersion, runMigrations, snapshotOnVersionChange } from "./db";
export type { AppDatabase } from "./db";
export { embeddedMigrations } from "./embedded-migrations";
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
