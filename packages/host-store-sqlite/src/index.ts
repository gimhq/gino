// @gino/host-store-sqlite — the public barrel (docs/host-sdk-design.md §8.2).
//
// FROZEN as of P3. This list is the package's API: an explicit allow-list, no
// wildcards, so the surface cannot grow by accident when a module gains an
// export. `tests/unit/api-surface.test.ts` snapshots it — adding or removing a
// name here fails that test until the snapshot is updated deliberately.
//
// What belongs here: the driver-agnostic database opener, the driver seam the
// `./bun` and `./node` entries implement, the committed migration set with its
// runner, and `sqliteStorage` + the per-table repositories it composes.
//
// What does NOT: drizzle ROW types. §5 step 8 keeps them internal, which is
// why host-core hand-mirrors the rows in `ports/rows.ts` rather than importing
// them from here. What crosses this entry is the database HANDLE
// (`AppDatabase`, drizzle-typed because `sqliteStorage` queries through it),
// never a row. The drizzle tables themselves live behind the separate
// `./schema` subpath — the deliberate, single escape hatch for an embedder
// authoring its own migrations.

// ─── The database (driver-agnostic; the drivers are `./bun` and `./node`) ───
export { openDatabase } from "./db";
export type { OpenDatabaseOptions } from "./db";
export type { AppDatabase, DrizzleSqliteDatabase, SqliteDriver, SqliteDriverHandle } from "./driver";

// ─── Migrations — the committed embedded set and the runner over it ───
export { embeddedMigrations } from "./embedded-migrations";
export { runMigrations } from "./migrate";

// ─── The `Storage` port implementation (§8.2) ───
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
