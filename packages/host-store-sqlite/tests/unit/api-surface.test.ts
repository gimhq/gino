// API SURFACE — the FROZEN public surface of `@gino/host-store-sqlite` (P3).
//
// Four entries, snapshotted separately: `.` (the driver-agnostic adapter),
// `./schema` (the drizzle tables), `./bun` and `./node` (the two drivers).
// None of the lists changes by accident.
//
// Two mechanisms, because a TypeScript type does not exist at runtime:
//
//   * VALUES — `Object.keys(await import(…))` against the literal lists below.
//     Catches an added export as well as a removed one.
//   * TYPES — the `PromisedPublicTypes` tuple at the bottom. It names every
//     type-only export of `.`, so `tsc --noEmit` (part of `bun run check`)
//     fails the moment one is removed or renamed. Nothing can enumerate types
//     at runtime, so an ADDED type is not caught here — it is caught by review
//     of `src/index.ts`, which is an explicit allow-list with no wildcards.
//     `./schema` has no tuple: its type-only exports are the per-table
//     `$inferSelect` / `$inferInsert` aliases, mechanically derived from the
//     table list already frozen by the value snapshot.
//
// The drizzle boundary this file guards (docs/host-sdk-design.md §5 step 8):
// drizzle ROW types stay off `.` — host-core hand-mirrors them in
// `ports/rows.ts` precisely so they do not have to cross. `./schema` is the
// one deliberate escape hatch, for an embedder authoring its own migrations.
//
// TO CHANGE THE SURFACE: edit `src/index.ts`, then update the list here in the
// same commit. That is the point — the diff is the record of an API change.
import type * as PublicApi from "../../src/index";
import { describe, expect, it } from "bun:test";
import * as bunEntry from "../../src/bun";
import * as index from "../../src/index";
import * as nodeEntry from "../../src/node";
import * as schema from "../../src/schema";

/** Runtime (value) exports of `@gino/host-store-sqlite` — the frozen public entry. */
const PUBLIC_RUNTIME_EXPORTS: readonly string[] = [
  "embeddedMigrations",
  "openDatabase",
  "runMigrations",
  "sqliteAgentRepo",
  "sqliteAttachmentRepo",
  "sqliteChannelRepo",
  "sqliteConfigRepo",
  "sqliteCronMirrorRepo",
  "sqliteOutboxRepo",
  "sqliteStorage",
  "sqliteTaskRepo",
  "sqliteThreadRunRepo",
  "sqliteTranscriptRepo",
];

/**
 * Runtime (value) exports of `@gino/host-store-sqlite/schema` — the drizzle
 * tables. This IS the host's data model, so the list doubles as the record of
 * which tables exist; a new table is an API change to this entry.
 */
const SCHEMA_RUNTIME_EXPORTS: readonly string[] = [
  "SCHEMA_VERSION",
  "agentGrants",
  "agents",
  "attachments",
  "channelBindings",
  "channelCursors",
  "channels",
  "configArtifacts",
  "configEntries",
  "crons",
  "messages",
  "outbox",
  "pendingLaunches",
  "routerState",
  "tasks",
  "threadRuns",
];

/** Runtime (value) exports of `@gino/host-store-sqlite/bun` — the `bun:sqlite` driver. */
const BUN_RUNTIME_EXPORTS: readonly string[] = [
  "bunSqliteDriver",
  "createDb",
];

/** Runtime (value) exports of `@gino/host-store-sqlite/node` — the `better-sqlite3` driver. */
const NODE_RUNTIME_EXPORTS: readonly string[] = [
  "nodeSqliteDriver",
];

function assertSurface(entry: string, actual: readonly string[], expected: readonly string[]): void {
  const added = actual.filter(name => !expected.includes(name));
  const removed = expected.filter(name => !actual.includes(name));
  if (added.length > 0 || removed.length > 0) {
    throw new Error([
      `The ${entry} export surface changed.`,
      added.length > 0 ? `  added:   ${added.join(", ")}` : null,
      removed.length > 0 ? `  removed: ${removed.join(", ")}` : null,
      "",
      "This is a snapshot of a FROZEN API, not an incidental assertion.",
      "If the change is intended, update the list in this file in the same",
      "commit as the change to the entry point and say so in the commit",
      "message. If it is not intended, you have leaked an internal symbol",
      "into a published entry point — take it back out of the barrel.",
    ].filter(line => line !== null).join("\n"));
  }
  expect(actual).toEqual([...expected]);
}

describe("api surface", () => {
  it("@gino/host-store-sqlite exports exactly the frozen value list", () => {
    assertSurface("@gino/host-store-sqlite", Object.keys(index).sort(), PUBLIC_RUNTIME_EXPORTS);
  });

  it("@gino/host-store-sqlite/schema exports exactly the frozen table list", () => {
    assertSurface("@gino/host-store-sqlite/schema", Object.keys(schema).sort(), SCHEMA_RUNTIME_EXPORTS);
  });

  it("@gino/host-store-sqlite/bun exports exactly the frozen value list", () => {
    assertSurface("@gino/host-store-sqlite/bun", Object.keys(bunEntry).sort(), BUN_RUNTIME_EXPORTS);
  });

  it("@gino/host-store-sqlite/node exports exactly the frozen value list", () => {
    assertSurface("@gino/host-store-sqlite/node", Object.keys(nodeEntry).sort(), NODE_RUNTIME_EXPORTS);
  });
});

/**
 * The TYPE half of the snapshot — one member per type-only export of `.`.
 * Removing or renaming any of them is a `tsc` error, which `bun run check`
 * surfaces before the value snapshot above ever runs. Keep it sorted.
 */
export type PromisedPublicTypes = [
  PublicApi.AppDatabase,
  PublicApi.DrizzleSqliteDatabase,
  PublicApi.OpenDatabaseOptions,
  PublicApi.SqliteDriver,
  PublicApi.SqliteDriverHandle,
  PublicApi.SqliteStorageOptions,
];
