import type { DrizzleSqliteDatabase } from "./driver";
import { createHash } from "node:crypto";
import { sql } from "drizzle-orm";
import { embeddedMigrations } from "./embedded-migrations";

interface JournalEntry {
  readonly tag: string;
  readonly when: number;
}

interface Migration {
  readonly statements: readonly string[];
  readonly folderMillis: number;
  readonly hash: string;
}

/**
 * Turn an embedded `drizzle/` folder (`meta/_journal.json` + `<tag>.sql`,
 * keyed by path relative to the folder) into the ordered migration list —
 * the same shape drizzle's own `readMigrationFiles` produces: `hash` is the
 * sha256 hex of the whole file, `folderMillis` the journal `when`, and the
 * statements the file split on `--> statement-breakpoint`.
 */
export function readEmbeddedMigrations(migrations: ReadonlyMap<string, string>): Migration[] {
  // Migration journal sentinel — when present in the map it proves the
  // generated set is complete, not a truncated prefix.
  const journal = migrations.get("meta/_journal.json");
  if (!journal) {
    throw new Error(
      "Embedded migrations are corrupt: meta/_journal.json missing. "
      + "Regenerate src/embedded-migrations.ts with `bun run gen:migrations`.",
    );
  }
  const entries = (JSON.parse(journal) as { entries: JournalEntry[] }).entries;
  return entries.map((entry) => {
    const query = migrations.get(`${entry.tag}.sql`);
    if (query === undefined)
      throw new Error(`Embedded migrations are corrupt: ${entry.tag}.sql missing (listed in meta/_journal.json)`);
    return {
      statements: query.split("--> statement-breakpoint"),
      folderMillis: entry.when,
      hash: createHash("sha256").update(query).digest("hex"),
    };
  });
}

/**
 * Apply the embedded migrations to `db`, driver-agnostic and byte-for-byte
 * compatible with drizzle's sqlite migrator (D9): the same
 * `__drizzle_migrations` journal table, the same `hash` / `created_at`
 * values, the same "apply every entry newer than the last recorded one"
 * rule — so a database migrated by either runner is picked up by the other.
 * Each pending entry (its statements + its journal row) commits in one
 * transaction.
 *
 * Exported for tests; runtime callers use the default map.
 */
export function runMigrations(db: DrizzleSqliteDatabase, migrations: ReadonlyMap<string, string> = embeddedMigrations): void {
  const pending = readEmbeddedMigrations(migrations);

  // Verbatim drizzle's statement (tabs included) so `sqlite_master.sql` for the
  // journal table is identical whichever runner created it.
  /* eslint-disable style/no-tabs */
  db.run(sql`
			CREATE TABLE IF NOT EXISTS "__drizzle_migrations" (
				id SERIAL PRIMARY KEY,
				hash text NOT NULL,
				created_at numeric
			)
		`);
  /* eslint-enable style/no-tabs */
  // `.all()` yields row objects on every sync driver (`.get()` yields value arrays on the Bun driver).
  const last = db.all<{ id: number | null; hash: string; created_at: number | string }>(
    sql`SELECT id, hash, created_at FROM "__drizzle_migrations" ORDER BY created_at DESC LIMIT 1`,
  )[0];
  for (const migration of pending) {
    if (last && Number(last.created_at) >= migration.folderMillis)
      continue;
    db.transaction((tx) => {
      for (const stmt of migration.statements) {
        tx.run(sql.raw(stmt));
      }
      tx.run(sql`INSERT INTO "__drizzle_migrations" ("hash", "created_at") VALUES(${migration.hash}, ${migration.folderMillis})`);
    });
  }
}
