import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import process from "node:process";
import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { ROOT_DIR } from "../dirs";
import { embeddedMigrations } from "./embedded-migrations";
import * as schema from "./schema";

/**
 * Open (or create) the v2 host database on `bun:sqlite` via
 * `drizzle-orm/bun-sqlite`, apply the WAL / foreign-keys / busy-timeout
 * PRAGMAs, run migrations, and return the drizzle handle augmented with
 * `close()`. There is no encryption-at-rest: the host is the credential
 * proxy, not the keystore (see docs/host-v2-design.md).
 *
 * `:memory:` is passed through untouched for ephemeral test databases.
 */
export function createDb(path: string): AppDatabase {
  if (path !== ":memory:") {
    const dir = dirname(path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  const sqlite = new Database(path, { create: true });

  sqlite.exec("PRAGMA journal_mode = WAL");
  sqlite.exec("PRAGMA foreign_keys = ON");
  sqlite.exec("PRAGMA busy_timeout = 5000");

  // Performance / footprint tuning. Best-effort — a PRAGMA that a given
  // build or platform rejects should not abort the whole bootstrap.
  for (const pragma of [
    "PRAGMA synchronous = NORMAL",
    "PRAGMA cache_size = -65536",
    "PRAGMA mmap_size = 268435456",
    "PRAGMA temp_store = MEMORY",
  ]) {
    try {
      sqlite.exec(pragma);
    }
    catch (err) {
      // eslint-disable-next-line no-console
      console.debug(`[db] ${pragma} skipped:`, err);
    }
  }

  const db = drizzle(sqlite, { schema });

  runMigrations(db);

  return Object.assign(db, {
    close: () => sqlite.close(),
  });
}

type DrizzleDb = ReturnType<typeof drizzle<typeof schema>>;

function runMigrations(db: DrizzleDb): void {
  const fsMigrationsFolder = resolve(ROOT_DIR, "drizzle");
  const journalPath = resolve(fsMigrationsFolder, "meta/_journal.json");

  if (existsSync(journalPath)) {
    migrate(db, { migrationsFolder: fsMigrationsFolder });
    return;
  }

  // Package path: `scripts/package.ts` writes the migration files into
  // `embedded-migrations.ts` before invoking `bun build`, then
  // restores the stub. A binary built outside that step — or from a
  // worktree where the stub was restored but `drizzle/` was excluded — would
  // otherwise boot with an empty map and crash later on its first DB write.
  // Fail fast with a concrete fix.
  if (embeddedMigrations.size === 0) {
    throw new Error(
      "No migrations available: filesystem drizzle/ folder is missing and the "
      + "compiled binary has no embedded migrations. This binary was built "
      + "outside `bun run package` (which populates embedded-migrations.ts at "
      + "build time) or against a worktree without `drizzle/`. Run "
      + "`bun run package` to rebuild, or mount the project's drizzle/ folder.",
    );
  }

  // Migration journal sentinel — when present in the embedded map it proves
  // the compile step finished writing the full set, not just a truncated
  // prefix. Drizzle's migrator reads it first, so a missing journal here
  // would silently no-op.
  if (!embeddedMigrations.has("meta/_journal.json")) {
    throw new Error(
      "Embedded migrations are corrupt: meta/_journal.json missing. "
      + "Rebuild the bundle with `bun run package`.",
    );
  }

  const tmpMigrations = resolve(tmpdir(), `gino-migrations-${process.pid}`);
  try {
    mkdirSync(resolve(tmpMigrations, "meta"), { recursive: true });
    for (const [name, content] of embeddedMigrations) {
      const filePath = resolve(tmpMigrations, name);
      mkdirSync(dirname(filePath), { recursive: true });
      writeFileSync(filePath, content);
    }
    migrate(db, { migrationsFolder: tmpMigrations });
  }
  finally {
    rmSync(tmpMigrations, { recursive: true, force: true });
  }
}

export type AppDatabase = DrizzleDb & { close: () => void };
