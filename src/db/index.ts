import { closeSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, unlinkSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { migrate } from "drizzle-orm/bun-sqlite/migrator";
import { BUILD_INFO } from "../build-info";
import { IS_COMPILED, ROOT_DIR } from "../dirs";
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
export function createDb(path: string, opts: { migrationLockTimeoutMs?: number } = {}): AppDatabase {
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

  // Pre-migration snapshot (host-upgrade-design.md §7): on a version change, back
  // up the existing DB BEFORE the new code migrates it, so a rollback to the
  // previous version still finds a compatible schema. Then migrate, then record
  // the version that owns the on-disk schema. The whole window is serialized
  // across processes (HOST-M9-14): a concurrent start waits for the holder to
  // finish migrating, then proceeds against the already-migrated file.
  try {
    const releaseLock = path === ":memory:"
      ? null
      : acquireMigrationLock(path, opts.migrationLockTimeoutMs);
    try {
      snapshotOnVersionChange(sqlite, path, BUILD_INFO.version);
      runMigrations(db);
      recordDbVersion(path, BUILD_INFO.version);
    }
    finally {
      releaseLock?.();
    }
  }
  catch (err) {
    sqlite.close();
    throw err;
  }

  return Object.assign(db, {
    close: () => sqlite.close(),
    // Fold the WAL into the main DB file. Called before lode cuts over to a new
    // version (src/index.ts prepare handshake) so the next version starts from a
    // consolidated file, not a half-flushed WAL.
    checkpoint: () => {
      sqlite.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    },
  });
}

const VERSION_MARKER = ".gino-version";

/**
 * Snapshot the DB to `backups/<prev>.db` when the running `version` differs from
 * the version that last migrated it (recorded in `<dbDir>/.gino-version`) — the
 * cheap insurance a strict expand-contract migration policy can't fully cover.
 * No-op for `:memory:`, the dev placeholder version, a first run (no marker), or
 * an unchanged version. Best-effort: a failure logs and continues (the running
 * version is still good; only the rollback safety net is missed).
 */
export function snapshotOnVersionChange(sqlite: Database, dbPath: string, version: string): void {
  if (dbPath === ":memory:" || version === "0.0.0-dev")
    return;
  const dir = dirname(dbPath);
  let prev: string | null = null;
  try {
    prev = readFileSync(resolve(dir, VERSION_MARKER), "utf8").trim() || null;
  }
  catch {}
  if (!prev || prev === version || !existsSync(dbPath))
    return;
  try {
    // TRUNCATE checkpoint folds the WAL into the main file, so a plain copy is a
    // consistent point-in-time snapshot of the pre-migration schema.
    sqlite.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    const backups = resolve(dir, "backups");
    mkdirSync(backups, { recursive: true });
    copyFileSync(dbPath, resolve(backups, `${prev}.db`));
    // eslint-disable-next-line no-console
    console.info(`[db] snapshot ${prev} → backups/${prev}.db before migrating to ${version}`);
  }
  catch (err) {
    console.warn("[db] pre-migration snapshot failed (continuing):", err);
  }
}

/** Record the version that now owns the migrated on-disk schema (for next boot). */
export function recordDbVersion(dbPath: string, version: string): void {
  if (dbPath === ":memory:" || version === "0.0.0-dev")
    return;
  try {
    writeFileSync(resolve(dirname(dbPath), VERSION_MARKER), `${version}\n`);
  }
  catch (err) {
    console.warn("[db] version marker write failed:", err);
  }
}

type DrizzleDb = ReturnType<typeof drizzle<typeof schema>>;

/**
 * Cross-process guard around the snapshot + migrate window (HOST-M9-14): an
 * exclusively-created (`O_EXCL`) pidfile beside the DB. A SQLite
 * `BEGIN EXCLUSIVE` on a sidecar file would be the natural choice, but
 * bun:sqlite's VFS does not place OS file locks, so its transactions are
 * invisible to other processes — verified empirically (a concurrent `sqlite3`
 * CLI writes straight through a held `BEGIN EXCLUSIVE`).
 *
 * A concurrent starter polls up to `busyTimeoutMs` for the holder to finish,
 * then fails with a clear error. A lock left behind by a crashed holder is
 * detected via pid liveness and reclaimed immediately.
 */
export function acquireMigrationLock(dbPath: string, busyTimeoutMs = 30_000): () => void {
  const lockPath = `${dbPath}.lock`;
  const deadline = Date.now() + busyTimeoutMs;
  while (true) {
    try {
      const fd = openSync(lockPath, "wx", 0o600);
      writeSync(fd, `${process.pid}\n`);
      closeSync(fd);
      return () => {
        try {
          unlinkSync(lockPath);
        }
        catch {}
      };
    }
    catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST")
        throw err;
    }
    // Lock exists. Reclaim it right away when the holder is dead; otherwise
    // wait for it to finish, up to the deadline.
    const holderPid = readLockPid(lockPath);
    if (holderPid !== null && !isProcessAlive(holderPid)) {
      try {
        unlinkSync(lockPath);
      }
      catch {}
      continue;
    }
    if (Date.now() >= deadline) {
      throw new Error(
        `another gino process holds the migration lock (${lockPath}`
        + `${holderPid ? `, pid ${holderPid}` : ""}); wait for it to finish `
        + `starting up, then retry — or delete the file if that process is gone`,
      );
    }
    Bun.sleepSync(Math.min(100, deadline - Date.now()));
  }
}

function readLockPid(lockPath: string): number | null {
  try {
    const pid = Number.parseInt(readFileSync(lockPath, "utf8").trim(), 10);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  }
  catch {
    return null;
  }
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  }
  catch (err) {
    // EPERM: the pid exists but belongs to another user — still alive.
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

/**
 * Materialize an embedded-migrations map into a fresh private temp directory
 * so drizzle's folder-based migrator can read it. `mkdtemp` yields an
 * unpredictable, owner-only (0700) directory, so a local attacker cannot
 * pre-create it or swap contents between write and migrate (HOST-M9-06).
 * The caller removes the directory when done.
 */
export function materializeEmbeddedMigrations(migrations: ReadonlyMap<string, string>): string {
  const dir = mkdtempSync(join(tmpdir(), "gino-migrations-"));
  for (const [name, content] of migrations) {
    const filePath = resolve(dir, name);
    mkdirSync(dirname(filePath), { recursive: true });
    writeFileSync(filePath, content);
  }
  return dir;
}

/**
 * Apply migrations from the right source. Packaged builds — the compiled
 * binary (`/$bunfs`) or the `bun run package` bundle, both of which carry a
 * populated `embeddedMigrations` map — use the embedded set exclusively, so a
 * stale, foreign, or attacker-planted `drizzle/` tree in the working
 * directory can never reach the database (HOST-M9-13). Only a dev/test run
 * (empty stub map) reads `<ROOT_DIR>/drizzle` from disk.
 *
 * Exported for tests; runtime callers use the defaults.
 */
export function runMigrations(
  db: DrizzleDb,
  opts: { compiled?: boolean; embedded?: ReadonlyMap<string, string>; rootDir?: string } = {},
): void {
  const compiled = opts.compiled ?? IS_COMPILED;
  const embedded = opts.embedded ?? embeddedMigrations;
  const rootDir = opts.rootDir ?? ROOT_DIR;

  if (!compiled && embedded.size === 0) {
    const fsMigrationsFolder = resolve(rootDir, "drizzle");
    if (existsSync(resolve(fsMigrationsFolder, "meta/_journal.json"))) {
      migrate(db, { migrationsFolder: fsMigrationsFolder });
      return;
    }
  }

  // Package path: `scripts/package.ts` writes the migration files into
  // `embedded-migrations.ts` before invoking `bun build`, then
  // restores the stub. A binary built outside that step — or from a
  // worktree where the stub was restored but `drizzle/` was excluded — would
  // otherwise boot with an empty map and crash later on its first DB write.
  // Fail fast with a concrete fix.
  if (embedded.size === 0) {
    throw new Error(
      "No migrations available: filesystem drizzle/ folder is missing and the "
      + "compiled binary has no embedded migrations. This binary was built "
      + "outside `bun run package` (which populates embedded-migrations.ts at "
      + "build time) or against a worktree without `drizzle/`. Run "
      + "`bun run package` to rebuild, or restore the repo's drizzle/ folder "
      + "for dev runs.",
    );
  }

  // Migration journal sentinel — when present in the embedded map it proves
  // the compile step finished writing the full set, not just a truncated
  // prefix. Drizzle's migrator reads it first, so a missing journal here
  // would silently no-op.
  if (!embedded.has("meta/_journal.json")) {
    throw new Error(
      "Embedded migrations are corrupt: meta/_journal.json missing. "
      + "Rebuild the bundle with `bun run package`.",
    );
  }

  const tmpMigrations = materializeEmbeddedMigrations(embedded);
  try {
    migrate(db, { migrationsFolder: tmpMigrations });
  }
  finally {
    rmSync(tmpMigrations, { recursive: true, force: true });
  }
}

export type AppDatabase = DrizzleDb & { close: () => void; checkpoint: () => void };
