import type { AppDatabase, SqliteDriver } from "./driver";
import { closeSync, copyFileSync, existsSync, mkdirSync, openSync, readFileSync, unlinkSync, writeFileSync, writeSync } from "node:fs";
import { dirname, resolve } from "node:path";
import process from "node:process";
import { runMigrations } from "./migrate";

/** The placeholder version of a dev run; the snapshot + marker are no-ops for it. */
const DEV_VERSION = "0.0.0-dev";

export interface OpenDatabaseOptions {
  /** The sqlite binding to open the file with (`bunSqliteDriver`, `nodeSqliteDriver`). */
  readonly driver: SqliteDriver;
  /** Database file path; `:memory:` for an ephemeral database. */
  readonly path: string;
  /** The running application version that owns the on-disk schema; defaults to the dev placeholder. */
  readonly version?: string;
  /** How long a concurrent starter waits for the migration lock holder (default 30s). */
  readonly migrationLockTimeoutMs?: number;
}

/**
 * Open (or create) the v2 host database through `driver`, apply the WAL /
 * foreign-keys / busy-timeout PRAGMAs, run migrations, and return the drizzle
 * handle augmented with `close()` and `checkpoint()`. There is no
 * encryption-at-rest: the host is the credential proxy, not the keystore (see
 * docs/host-v2-design.md).
 *
 * `:memory:` is passed through untouched for ephemeral test databases.
 *
 * `version` is the running application version that owns the on-disk schema
 * (pre-migration snapshot + version marker); it defaults to the dev
 * placeholder, for which both are no-ops.
 */
export function openDatabase(opts: OpenDatabaseOptions): AppDatabase {
  const { driver, path } = opts;
  if (path !== ":memory:") {
    const dir = dirname(path);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  const sqlite = driver.open(path);

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

  const db = sqlite.db;

  // Pre-migration snapshot (host-upgrade-design.md §7): on a version change, back
  // up the existing DB BEFORE the new code migrates it, so a rollback to the
  // previous version still finds a compatible schema. Then migrate, then record
  // the version that owns the on-disk schema. The whole window is serialized
  // across processes: a concurrent start waits for the holder to
  // finish migrating, then proceeds against the already-migrated file.
  const version = opts.version ?? DEV_VERSION;
  try {
    const releaseLock = path === ":memory:"
      ? null
      : acquireMigrationLock(path, opts.migrationLockTimeoutMs);
    try {
      snapshotOnVersionChange(sqlite, path, version);
      runMigrations(db);
      recordDbVersion(path, version);
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
export function snapshotOnVersionChange(sqlite: { exec: (sql: string) => void }, dbPath: string, version: string): void {
  if (dbPath === ":memory:" || version === DEV_VERSION)
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
  if (dbPath === ":memory:" || version === DEV_VERSION)
    return;
  try {
    writeFileSync(resolve(dirname(dbPath), VERSION_MARKER), `${version}\n`);
  }
  catch (err) {
    console.warn("[db] version marker write failed:", err);
  }
}

/**
 * Cross-process guard around the snapshot + migrate window: an
 * exclusively-created (`O_EXCL`) pidfile beside the DB. A SQLite
 * `BEGIN EXCLUSIVE` on a sidecar file would be the natural choice, but
 * the Bun sqlite binding's VFS does not place OS file locks, so its transactions are
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
    // Synchronous sleep without a Bun-only API: block on a never-notified atomic.
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Math.min(100, deadline - Date.now()));
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
