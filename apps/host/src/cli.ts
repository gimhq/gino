import { existsSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import { embeddedMigrations } from "@gino/host-store-sqlite";
import { Database } from "bun:sqlite";
import cac from "cac";
import { consola } from "consola";
import { BUILD_INFO } from "./build-info";
import { DATA_DIR } from "./dirs";

const migrateLog = consola.withTag("migrate");

/**
 * Lightweight CLI dispatcher built on `cac`. Handles non-bootstrap
 * subcommands (version, healthcheck, migrate --check) so the same binary can
 * run as `gino` (boot the server) and `gino healthcheck` (in-process probe —
 * no curl/wget required in the image).
 *
 * Returns the requested exit code, or `null` when no subcommand matched and
 * the caller should fall through to the normal boot path.
 */
export async function dispatchCliSubcommand(argv: readonly string[]): Promise<number | null> {
  const cli = cac("gino");

  let exitCode: number | null = null;

  cli
    .command("healthcheck", "Run an in-process probe against /api/health")
    .action(async () => {
      exitCode = await runHealthcheck();
    });

  cli
    .command("migrate", "Migration utilities")
    .option("--check", "List pending migrations without applying them")
    .action((opts: { check?: boolean }) => {
      exitCode = runMigrateSubcommand(opts);
    });

  cli.help();
  cli.version(`${BUILD_INFO.version} (${BUILD_INFO.commit}) built ${BUILD_INFO.buildTime}`);

  let parsed;
  try {
    parsed = cli.parse([...argv], { run: false });
  }
  catch (err) {
    consola.error(err instanceof Error ? err.message : String(err));
    return 2;
  }

  if (parsed.options.help || parsed.options.version) {
    return 0;
  }

  if (!cli.matchedCommand) {
    return null;
  }

  try {
    await cli.runMatchedCommand();
  }
  catch (err) {
    consola.error(err instanceof Error ? err.message : String(err));
    return 2;
  }
  return exitCode;
}

async function runHealthcheck(): Promise<number> {
  const port = Number(process.env.GINO_PORT ?? "3000");
  const basePath = (process.env.GINO_BASE_PATH ?? "").replace(/^\/+|\/+$/g, "");
  const path = basePath ? `/${basePath}/api/health` : "/api/health";
  const url = `http://127.0.0.1:${port}${path}`;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(5_000) });
    return res.ok ? 0 : 1;
  }
  catch {
    return 1;
  }
}

function runMigrateSubcommand(opts: { check?: boolean }): number {
  if (!opts.check) {
    consola.error("Usage: gino migrate --check");
    return 2;
  }
  const pending = listPendingMigrations({ dbPath: resolveDbPath() });
  if (pending === null) {
    migrateLog.error("cannot read migrations (no embedded set and no drizzle/ folder).");
    return 2;
  }
  if (pending.length === 0) {
    migrateLog.success("no pending migrations.");
    return 0;
  }
  migrateLog.info(`${pending.length} pending migration(s):`);
  for (const m of pending)
    consola.log(`  - ${m}`);
  return 0;
}

// Same DB path resolution as `loadConfig` (src/config.ts) without pulling in
// full config validation: GINO_DB_PATH (or the schema default), relative paths
// resolved against DATA_DIR.
function resolveDbPath(): string {
  const p = process.env.GINO_DB_PATH ?? "data/db/gino.db";
  return p.startsWith("/") ? p : resolve(DATA_DIR, p);
}

interface JournalEntry { tag: string; when: number }

/**
 * List the migrations the next boot would actually apply:
 * bundled journal entries whose `when` is newer than the last applied
 * `created_at` row in the target DB's `__drizzle_migrations` table — the
 * exact predicate drizzle's bun-sqlite migrator uses. The DB is opened
 * read-only; a missing DB file or journal table means nothing has been
 * applied yet, so every bundled migration is pending. Returns `null` when no
 * migration source can be read.
 */
export function listPendingMigrations(opts: { dbPath: string; migrations?: ReadonlyMap<string, string> }): string[] | null {
  const entries = readJournalEntries(opts.migrations ?? embeddedMigrations);
  if (entries === null)
    return null;
  const lastApplied = readLastAppliedMillis(opts.dbPath);
  return entries
    .filter(e => e.when > lastApplied)
    .map(e => e.tag);
}

// Migration source mirrors `runMigrations` (packages/host-store-sqlite/src/migrate.ts)
// exactly: the committed embedded set is the only source, in every
// build. Tests inject their own map through the same seam `runMigrations` takes.
function readJournalEntries(migrations: ReadonlyMap<string, string>): JournalEntry[] | null {
  const raw = migrations.get("meta/_journal.json");
  if (!raw)
    return null;
  return parseJournal(raw, tag => migrations.has(`${tag}.sql`));
}

function parseJournal(raw: string, hasSql: (tag: string) => boolean): JournalEntry[] | null {
  try {
    const journal = JSON.parse(raw) as { entries?: { tag: string; when: number }[] };
    return (journal.entries ?? [])
      .filter(e => hasSql(e.tag))
      .map(e => ({ tag: e.tag, when: e.when }));
  }
  catch {
    return null;
  }
}

function readLastAppliedMillis(dbPath: string): number {
  if (!existsSync(dbPath))
    return 0;
  let db: Database;
  try {
    db = new Database(dbPath, { readonly: true });
  }
  catch {
    return 0;
  }
  try {
    const row = db
      .query("SELECT max(created_at) AS m FROM __drizzle_migrations")
      .get() as { m: number | string | null } | null;
    return Number(row?.m ?? 0);
  }
  catch {
    // Table absent — the DB predates the migrator or is empty.
    return 0;
  }
  finally {
    db.close();
  }
}
