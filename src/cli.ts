import { existsSync, readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import cac from "cac";
import { consola } from "consola";
import { BUILD_INFO } from "./build-info";
import { ROOT_DIR } from "./root";

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
  const pending = listFsPendingMigrations();
  if (pending === null) {
    migrateLog.error("cannot read drizzle/ folder; check that the binary has access to migrations.");
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

/**
 * Compare `drizzle/meta/_journal.json` against the migration `.sql` files on
 * disk. Does NOT open the DB — `migrate --check` is meant to run against a
 * snapshot or a locked environment. Returns `null` when the journal cannot be
 * read (compiled binary without filesystem access).
 */
function listFsPendingMigrations(): string[] | null {
  const fsMigrationsFolder = resolve(ROOT_DIR, "drizzle");
  const journalPath = resolve(fsMigrationsFolder, "meta/_journal.json");
  if (!existsSync(journalPath))
    return null;
  try {
    const journal = JSON.parse(readFileSync(journalPath, "utf-8")) as { entries: { tag: string }[] };
    const entries = journal.entries ?? [];
    const knownTags = new Set(
      readdirSync(fsMigrationsFolder)
        .filter(f => f.endsWith(".sql"))
        .map(f => f.replace(/\.sql$/, "")),
    );
    return entries
      .map(e => e.tag)
      .filter(tag => knownTags.has(tag));
  }
  catch {
    return null;
  }
}
