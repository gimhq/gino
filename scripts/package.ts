#!/usr/bin/env bun
/* eslint-disable no-console */
/**
 * Package the host as a **lode** release asset, end to end:
 *   1. embed the drizzle migrations into `src/db/embedded-migrations.ts`,
 *   2. bundle to a single JS file `dist/gino.js` via `bun build --target bun`
 *      (NOT `--compile` — lode's `[runtime]` provides `bun`), injecting
 *      `BUILD_COMMIT`/`BUILD_TIME`/`BUILD_VERSION`, then restore the stub,
 *   3. emit a `lode/v1` `dist/manifest.json` (integrity sha256 + size + the
 *      launch command) and `dist/checksums.txt`.
 *
 * Signing (ed25519) is out of band: with a publisher key, run
 * `lode-cli sign`/`manifest-sign` over these outputs (see the release workflow).
 * An unsigned manifest still carries integrity and works under
 * `require_signature = auto` with no key configured.
 *
 * Usage:
 *   bun scripts/package.ts --version 0.2.0 \
 *     --asset-url https://github.com/gimhq/gino/releases/download/v0.2.0/gino.js
 *   # optional: --app-name gino-host  --asset-name gino.js
 */
import { copyFileSync, existsSync, mkdirSync, readFileSync, renameSync, unlinkSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";
import { Glob } from "bun";

const { values: args } = parseArgs({
  options: {
    "app-name": { type: "string" },
    "version": { type: "string" },
    "asset-name": { type: "string" },
    "asset-url": { type: "string" },
  },
  strict: false,
});

const ROOT = resolve(import.meta.dir, "..");
const OUT_DIR = resolve(ROOT, "dist");
const DRIZZLE_DIR = resolve(ROOT, "drizzle");
const MIGRATIONS_FILE = resolve(ROOT, "src/db/embedded-migrations.ts");
const MIGRATIONS_BACKUP = `${MIGRATIONS_FILE}.bak`;

const APP_NAME = (args["app-name"] as string | undefined) ?? "gino-host";
const ASSET_NAME = (args["asset-name"] as string | undefined) ?? "gino.js";
const VERSION = ((args.version as string | undefined) ?? "").replace(/^v/, "");
const ASSET_URL = (args["asset-url"] as string | undefined) ?? "";

if (!VERSION) {
  console.error("[package] --version is required (e.g. --version 0.2.0)");
  process.exit(1);
}
if (!ASSET_URL) {
  console.error("[package] --asset-url is required (the GitHub release download URL for the asset)");
  process.exit(1);
}

async function atomicWrite(filePath: string, content: string): Promise<void> {
  const tmp = `${filePath}.tmp`;
  try {
    await Bun.write(tmp, content);
    renameSync(tmp, filePath);
  }
  catch (err) {
    try {
      unlinkSync(tmp);
    }
    catch {}
    throw err;
  }
}

// Run a command, swallowing spawn errors (git may be absent in minimal build
// stages). Every caller has a fallback.
function tryRun(cmd: readonly string[]): string {
  try {
    return Bun.spawnSync([...cmd], { cwd: ROOT }).stdout.toString().trim();
  }
  catch {
    return "";
  }
}

const commit = (process.env.BUILD_COMMIT ?? "").trim()
  || tryRun(["git", "rev-parse", "--short", "HEAD"]) || "unknown";

function resolveBuildTime(): string {
  const envBuildTime = (process.env.BUILD_TIME ?? "").trim();
  if (envBuildTime)
    return envBuildTime;
  const epoch = (process.env.SOURCE_DATE_EPOCH ?? "").trim();
  if (epoch && /^\d+$/.test(epoch)) {
    const seconds = Number.parseInt(epoch, 10);
    if (Number.isFinite(seconds))
      return new Date(seconds * 1000).toISOString();
  }
  const commitIso = tryRun(["git", "show", "-s", "--format=%cI", "HEAD"]);
  if (commitIso)
    return new Date(commitIso).toISOString();
  return new Date().toISOString();
}
const buildTime = resolveBuildTime();

// ---------- 0. Recover from an interrupted previous run ----------
if (existsSync(`${MIGRATIONS_FILE}.tmp`))
  unlinkSync(`${MIGRATIONS_FILE}.tmp`);
if (existsSync(MIGRATIONS_BACKUP)) {
  console.warn("[package] Found stale migrations backup — restoring");
  copyFileSync(MIGRATIONS_BACKUP, MIGRATIONS_FILE);
  unlinkSync(MIGRATIONS_BACKUP);
}

function restoreStubFiles(): void {
  if (existsSync(MIGRATIONS_BACKUP)) {
    copyFileSync(MIGRATIONS_BACKUP, MIGRATIONS_FILE);
    try {
      unlinkSync(MIGRATIONS_BACKUP);
    }
    catch {}
  }
}

function signalHandler(): void {
  console.warn("\n[package] Interrupted — restoring stub files");
  restoreStubFiles();
  process.exit(2);
}
process.on("SIGINT", signalHandler);
process.on("SIGTERM", signalHandler);

mkdirSync(OUT_DIR, { recursive: true });
const outfile = resolve(OUT_DIR, ASSET_NAME);

let buildCode = -1;
try {
  // ---------- 1. Embed drizzle migrations ----------
  console.log("[package] Embedding drizzle migrations...");
  copyFileSync(MIGRATIONS_FILE, MIGRATIONS_BACKUP);

  const migrationFiles: string[] = [];
  for await (const entry of new Glob("**/*").scan({ cwd: DRIZZLE_DIR, onlyFiles: true })) {
    migrationFiles.push(entry);
  }
  migrationFiles.sort();

  const migrationEntries: string[] = [];
  for (const file of migrationFiles) {
    const content = readFileSync(resolve(DRIZZLE_DIR, file), "utf-8");
    migrationEntries.push(`  [${JSON.stringify(file)}, ${JSON.stringify(content)}],`);
  }
  await atomicWrite(MIGRATIONS_FILE, `// Auto-generated by scripts/package.ts — do not edit
export const embeddedMigrations = new Map<string, string>([
${migrationEntries.join("\n")}
])
`);
  console.log(`[package] Generated embedded-migrations.ts (${migrationFiles.length} files)`);

  // ---------- 2. Bundle to a single JS file (run by bun, not --compile) ----------
  console.log(`[package] Bundling ${ASSET_NAME}  (commit ${commit}, version ${VERSION})`);
  const build = Bun.spawn([
    "bun",
    "build",
    "src/index.ts",
    "--target",
    "bun",
    "--define",
    `BUILD_COMMIT=${JSON.stringify(commit)}`,
    "--define",
    `BUILD_TIME=${JSON.stringify(buildTime)}`,
    "--define",
    `BUILD_VERSION=${JSON.stringify(VERSION)}`,
    "--outfile",
    outfile,
  ], { cwd: ROOT, stdio: ["inherit", "inherit", "inherit"] });
  buildCode = await build.exited;
}
finally {
  restoreStubFiles();
  process.off("SIGINT", signalHandler);
  process.off("SIGTERM", signalHandler);
}

if (buildCode !== 0) {
  console.error("[package] JS bundling failed");
  process.exit(1);
}

// ---------- 3. Integrity + lode/v1 manifest + checksums ----------
const bytes = new Uint8Array(await Bun.file(outfile).arrayBuffer());
const hasher = new Bun.CryptoHasher("sha256");
hasher.update(bytes);
const sha256 = hasher.digest("hex");
const size = bytes.byteLength;

const manifest = {
  schema: "lode/v1",
  name: APP_NAME,
  channels: { stable: { latest: VERSION } },
  versions: {
    [VERSION]: {
      assets: [
        {
          name: ASSET_NAME,
          url: ASSET_URL,
          sha256,
          size,
          // Launch + passthrough (lode §4); cwd = the version dir where the bundle lands.
          run: `bun run ${ASSET_NAME}`,
          exec: "bun",
        },
      ],
    },
  },
};

await atomicWrite(resolve(OUT_DIR, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
await atomicWrite(resolve(OUT_DIR, "checksums.txt"), `${sha256}  ${ASSET_NAME}\n`);

console.log(`[package] ✓ ${ASSET_NAME}  sha256=${sha256}  size=${size}`);
console.log(`[package] ✓ dist/manifest.json (lode/v1, ${APP_NAME} → ${VERSION})`);
console.log(`[package] ✓ dist/checksums.txt`);
