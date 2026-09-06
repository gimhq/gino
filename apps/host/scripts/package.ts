#!/usr/bin/env bun
/* eslint-disable no-console */
/**
 * Package the host as a **lode** release asset, end to end:
 *   1. regenerate the committed embedded migrations map
 *      (`packages/host-store-sqlite/src/embedded-migrations.ts`, D9) via the
 *      store package's `gen:migrations` and fail if it differs from the
 *      committed file (drizzle/ and the embedded map must not diverge),
 *   2. bundle to a single JS file `apps/host/dist/gino.js` via
 *      `bun build --target bun` (NOT `--compile` — lode's `[runtime]` provides
 *      `bun`), injecting `BUILD_COMMIT`/`BUILD_TIME`/`BUILD_VERSION`,
 *   3. emit a `lode/v1` `dist/manifest.json` (integrity sha256 + size + the
 *      launch command) and `dist/checksums.txt` beside the bundle.
 *
 * Signing (ed25519) is out of band: with a publisher key, run
 * `lode-cli sign`/`manifest-sign` over these outputs (see the release workflow).
 * An unsigned manifest still carries integrity and works under
 * `require_signature = auto` with no key configured.
 *
 * Usage:
 *   bun run package --version 0.2.0 \
 *     --asset-url https://github.com/gimhq/gino/releases/download/v0.2.0/gino.js
 *   # optional: --app-name gino-host  --asset-name gino.js
 */
import { mkdirSync, renameSync, unlinkSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";

const { values: args } = parseArgs({
  options: {
    "app-name": { type: "string" },
    "version": { type: "string" },
    "asset-name": { type: "string" },
    "asset-url": { type: "string" },
  },
  strict: false,
});

// ROOT = apps/host (this script lives in apps/host/scripts); REPO_ROOT = the
// workspace root, where the store package and git live.
const ROOT = resolve(import.meta.dir, "..");
const REPO_ROOT = resolve(ROOT, "../..");
const OUT_DIR = resolve(ROOT, "dist");
const MIGRATIONS_FILE = "packages/host-store-sqlite/src/embedded-migrations.ts";

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

mkdirSync(OUT_DIR, { recursive: true });
const outfile = resolve(OUT_DIR, ASSET_NAME);

// ---------- 1. Embedded migrations must match drizzle/ ----------
console.log("[package] Regenerating embedded migrations...");
const gen = Bun.spawn(
  ["bun", "run", "--filter", "@gino/host-store-sqlite", "gen:migrations"],
  { cwd: REPO_ROOT, stdio: ["inherit", "inherit", "inherit"] },
);
if (await gen.exited !== 0) {
  console.error("[package] gen:migrations failed");
  process.exit(1);
}
const diff = Bun.spawn(
  ["git", "diff", "--exit-code", "--", MIGRATIONS_FILE],
  { cwd: REPO_ROOT, stdio: ["inherit", "inherit", "inherit"] },
);
if (await diff.exited !== 0) {
  console.error(`[package] ${MIGRATIONS_FILE} is out of sync with drizzle/ — commit the regenerated file`);
  process.exit(1);
}

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
const buildCode = await build.exited;

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
