#!/usr/bin/env bun
/* eslint-disable no-console */
/**
 * Validate the `dist/` lode release outputs before they are uploaded — an
 * INDEPENDENT guard (run by `.github/workflows/release.yml`, and usable locally
 * after `bun run package`). It re-reads `apps/host/dist/<asset>`, `manifest.json`
 * and `checksums.txt` beside it, re-hashes the asset bytes, and asserts the `lode/v1`
 * manifest is well-formed and consistent with the actual bytes + expected
 * release coordinates. Exits non-zero (with `::error::` annotations) on any
 * problem, so a packaging bug or a hand-edit can't ship a mismatched release.
 *
 * Usage:
 *   bun apps/host/scripts/validate-release.ts --version 0.2.0 \
 *     --asset-url https://github.com/gimhq/gino/releases/download/v0.2.0/gino.js
 *   # optional: --app-name gino-host  --asset-name gino.js
 */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";

interface ManifestAsset {
  name?: string;
  url?: string;
  sha256?: string;
  size?: number;
  run?: string;
  exec?: string;
}
interface Manifest {
  schema?: string;
  name?: string;
  channels?: Record<string, { latest?: string }>;
  versions?: Record<string, { assets?: ManifestAsset[] }>;
}

const { values: args } = parseArgs({
  options: {
    "app-name": { type: "string" },
    "version": { type: "string" },
    "asset-name": { type: "string" },
    "asset-url": { type: "string" },
  },
  strict: false,
});

const OUT = resolve(import.meta.dir, "..", "dist");
const APP_NAME = (args["app-name"] as string | undefined) ?? "gino-host";
const ASSET_NAME = (args["asset-name"] as string | undefined) ?? "gino.js";
const VERSION = ((args.version as string | undefined) ?? "").replace(/^v/, "");
const ASSET_URL = (args["asset-url"] as string | undefined) ?? "";

if (!VERSION) {
  console.error("[validate-release] --version is required");
  process.exit(2);
}
if (!ASSET_URL) {
  console.error("[validate-release] --asset-url is required");
  process.exit(2);
}

const problems: string[] = [];
function check(cond: unknown, msg: string): void {
  if (!cond)
    problems.push(msg);
}

const assetPath = resolve(OUT, ASSET_NAME);
const manifestPath = resolve(OUT, "manifest.json");
const checksumsPath = resolve(OUT, "checksums.txt");

check(existsSync(assetPath), `missing dist/${ASSET_NAME}`);
check(existsSync(manifestPath), "missing dist/manifest.json");
check(existsSync(checksumsPath), "missing dist/checksums.txt");

if (problems.length === 0) {
  // Independent integrity: hash the actual asset bytes and require the manifest
  // (+ checksums) to match them — not just the value package.ts wrote.
  const bytes = new Uint8Array(await Bun.file(assetPath).arrayBuffer());
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(bytes);
  const sha256 = hasher.digest("hex");
  const size = bytes.byteLength;

  const m = JSON.parse(readFileSync(manifestPath, "utf8")) as Manifest;
  const asset = m.versions?.[VERSION]?.assets?.find(a => a.name === ASSET_NAME);

  check(m.schema === "lode/v1", `manifest schema must be lode/v1 (got ${m.schema})`);
  check(m.name === APP_NAME, `manifest name must be ${APP_NAME} (got ${m.name})`);
  check(m.channels?.stable?.latest === VERSION, `stable channel must point at ${VERSION} (got ${m.channels?.stable?.latest})`);
  check(asset != null, `manifest missing asset ${ASSET_NAME} under version ${VERSION}`);
  if (asset) {
    check(asset.url === ASSET_URL, `asset url mismatch: ${asset.url} != ${ASSET_URL}`);
    check(asset.run === `bun run ${ASSET_NAME}`, `asset run mismatch: ${asset.run}`);
    check(asset.exec === "bun", `asset exec mismatch: ${asset.exec}`);
    check(asset.sha256 === sha256, `asset sha256 != actual bytes (${asset.sha256} != ${sha256})`);
    check(asset.size === size, `asset size != actual bytes (${asset.size} != ${size})`);
  }

  const checksums = readFileSync(checksumsPath, "utf8");
  check(checksums.includes(ASSET_NAME), "checksums.txt must reference the asset");
  check(checksums.includes(sha256), "checksums.txt sha256 must match the asset bytes");
}

if (problems.length > 0) {
  for (const p of problems)
    console.error(`::error::${p}`);
  console.error(`[validate-release] ✗ ${problems.length} problem(s)`);
  process.exit(1);
}

console.log(`[validate-release] ✓ ${APP_NAME} ${VERSION} (${ASSET_NAME}) — manifest + integrity ok`);
