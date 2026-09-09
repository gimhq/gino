#!/usr/bin/env bun
/* eslint-disable no-console */
/**
 * Pre-publish gate: every publishable `packages/*` entry point must resolve —
 * and, unless it is deliberately Bun-only, load — under plain `node`, against
 * the BUILT `dist/`, through the package's own `exports` map.
 *
 * Why this is a separate check: `bun run test` reads the `bun` condition, which
 * points at `src/**`, so it exercises one source tree and never the shipped
 * files. In P2 that blind spot shipped a real bug — `dist/index.js` bundled its
 * own copy of the domain modules, so a Node consumer importing values from both
 * `@gino/host-core` and `@gino/host-core/domain` got two distinct copies of
 * every class and `instanceof` silently failed. Hence §2/§3 of the probe.
 *
 * What it does:
 *   1. reads `exports` + `files` from every non-private `packages/*` manifest;
 *   2. statically asserts each exports target exists on disk and is covered by
 *      `files` (an entry the tarball would not contain is a broken package);
 *   3. builds a scratch consumer under `.tmp/node-resolution/` whose
 *      `node_modules/@gino/*` symlinks the packages, so bare specifiers go
 *      through the real exports map with the `bun` condition off;
 *   4. runs `scripts/node-resolution-probe.mjs` there under `node`.
 *
 * Usage:  bun run check:node-resolution   (needs a current `bun run build`)
 */
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dir, "..");
const PACKAGES_DIR = join(ROOT, "packages");
const SCRATCH = join(ROOT, ".tmp", "node-resolution");

/**
 * Subpaths that cannot load under Node by design, with the reason. They are
 * still resolution-checked: the exports map must point Node at a file that
 * exists in the tarball. Anything NOT listed here must load, so a new subpath
 * fails closed until someone decides which it is.
 */
const RESOLVE_ONLY: Record<string, string> = {
  "@gino/host-core#./testing": "TypeScript source, not built output — Node refuses to strip types under node_modules",
  "@gino/host-store-sqlite#./bun": "Bun-only: imports bun:sqlite",
  "@gino/host-hono#./bun": "Bun-only: Bun.serve WebSocket upgrade",
  "@gino/host-proxy#./bun": "Bun-only: raw Bun socket relay (host-sdk-design §7.5)",
  "@gino/host-proxy#./hono": "Bun-only: /llm router over the Bun WS bridge (host-sdk-design §7.5)",
};

/** Packages whose entry points must share classes, so the identity check cannot go vacuous. */
const MIN_SHARED_CLASSES: Record<string, number> = {
  "@gino/host-core": 1,
};

interface Manifest {
  name: string;
  private?: boolean;
  files?: string[];
  exports?: Record<string, Record<string, string>>;
}

interface PlanEntry {
  spec: string;
  target: string;
  load: boolean;
  note: string;
}

/** `files` here is a list of plain paths, not globs; a `*` would fail closed and say so. */
function coveredByFiles(files: string[], target: string): boolean {
  return files.some(f => target === f || target.startsWith(`${f}/`));
}

const problems: string[] = [];
const entries: PlanEntry[] = [];
const identityGroups: { specs: string[]; minSharedClasses: number }[] = [];
const linked: { name: string; dir: string }[] = [];

for (const dir of readdirSync(PACKAGES_DIR).sort()) {
  const pkgDir = join(PACKAGES_DIR, dir);
  const manifestPath = join(pkgDir, "package.json");
  if (!existsSync(manifestPath)) continue;
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as Manifest;
  if (manifest.private) continue;

  const files = manifest.files ?? [];
  const exportsMap = manifest.exports ?? {};
  linked.push({ name: manifest.name, dir: pkgDir });
  const loadable: string[] = [];

  for (const [subpath, conditions] of Object.entries(exportsMap)) {
    const spec = subpath === "." ? manifest.name : `${manifest.name}/${subpath.slice(2)}`;
    const key = `${manifest.name}#${subpath}`;

    // Every condition target must exist and ship in the tarball, not just the
    // one Node takes — a missing `bun` or `types` target is just as broken.
    for (const [condition, targetSpec] of Object.entries(conditions)) {
      const rel = targetSpec.replace(/^\.\//, "");
      if (!existsSync(join(pkgDir, rel))) {
        problems.push(`${key} (${condition}) → ${targetSpec} does not exist; run \`bun run build\``);
      }
      if (!coveredByFiles(files, rel)) {
        problems.push(`${key} (${condition}) → ${targetSpec} is not covered by "files": ${JSON.stringify(files)}`);
      }
    }

    const nodeTarget = conditions.import;
    if (!nodeTarget) {
      problems.push(`${key} has no "import" condition, so Node cannot resolve it`);
      continue;
    }
    const note = RESOLVE_ONLY[key] ?? "";
    const rel = nodeTarget.replace(/^\.\//, "");
    if (!note && !rel.startsWith("dist/")) {
      problems.push(`${key} points Node at ${nodeTarget}, which is not built output`);
    }
    if (!existsSync(join(pkgDir, rel))) continue;

    entries.push({ spec, target: realpathSync(join(pkgDir, rel)), load: note === "", note });
    if (note === "") loadable.push(spec);
  }

  if (loadable.length > 1) {
    identityGroups.push({ specs: loadable, minSharedClasses: MIN_SHARED_CLASSES[manifest.name] ?? 0 });
  }
}

if (problems.length > 0) {
  console.error("package metadata / build problems:");
  for (const p of problems) console.error(`  FAIL  ${p}`);
  process.exit(1);
}

// Scratch consumer: bare specifiers must go through the exports map, and an
// ESM `import()` resolves from the importing FILE, so the probe has to live here.
rmSync(SCRATCH, { recursive: true, force: true });
mkdirSync(join(SCRATCH, "node_modules", "@gino"), { recursive: true });
writeFileSync(join(SCRATCH, "package.json"), `${JSON.stringify({ name: "gino-node-resolution-check", private: true, type: "module" }, null, 2)}\n`);
for (const { name, dir } of linked) {
  symlinkSync(dir, join(SCRATCH, "node_modules", name));
}
cpSync(join(ROOT, "scripts", "node-resolution-probe.mjs"), join(SCRATCH, "probe.mjs"));

const planPath = join(SCRATCH, "plan.json");
writeFileSync(planPath, `${JSON.stringify({
  entries,
  identityGroups,
  instanceofCheck: { indexSpec: "@gino/host-core", domainSpec: "@gino/host-core/domain" },
}, null, 2)}\n`);

console.log(`node resolution check — ${entries.length} entry point(s) across ${linked.length} package(s)\n`);
const run = Bun.spawnSync(["node", "probe.mjs", planPath], { cwd: SCRATCH, stdio: ["ignore", "inherit", "inherit"] });
process.exit(run.exitCode ?? 1);
