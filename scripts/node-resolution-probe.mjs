// Node side of `bun run check:node-resolution`. Copied into a scratch consumer
// directory by scripts/check-node-resolution.ts and run by plain `node` from
// there, so every `import()` below goes through the real package `exports` map
// of an installed-looking `node_modules/@gino/*` — the `bun` condition is off,
// which is the only way to exercise what a Node consumer actually gets.
//
// Reads its plan from argv[2] (written by the driver). Prints one line per
// check and exits non-zero if any of them failed.
import { readFileSync, realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

const plan = JSON.parse(readFileSync(process.argv[2], "utf8"));

let failed = 0;

function ok(what, detail) {
  console.log(`  ok    ${what}${detail ? `  ${detail}` : ""}`);
}

function fail(what, detail) {
  failed += 1;
  console.log(`  FAIL  ${what}  ${detail}`);
}

function realpathOf(url) {
  try {
    return realpathSync(fileURLToPath(url));
  }
  catch {
    return null;
  }
}

// ── 1. every entry point resolves to the file the exports map promises ───────
const loaded = new Map();

for (const entry of plan.entries) {
  const label = `${entry.spec}`;
  let resolved;
  try {
    resolved = import.meta.resolve(entry.spec);
  }
  catch (error) {
    fail(label, `does not resolve under Node: ${error.code ?? error.message}`);
    continue;
  }

  const actual = realpathOf(resolved);
  if (actual !== entry.target) {
    fail(label, `resolved to ${actual ?? resolved}, expected ${entry.target}`);
    continue;
  }

  if (!entry.load) {
    ok(label, `resolves only (${entry.note})`);
    continue;
  }

  try {
    loaded.set(entry.spec, await import(entry.spec));
    ok(label, "resolves + loads");
  }
  catch (error) {
    fail(label, `resolves but fails to load: ${error.code ?? error.constructor.name}: ${error.message}`);
  }
}

// ── 2. one copy of every shared value across a package's entry points ────────
// The P2 regression this exists for: `dist/index.js` bundled its own copy of
// the domain modules, so `@gino/host-core` and `@gino/host-core/domain` handed
// a Node consumer two distinct `AppError` constructors and `instanceof` broke.
// Bun never sees it — it takes the `bun` condition and reads one src tree.
for (const group of plan.identityGroups) {
  const specs = group.specs.filter(spec => loaded.has(spec));
  for (let a = 0; a < specs.length; a++) {
    for (let b = a + 1; b < specs.length; b++) {
      const [left, right] = [specs[a], specs[b]];
      const A = loaded.get(left);
      const B = loaded.get(right);
      const label = `${left} ≡ ${right}`;
      const shared = Object.keys(A).filter(key => key in B);
      const split = shared.filter(key => A[key] !== B[key]);
      if (split.length > 0) {
        fail(label, `${split.length} export(s) exist twice: ${split.slice(0, 8).join(", ")}`);
        continue;
      }
      const classes = shared.filter(key => typeof A[key] === "function");
      if (group.minSharedClasses > 0 && classes.length < group.minSharedClasses) {
        fail(label, `only ${classes.length} shared class(es); expected at least ${group.minSharedClasses} — the identity check has gone vacuous`);
        continue;
      }
      ok(label, `${shared.length} shared export(s), one copy each`);
    }
  }
}

// ── 3. the behavioural half: `instanceof` across the two entries ─────────────
// Value identity above is the mechanism; this is the symptom a consumer hits.
// packages/host-core/tests/node/identity.mts asserts the same thing from inside
// the package, against its own workspace resolution; here it runs against the
// installed-looking layout the probe sets up.
if (plan.instanceofCheck) {
  const { indexSpec, domainSpec } = plan.instanceofCheck;
  const label = `${indexSpec} instanceof ${domainSpec}`;
  const index = loaded.get(indexSpec);
  const domain = loaded.get(domainSpec);
  if (!index || !domain) {
    fail(label, "one of the two entries did not load");
  }
  else {
    const { storage } = index.memoryStorageForTests();
    try {
      storage.blobs.read("skill/000000000000");
      fail(label, "expected a MissingBlobError, got none");
    }
    catch (error) {
      if (error instanceof domain.MissingBlobError) {
        ok(label, "MissingBlobError thrown by the `.` entry is caught by the `./domain` class");
      }
      else {
        fail(label, `threw ${error.constructor.name}, which is not the \`./domain\` MissingBlobError`);
      }
    }
    finally {
      storage.close();
    }
  }
}

console.log(failed === 0 ? "\nnode resolution: OK" : `\nnode resolution: ${failed} failure(s)`);
process.exit(failed === 0 ? 0 : 1);
