#!/usr/bin/env bun
/**
 * Remove local build artifacts, coverage, the PID lock, logs, and the dev
 * database. `--all` also removes node_modules and runtime data.
 *
 * Usage:
 *   bun run clean          # standard clean
 *   bun run clean --all    # also remove node_modules + data/
 */
import { existsSync, rmSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import { consola } from "consola";

const ROOT = resolve(import.meta.dirname, "..");
const args = new Set(process.argv.slice(2));
const all = args.has("--all");

const targets = [
  "dist",
  "coverage",
  "data/logs",
  "data/db",
];

const allTargets = [
  "node_modules",
  "data",
];

let cleaned = 0;

function remove(rel: string): void {
  const abs = resolve(ROOT, rel);
  if (existsSync(abs)) {
    rmSync(abs, { recursive: true, force: true });
    consola.log(`  removed ${rel}`);
    cleaned++;
  }
}

for (const rel of targets)
  remove(rel);
if (all) {
  for (const rel of allTargets)
    remove(rel);
}

if (cleaned === 0) {
  consola.log("  nothing to clean");
}
else {
  consola.log(`\n  cleaned ${cleaned} items`);
  if (all)
    consola.log("  run 'bun install' to restore dependencies");
}
