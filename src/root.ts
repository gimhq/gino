import { dirname, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

/**
 * ROOT_DIR resolution:
 * 1. ROOT_DIR env var — explicit override
 * 2. Compiled binary (/$bunfs) — process.cwd()
 * 3. Otherwise — one level up from this file (src/) to the repo root
 *
 * This is a single-root project (no monorepo): `src/` sits at the repo
 * root, so `drizzle/` and `data/` resolve relative to `../`.
 */
function detectRootDir(): string {
  if (process.env.ROOT_DIR) {
    return resolve(process.env.ROOT_DIR);
  }

  // import.meta.url is always available (works in Bun, Node, Vite).
  const thisDir = dirname(fileURLToPath(import.meta.url));

  // Compiled binary: Bun virtual filesystem.
  if (thisDir.startsWith("/$bunfs")) {
    return process.cwd();
  }

  // Dev / test: this file is at <root>/src/root.ts → go up one level.
  return resolve(thisDir, "..");
}

export const ROOT_DIR = detectRootDir();
