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

  // Dev / test: this file is at <root>/src/dirs.ts → go up one level.
  return resolve(thisDir, "..");
}

export const ROOT_DIR = detectRootDir();

/**
 * DATA_DIR resolution — the persistent base for *runtime data* (DB / store /
 * logs), a separate concern from `ROOT_DIR` (which locates code + migrations).
 * This mirrors the lode SDK's `dataDir()` (`src/lode/sdk.ts`):
 *
 *   DATA_DIR (runtime) > LODE_DIR (runtime, loader-injected) > ROOT_DIR (dev)
 *
 * The first two are **runtime** dependencies — set in production / under a loader.
 * Under lode (SDK ≥ v0.0.10) the supervisor injects `LODE_DIR` (lode's own dir,
 * where `state.json` + `versions/` live); gino's relative defaults carry a
 * `data/` prefix, so data lands in `$LODE_DIR/data/**` — beside lode's state, not
 * inside the swappable version dir. The final fallback is `ROOT_DIR`, which
 * **always exists** and is used for the development environment; because the
 * loader always provides `LODE_DIR`, that fallback is only ever reached in dev —
 * where `ROOT_DIR` is the stable repo root, never an ephemeral per-version dir.
 * Relative `GINO_DB_PATH`/`GINO_STORE_DIR`/`GINO_LOG_FILE` resolve against this
 * base; an absolute path overrides it entirely.
 */
function detectDataDir(): string {
  return resolve(process.env.DATA_DIR ?? process.env.LODE_DIR ?? ROOT_DIR);
}

export const DATA_DIR = detectDataDir();
