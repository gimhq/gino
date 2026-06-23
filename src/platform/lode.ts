// lode loader integration (docs/host-upgrade-design.md §6).
//
// When the host runs under the **lode** loader it communicates through
// `$LODE_DATA_DIR/state.json`: lode writes `status`/`current`/`last_good`/
// `available`/…; the host writes the app-owned fields `ready` / `target` /
// `restart_nonce`. The two writers serialise their read-modify-write with an
// exclusive `flock(2)` on the sibling `state.json.lock` (the same lock lode
// takes). **Outside lode every function here is a no-op** — gated on the
// `LODE_DATA_DIR` + `LODE_INSTANCE` env vars lode injects, so dev / tests / the
// docker e2e are unaffected.
import { closeSync, openSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";
import { dlopen, FFIType } from "bun:ffi";

// Both read dynamically (not captured at import) so the values track the process
// env — and so the integration is unit-testable by setting the vars per test.
function dataDir(): string | undefined {
  return process.env.LODE_DATA_DIR || undefined;
}
function instance(): string | undefined {
  return process.env.LODE_INSTANCE || undefined;
}

/** True when running under the lode loader (both injected vars present). */
export function isUnderLode(): boolean {
  return Boolean(dataDir() && instance());
}

/** The shape of `state.json` (open — lode may add fields). All optional. */
export interface LodeState {
  current?: string;
  last_good?: string;
  available?: string;
  channel?: string;
  status?: string;
  config_generation?: number;
  // app-owned:
  target?: string | null;
  restart_nonce?: number;
  ready?: string | null;
  [k: string]: unknown;
}

const statePath = (dir: string): string => resolve(dir, "state.json");
const lockPath = (dir: string): string => resolve(dir, "state.json.lock");

/**
 * Read `state.json` (no lock — the atomic temp+rename write guarantees a
 * consistent snapshot). Returns `null` outside lode or if absent/unparsable.
 */
export function readLodeState(): LodeState | null {
  const dir = dataDir();
  if (!dir)
    return null;
  try {
    return JSON.parse(readFileSync(statePath(dir), "utf8")) as LodeState;
  }
  catch {
    return null;
  }
}

// flock(2) via libc so the host's RMW serialises with lode's. BSD/Linux constants
// LOCK_EX=2 / LOCK_UN=8. Lazily resolved; a lock-free fallback (atomic write only)
// applies when libc/FFI is unavailable — dev / non-Linux, where there is no lode.
const LOCK_EX = 2;
const LOCK_UN = 8;
let flockResolved = false;
let flockFn: ((fd: number, op: number) => number) | null = null;
function getFlock(): ((fd: number, op: number) => number) | null {
  if (flockResolved)
    return flockFn;
  flockResolved = true;
  try {
    const lib = dlopen("libc.so.6", {
      flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
    });
    flockFn = (fd, op) => lib.symbols.flock(fd, op) as number;
  }
  catch {
    flockFn = null;
  }
  return flockFn;
}

function withLock<T>(dir: string, fn: () => T): T {
  const flock = getFlock();
  if (!flock)
    return fn();
  let fd = -1;
  try {
    fd = openSync(lockPath(dir), "a"); // create-if-absent; never deleted
    flock(fd, LOCK_EX);
    return fn();
  }
  finally {
    if (fd >= 0) {
      try {
        flock(fd, LOCK_UN);
      }
      catch {}
      try {
        closeSync(fd);
      }
      catch {}
    }
  }
}

/** RMW the APP-owned fields under the lock, preserving lode's own fields. */
function patch(make: (cur: LodeState) => Partial<LodeState>): void {
  const dir = dataDir();
  if (!dir || !instance())
    return;
  withLock(dir, () => {
    const cur = readLodeState() ?? {};
    const next = { ...cur, ...make(cur) };
    const tmp = `${statePath(dir)}.tmp`;
    writeFileSync(tmp, `${JSON.stringify(next, null, 2)}\n`);
    renameSync(tmp, statePath(dir));
  });
}

/**
 * Report "I can serve now" (readiness=state, §6.1): write `ready = $LODE_INSTANCE`
 * so lode commits the new version / stops the old instance. No-op outside lode.
 */
export function markReady(): void {
  const id = instance();
  if (!id)
    return;
  patch(() => ({ ready: id }));
}

/** Ask lode to hot-update to a version (or `"latest"`) by setting `target`. */
export function requestUpdate(version: string): void {
  patch(() => ({ target: version }));
}

/** Ask lode to restart the current version (it re-reads lode.toml on relaunch). */
export function requestRestart(): void {
  patch(cur => ({
    restart_nonce: (typeof cur.restart_nonce === "number" ? cur.restart_nonce : 0) + 1,
  }));
}
