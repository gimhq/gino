// gino's thin adapter over the vendored lode SDK (`./lode-sdk`, github.com/dotns/lode).
// The SDK owns the whole state.json contract (flock-serialised RMW, readiness,
// update/restart/rollback, forward-compatible field preservation); this module
// exposes just the slice the host uses (docs/host-upgrade-design.md §6) and gates
// every call on actually running under lode. Outside lode every function is inert.
import { isSupervised, Lode } from "./lode-sdk";

/** The parsed `state.json` shape (camelCase) the SDK returns. */
export type LodeState = ReturnType<Lode["read"]>;

/**
 * Supervised by lode (LODE_DATA_DIR present) — the gate for reading state and
 * requesting update/restart/rollback. The readiness handshake additionally needs
 * the instance id; under real lode both are always injected together.
 */
export function isUnderLode(): boolean {
  return isSupervised();
}

function client(): Lode | null {
  return isSupervised() ? Lode.fromEnv() : null;
}

/** Read lode's `state.json` (typed; `null` outside lode or when absent). */
export function readLodeState(): LodeState {
  return client()?.read() ?? null;
}

/** Readiness (§6.1): report "I can serve now". No-op without a supervised launch. */
export function markReady(): void {
  const c = client();
  if (c && c.instance)
    c.markReady();
}

/** Ask lode to hot-update to a version (or `"latest"`) by setting `target`. */
export function requestUpdate(version: string): void {
  client()?.requestUpdate(version);
}

/** Ask lode to restart the current version (it re-reads lode.toml on relaunch). */
export function requestRestart(): void {
  client()?.reboot();
}

/**
 * Ask lode to roll back to `version`, else the recorded `last_good`. Returns the
 * chosen version, or `null` outside lode / when there is no `last_good` to use.
 */
export function requestRollback(version?: string): string | null {
  const c = client();
  if (!c)
    return null;
  try {
    return c.rollback(version);
  }
  catch {
    return null;
  }
}
