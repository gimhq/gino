// M3 — AGENT MANAGEMENT / launchers (pluggable "where/how an agent runs" seam,
// OOB only — never on the data path).
//
// The registry maps an agent's `launcher_kind` → its `Launcher`. `manual` is
// registered as the DEFAULT: an agent with no/unknown `launcher_kind` resolves
// to `manual`, so the host controls nothing it cannot reach and starts nothing
// on its own (safe by default). A real launcher (docker, B2b) registers itself
// here via `registerLauncher` and is selected by an agent's `launcher_kind`.
import type { Launcher } from "./types";
import { dockerLauncher } from "./docker";
import { manualLauncher } from "./manual";

const registry = new Map<string, Launcher>();

/** Register (or replace) a launcher by its `kind`. Called at module load. */
export function registerLauncher(launcher: Launcher): void {
  registry.set(launcher.kind, launcher);
}

/**
 * Resolve the launcher for a `kind`, falling back to `manual` for an
 * unset/unknown kind — so OOB control of an agent whose launcher is gone (or
 * was never set) safely degrades to a WS `close` instead of throwing.
 */
export function getLauncher(kind?: string | null): Launcher {
  if (kind) {
    const found = registry.get(kind);
    if (found) {
      return found;
    }
  }
  return manualLauncher;
}

// The docker launcher (B2b) — the first real launcher; constructor exposed so
// deployments/tests can build a variant (custom binary/image/mock spawn).
export { createDockerLauncher, DOCKER_LAUNCHER_KIND, dockerLauncher } from "./docker";
export type { DockerLauncherOptions, SpawnFn, SpawnResult } from "./docker";
/** The default launcher (`manual`). Exposed for the registry default + tests. */
export { manualLauncher } from "./manual";
export type { InspectState, Launcher, LauncherCapabilities, LaunchRequest, LaunchResult } from "./types";

// `manual` is always available (the safe default); `docker` (B2b) is the first
// real launcher, selected by an agent's `launcher_kind = "docker"`.
registerLauncher(manualLauncher);
registerLauncher(dockerLauncher);

export const MODULE = "agents/launchers" as const;
