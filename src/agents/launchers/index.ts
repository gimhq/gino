// M3 — AGENT MANAGEMENT / launchers (pluggable "where/how an agent runs" seam,
// OOB only — never on the data path).
//
// A `LauncherRegistry` maps an agent's `launcher_kind` → its `Launcher`. There
// is one per `HostContext` (not per process). `manual` is the fallback: an
// agent with no/unknown `launcher_kind` resolves to `manual`, so the host
// controls nothing it cannot reach and starts nothing on its own (safe by
// default). A real launcher (docker, B2b) is registered on the context's
// registry and selected by an agent's `launcher_kind`.
import type { Launcher } from "./types";
import { dockerLauncher } from "./docker";
import { manualLauncher } from "./manual";

/** Per-host `launcher_kind` → Launcher registry; `manual` is the fallback for an unset/unknown kind. */
export class LauncherRegistry {
  private readonly byKind = new Map<string, Launcher>();

  constructor(launchers: readonly Launcher[] = []) {
    for (const l of launchers) {
      this.register(l);
    }
  }

  /** Register (or replace) a launcher by its `kind`. */
  register(launcher: Launcher): void {
    this.byKind.set(launcher.kind, launcher);
  }

  /**
   * Resolve the launcher for a `kind`, falling back to `manual` for an
   * unset/unknown kind — so OOB control of an agent whose launcher is gone (or
   * was never set) safely degrades to a WS `close` instead of throwing.
   * Arrow property so it can be passed unbound as a `resolveLauncher` seam.
   */
  readonly resolve = (kind?: string | null): Launcher => (kind ? this.byKind.get(kind) : undefined) ?? manualLauncher;
}

/** The standalone app's default set: `manual` (safe default) + `docker` (B2b). */
export const DEFAULT_LAUNCHERS: readonly Launcher[] = [manualLauncher, dockerLauncher];

// The docker launcher (B2b) — the first real launcher; constructor exposed so
// deployments/tests can build a variant (custom binary/image/mock spawn).
export { createDockerLauncher, DEFAULT_DOCKER_OP_TIMEOUT_MS, DOCKER_LAUNCHER_KIND, dockerLauncher, DockerOpTimeoutError } from "./docker";
export type { DockerLauncherOptions, SpawnFn, SpawnOptions, SpawnResult } from "./docker";
/** The default launcher (`manual`). Exposed for the registry default + tests. */
export { manualLauncher } from "./manual";
export type { InspectState, Launcher, LauncherCapabilities, LaunchRequest, LaunchResult } from "./types";

export const MODULE = "agents/launchers" as const;
