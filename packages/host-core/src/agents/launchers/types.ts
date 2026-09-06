// M3 — LAUNCHER interface (the "where/how an agent runs" seam, OOB only).
//
// A Launcher is the ONLY place that knows about a runtime (a docker daemon, a
// VM API, k8s …). It is never on the data path — the host commands agents over
// the protocol (M2), and reaches their *runtime* only out-of-band through this
// seam (docs/host-v2-design.md §M3). `kind` selects the launcher for an agent
// (`agents.launcher_kind`); `capabilities` tell the control layer which OOB ops
// the launcher can perform vs. which must fall back to a WS `close`.

/** Where an inspect maps an opaque `ref` to a coarse runtime state. */
export type InspectState = "running" | "exited" | "unknown";

/**
 * What a launcher can do OOB. The control layer gates on these: a `false`
 * capability means "this launcher cannot do it" → fall back to a WS `close`
 * (stop/remove) or report `unknown` (inspect). `manual` is all-`false`.
 */
export interface LauncherCapabilities {
  readonly canLaunch: boolean;
  readonly canStop: boolean;
  readonly canRestart: boolean;
  readonly canInspect: boolean;
}

/**
 * A launch request (config-driven, B2b). The launcher starts a runtime that
 * dials back into the host and registers with the shared register token + a
 * per-launch correlation nonce (the token is no longer one-time). `ref` is the
 * launcher's handle on the started runtime (e.g. a container id), persisted as
 * `agents.launch_ref` once the registration correlates.
 */
export interface LaunchRequest {
  readonly hostUrl: string;
  readonly registerToken: string;
  readonly workload: { readonly taskId?: string };
  readonly env?: Record<string, string>;
  readonly resources?: Record<string, unknown>;
}

/** The result of a launch — `ref` null when nothing was started (manual). */
export interface LaunchResult {
  readonly ref: string | null;
}

/**
 * The pluggable runtime seam (docs/host-v2-design.md §M3). Implementations:
 * `manual` (default, starts nothing — this phase), `docker` (B2b), `vm/gcp/k8s`
 * later. The agent + protocol are unaffected by which launcher owns an agent.
 */
export interface Launcher {
  readonly kind: string;
  readonly capabilities: LauncherCapabilities;
  launch: (req: LaunchRequest) => Promise<LaunchResult>;
  stop: (ref: string) => Promise<void>;
  restart: (ref: string) => Promise<void>;
  remove: (ref: string) => Promise<void>;
  inspect: (ref: string) => Promise<InspectState>;
}

/**
 * Thrown by a launcher op the launcher cannot perform (e.g. any `manual`
 * runtime op). The control layer never relies on this — it gates on
 * `capabilities` first — but throwing keeps a *direct* misuse loud rather than
 * silently succeeding.
 */
export class UnsupportedLauncherOp extends Error {
  constructor(kind: string, op: string) {
    super(`launcher '${kind}' does not support '${op}'`);
    this.name = "UnsupportedLauncherOp";
  }
}
