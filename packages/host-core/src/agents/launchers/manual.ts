// M3 — MANUAL launcher (the default; SAFE BY DEFAULT — host auto-starts nothing).
//
// `manual` is for agents an operator runs by hand: the host hands out
// `{ hostUrl, registerToken }` and the operator launches the runtime however
// they like. It starts nothing (`canLaunch=false`) and controls no runtime
// (`canStop/canRestart/canInspect=false`) — so the control layer falls back to
// a WS `close` for stop/remove and reports `unknown` for inspect
// (docs/host-v2-design.md §M3). It is the default for every enrolled agent
// until a real launcher (docker, B2b) owns it.
import type { InspectState, Launcher, LaunchResult } from "./types";
import { UnsupportedLauncherOp } from "./types";

/**
 * The default launcher: it starts and controls nothing, so the host never
 * spawns a runtime it was not explicitly asked to. Every capability is
 * `false`, which makes stop and restart degrade to a `close` frame and inspect
 * report `"unknown"`. Use it for agents an operator runs by hand.
 */
export const manualLauncher: Launcher = {
  kind: "manual",
  capabilities: { canLaunch: false, canStop: false, canRestart: false, canInspect: false },

  // Starts nothing — the operator runs the agent by hand. `ref` is null so the
  // control layer treats every OOB op as launcher-incapable (→ WS close).
  async launch(): Promise<LaunchResult> {
    return { ref: null };
  },

  // No runtime to control. These throw `UnsupportedLauncherOp` so a *direct*
  // call is loud; the control layer never reaches them (it gates on
  // `capabilities` and falls back to a WS `close`).
  async stop(): Promise<void> {
    throw new UnsupportedLauncherOp("manual", "stop");
  },
  async restart(): Promise<void> {
    throw new UnsupportedLauncherOp("manual", "restart");
  },
  async remove(): Promise<void> {
    throw new UnsupportedLauncherOp("manual", "remove");
  },

  // No runtime to inspect — the host only knows the agent's protocol state.
  async inspect(): Promise<InspectState> {
    return "unknown";
  },
};
