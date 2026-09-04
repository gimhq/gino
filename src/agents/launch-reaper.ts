import type { Launcher } from "./launchers/types";
// M3 — PENDING-LAUNCH REAPER (HOST-M3-05).
//
// `control.launchAgent` records every launch in the persisted
// `pending_launches` table (keyed by the OD17 nonce) and a successful
// registration consumes the row via `correlateLaunch`. When the agent NEVER
// registers (bad image, crash-loop, network fault), the runtime would keep
// running forever with `GINO_REGISTER_TOKEN` in its env, invisible to
// `GET /admin/agents`. This reaper is the backstop: any pending launch older
// than the TTL has its runtime torn down (`launcher.remove(ref)`) and its row
// dropped. Because the table is persisted, a host restart changes nothing —
// orphans from before the restart are reaped on the next tick, and a LATE
// registration inside the TTL still correlates.
//
// Teardown is best-effort: a launcher failure (wedged docker daemon) is logged
// and the row is dropped anyway — the reaper bounds pending-launch state; it
// never retries forever against a broken runtime handle.
import type { AppDatabase } from "@/db";
import type { Logger } from "@/shared/lib/logger";
import { manualLauncher } from "./launchers/manual";
import { deletePendingLaunch, hasPendingLaunch, listExpiredPendingLaunches } from "./registry";

/** Default launch→register TTL: a launched agent has 10 minutes to register. */
export const PENDING_LAUNCH_TTL_MS = 10 * 60_000;

/** Default reap cadence. */
export const LAUNCH_REAPER_INTERVAL_MS = 60_000;

export interface LaunchReaperDeps {
  readonly db: AppDatabase;
  readonly logger?: Logger;
  /** Launch→register TTL in ms. Defaults to `PENDING_LAUNCH_TTL_MS`. */
  readonly ttlMs?: number;
  /** Reap cadence in ms. Defaults to `LAUNCH_REAPER_INTERVAL_MS`. */
  readonly intervalMs?: number;
  /** Clock seam (tests inject a fake clock). Defaults to `Date.now`. */
  readonly now?: () => number;
  /** Launcher lookup seam — pass `host.launchers.resolve`; defaults to the manual launcher (controls nothing). */
  readonly resolveLauncher?: (kind: string | null | undefined) => Launcher;
}

export interface LaunchReaperHandle {
  /** Stop the periodic reap (call on shutdown). Idempotent. */
  readonly stop: () => void;
  /** Run a reap immediately; resolves to the number of entries reaped. */
  readonly reap: () => Promise<number>;
}

/**
 * One reap pass: for every pending launch older than the TTL, tear down its
 * runtime (when the launcher can and a `ref` exists) and drop the entry —
 * consumed-on-correlate rows are naturally never seen here. Returns the number
 * of entries reaped.
 */
export async function reapPendingLaunches(deps: LaunchReaperDeps): Promise<number> {
  const now = deps.now?.() ?? Date.now();
  const cutoff = now - (deps.ttlMs ?? PENDING_LAUNCH_TTL_MS);
  const expired = listExpiredPendingLaunches(deps.db, cutoff);

  let reaped = 0;
  for (const entry of expired) {
    // Re-check under the loop (`hasPendingLaunch`): a registration racing the
    // reap may have consumed the row since the list was read — never tear down
    // a runtime that just correlated.
    if (!hasPendingLaunch(deps.db, entry.nonce)) {
      continue;
    }
    if (entry.launchRef) {
      const launcher = (deps.resolveLauncher ?? (() => manualLauncher))(entry.launcherKind);
      try {
        await launcher.remove(entry.launchRef);
        deps.logger?.info(
          { launcher: entry.launcherKind, ref: entry.launchRef },
          "launch reaper: runtime torn down (agent never registered inside the TTL)",
        );
      }
      catch (err) {
        deps.logger?.warn(
          { launcher: entry.launcherKind, ref: entry.launchRef, err },
          "launch reaper: runtime teardown failed — dropping the entry anyway",
        );
      }
    }
    deletePendingLaunch(deps.db, entry.nonce);
    reaped += 1;
  }
  return reaped;
}

/**
 * Start the periodic pending-launch reap. The timer is `unref`'d so it never
 * keeps the process alive on its own; `stop()` clears it. A failing pass is
 * logged and swallowed so the loop survives a transient error.
 */
export function startLaunchReaper(deps: LaunchReaperDeps): LaunchReaperHandle {
  const tick = () => {
    reapPendingLaunches(deps).catch((err) => {
      deps.logger?.error({ err }, "launch reaper failed");
    });
  };

  const timer = setInterval(tick, deps.intervalMs ?? LAUNCH_REAPER_INTERVAL_MS);
  timer.unref?.();

  return {
    stop: () => clearInterval(timer),
    reap: () => reapPendingLaunches(deps),
  };
}
