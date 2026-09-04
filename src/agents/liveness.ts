// M3 — LIVENESS (offline-grace sweep; DP4).
//
// `heartbeat` (every `config.heartbeatIntervalMs`) bumps `last_seen` via the
// registry's `touchLiveness` (B1b). This sweep is the backstop: an `online`
// agent silent past the grace — including one whose socket died WITHOUT a clean
// WS close (a half-open connection or a crashed agent) — is flipped to
// `offline`.
//
// DP4 invariant: `offline` is an AGENT state, NOT a task state. The sweep NEVER
// reaps, terminates, or migrates a task or cron — the agent owns its work
// durably and resumes it on reconnect (the WS `onOpen` flips it back `online`
// and replays the unacked outbox; a resumed heartbeat on a still-live socket
// also restores `online` via `touchLiveness`). The grace window is the only
// thing tuned here; everything else is left untouched.
import type { SessionRegistry } from "@/agents/sessions";
import type { AppDatabase } from "@/db";
import type { HostEvents } from "@/host/events";
import type { Logger } from "@/shared/lib/logger";
import { and, eq, isNotNull, lt } from "drizzle-orm";
import { agents } from "@/db/schema";
import { HEARTBEAT_INTERVAL_MS } from "@/protocol/version";

/** DP4 grace = N × heartbeat interval. N missed beats of silence → `offline`. */
export const OFFLINE_GRACE_MULTIPLIER = 3;

/** Default offline-grace window (ms): 3 × the advertised heartbeat interval. */
export const OFFLINE_GRACE_MS = OFFLINE_GRACE_MULTIPLIER * HEARTBEAT_INTERVAL_MS;

export interface LivenessDeps {
  readonly db: AppDatabase;
  readonly logger?: Logger;
  /**
   * Live-session registry (HOST-M3-14): when provided, the sweep also closes +
   * detaches a swept agent's zombie socket so `sessions.isOnline` converges
   * with `agents.status` instead of answering `true` for a dead pipe.
   */
  readonly sessions?: Pick<SessionRegistry, "get" | "detach">;
  /** Host event emitter: each swept agent is announced as `agent.offline` (reason `sweep`). */
  readonly events?: HostEvents;
  /** Grace window in ms. Defaults to `OFFLINE_GRACE_MS`. */
  readonly graceMs?: number;
  /** Sweep cadence in ms. Defaults to `HEARTBEAT_INTERVAL_MS`. */
  readonly intervalMs?: number;
  /** Clock seam (tests inject a fake clock). Defaults to `Date.now`. */
  readonly now?: () => number;
}

export interface LivenessHandle {
  /** Stop the periodic sweep (call on shutdown). Idempotent. */
  readonly stop: () => void;
  /** Run a sweep immediately; returns the number of agents marked offline. */
  readonly sweep: () => number;
}

/**
 * One offline-grace pass: flip every `online` agent whose `last_seen` is older
 * than the grace to `offline`, in a single UPDATE. Agents that never connected
 * (`last_seen` null, still `enrolled`), already `offline`, or `removed` are
 * left untouched. Tasks/crons are NEVER touched (DP4). Returns the count
 * transitioned (for logging + tests).
 */
export function sweepLiveness(deps: LivenessDeps): number {
  const now = deps.now?.() ?? Date.now();
  const graceMs = deps.graceMs ?? OFFLINE_GRACE_MS;
  const cutoff = now - graceMs;

  const transitioned = deps.db
    .update(agents)
    .set({ status: "offline" })
    .where(and(
      eq(agents.status, "online"),
      isNotNull(agents.lastSeen),
      lt(agents.lastSeen, cutoff),
    ))
    .returning({ id: agents.id })
    .all();

  // HOST-M3-14: evict the half-open zombie socket for every swept agent. The
  // detach runs first so the close's own `onClose` (detach + offline write) is
  // a guarded no-op; unacked outbox rows stay put and replay on reconnect.
  if (deps.sessions) {
    for (const { id } of transitioned) {
      const session = deps.sessions.get(id);
      if (!session) {
        continue;
      }
      deps.sessions.detach(id);
      try {
        session.ws.close(1001, "liveness sweep: silent past offline grace");
      }
      catch {
        // The zombie socket may already be gone — eviction is best-effort.
      }
    }
  }

  for (const { id } of transitioned) {
    deps.events?.emit("agent.offline", { agentId: id, reason: "sweep" });
  }

  if (transitioned.length > 0) {
    deps.logger?.info(
      { count: transitioned.length, graceMs, agentIds: transitioned.map(a => a.id) },
      "liveness: agents marked offline (tasks/crons untouched, DP4)",
    );
  }
  return transitioned.length;
}

/**
 * Start the periodic offline-grace sweep. The timer is `unref`'d so it never
 * keeps the process alive on its own; `stop()` clears it. A failing sweep is
 * logged and swallowed so the loop survives a transient DB error.
 */
export function startLivenessSweep(deps: LivenessDeps): LivenessHandle {
  const intervalMs = deps.intervalMs ?? HEARTBEAT_INTERVAL_MS;

  const tick = () => {
    try {
      sweepLiveness(deps);
    }
    catch (err) {
      deps.logger?.error({ err }, "liveness sweep failed");
    }
  };

  const timer = setInterval(tick, intervalMs);
  timer.unref?.();

  return {
    stop: () => clearInterval(timer),
    sweep: () => sweepLiveness(deps),
  };
}
