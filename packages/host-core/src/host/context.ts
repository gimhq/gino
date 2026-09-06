// HOST — per-instance context (replaces the process-wide singletons).
//
// One `HostContext` per host instance: it owns the live session registry
// (M3), the launcher registry (M3), the metrics sink, the resolved options +
// the `Storage` port (repositories + blob store), the clock/id seams, and the
// background-loop lifecycle (`start` / `stop`). `createHost` (host.ts) layers
// the public namespaces on top; the standalone app builds one in `bootstrap`
// and threads it through the hono context; tests and embedding hosts build
// their own.
import type { Launcher } from "../agents/launchers";
import type { Logger } from "../logger";
import type { HostEvents } from "./events";
import type { Metrics } from "./metrics";
import type { HostOptions, HostOptionsInput } from "./options";
import type { Storage } from "./ports/storage";
import type { Wire } from "./wire";
import { startLaunchReaper } from "../agents/launch-reaper";
import { DEFAULT_LAUNCHERS, LauncherRegistry } from "../agents/launchers";
import { startLivenessSweep } from "../agents/liveness";
import { SessionRegistry } from "../agents/sessions";
import { startConfigStoreGc } from "../configstore";
import { generateId, ulid } from "../platform/ids";
import { startOutboxGc } from "../protocol/outbox";
import { createHostEvents } from "./events";
import { noopMetrics } from "./metrics";
import { resolveHostOptions } from "./options";
import { createWire } from "./wire";

export interface HostContext {
  readonly logger: Logger;
  readonly sessions: SessionRegistry;
  readonly launchers: LauncherRegistry;
  readonly metrics: Metrics;
  /** Typed post-write event hints (see `events.ts`); one emitter per context. */
  readonly events: HostEvents;
  /** Resolved per-instance options (defaults mirror the env schema). */
  readonly options: HostOptions;
  /** The `Storage` port (host-sdk-design §8.3) — repositories + the blob store. */
  readonly storage: Storage;
  readonly clock: () => number;
  readonly ids: { readonly generateId: typeof generateId; readonly ulid: typeof ulid };
  /** The framework-free `/agent/*` data plane (host-sdk-design §3.2); the Hono shim mounts it 1:1. */
  readonly wire: Wire;
  /**
   * Start the background loops (liveness sweep, outbox GC, launch reaper,
   * config-store GC). Idempotent; the timers are `unref`'d so they never hold
   * the process open.
   */
  readonly start: () => void;
  /**
   * Stop everything, in order: registered services (reverse registration
   * order) → the core loops → close every live agent socket (`1001`) → close
   * the storage. Idempotent; a throwing `stop()` is logged and the sequence continues.
   */
  readonly stop: () => void;
  /** Register an app-owned background service (e.g. the webhook pump); stopped by `stop()` in reverse registration order, before the core loops. */
  readonly registerService: (service: { readonly stop: () => void }) => void;
}

/** Close code sent to every attached agent socket on `stop()` (going away). */
const SHUTDOWN_CLOSE_CODE = 1001;

interface Stoppable {
  readonly stop: () => void;
}

export interface CreateHostContextOptions {
  /** The `Storage` port (repositories + blob store); `stop()` closes it. */
  readonly storage: Storage;
  readonly logger: Logger;
  /** Launchers to register; defaults to `DEFAULT_LAUNCHERS` (manual only — apps add `createDockerLauncher({ spawn })`). */
  readonly launchers?: readonly Launcher[];
  /** Metrics sink; defaults to `noopMetrics`. */
  readonly metrics?: Metrics;
  /** Event emitter to share/inject; defaults to a fresh `createHostEvents(logger)`. */
  readonly events?: HostEvents;
  /** Per-instance options; missing keys fall back to `DEFAULT_HOST_OPTIONS`. */
  readonly options?: HostOptionsInput;
  /** Clock seam; defaults to `Date.now`. */
  readonly clock?: () => number;
  /** Id generators; default to the platform `generateId` / `ulid`. */
  readonly ids?: Partial<HostContext["ids"]>;
}

/** Build a fresh context: every call gets its own session + launcher registries. */
export function createHostContext(opts: CreateHostContextOptions): HostContext {
  const { storage, logger } = opts;
  const options = resolveHostOptions(opts.options);
  const sessions = new SessionRegistry();
  const launchers = new LauncherRegistry(opts.launchers ?? DEFAULT_LAUNCHERS);
  const events = opts.events ?? createHostEvents(logger);

  const services: Stoppable[] = [];
  let loops: Stoppable[] | null = null;
  let stopped = false;

  const stopOne = (name: string, target: Stoppable): void => {
    try {
      target.stop();
    }
    catch (err) {
      logger.warn({ err, name }, "host stop step failed");
    }
  };

  const start = (): void => {
    if (loops || stopped) {
      return;
    }
    loops = [
      startLivenessSweep({ storage, logger, sessions, events, graceMs: options.offlineGraceMs, intervalMs: options.heartbeatIntervalMs }),
      startOutboxGc({ storage, logger }),
      startLaunchReaper({ storage, logger, resolveLauncher: launchers.resolve }),
      startConfigStoreGc({ storage, logger, enabled: options.configGc.enabled, maxAgeMs: options.configGc.maxAgeMs, intervalMs: options.configGc.intervalMs }),
    ];
  };

  const stop = (): void => {
    if (stopped) {
      return;
    }
    stopped = true;
    for (const service of services.splice(0).reverse()) {
      stopOne("service", service);
    }
    for (const loop of loops ?? []) {
      stopOne("loop", loop);
    }
    loops = null;
    sessions.closeAll(SHUTDOWN_CLOSE_CODE, "host shutting down");
    // The sqlite adapter's `close()` closes the db — never also `db.close()`.
    stopOne("storage.close", { stop: () => storage.close() });
  };

  const metrics = opts.metrics ?? noopMetrics;
  return {
    logger,
    sessions,
    launchers,
    metrics,
    events,
    options,
    storage,
    clock: opts.clock ?? Date.now,
    ids: { generateId: opts.ids?.generateId ?? generateId, ulid: opts.ids?.ulid ?? ulid },
    wire: createWire({ storage, sessions, logger, metrics, events, options }),
    start,
    stop,
    registerService: (service) => {
      services.push(service);
    },
  };
}
