import type { Metrics } from "./metrics";
// HOST — per-instance context (replaces the process-wide singletons).
//
// One `HostContext` per host instance: it owns the live session registry
// (M3), the launcher registry (M3), the metrics sink, and the clock/id seams.
// The standalone app builds one in `bootstrap` and threads it through the
// hono context; tests and embedding hosts build their own.
import type { Launcher } from "@/agents/launchers";
import type { AppDatabase } from "@/db";
import type { Logger } from "@/shared/lib/logger";
import { DEFAULT_LAUNCHERS, LauncherRegistry } from "@/agents/launchers";
import { SessionRegistry } from "@/agents/sessions";
import { generateId, ulid } from "@/platform/ids";
import { noopMetrics } from "./metrics";

export interface HostContext {
  readonly db: AppDatabase;
  readonly logger: Logger;
  readonly sessions: SessionRegistry;
  readonly launchers: LauncherRegistry;
  readonly metrics: Metrics;
  readonly clock: () => number;
  readonly ids: { readonly generateId: typeof generateId; readonly ulid: typeof ulid };
}

export interface CreateHostContextOptions {
  readonly db: AppDatabase;
  readonly logger: Logger;
  /** Launchers to register; defaults to `DEFAULT_LAUNCHERS` (manual + docker). */
  readonly launchers?: readonly Launcher[];
  /** Metrics sink; defaults to `noopMetrics`. */
  readonly metrics?: Metrics;
  /** Clock seam; defaults to `Date.now`. */
  readonly clock?: () => number;
  /** Id generators; default to the platform `generateId` / `ulid`. */
  readonly ids?: Partial<HostContext["ids"]>;
}

/** Build a fresh context: every call gets its own session + launcher registries. */
export function createHostContext(opts: CreateHostContextOptions): HostContext {
  return {
    db: opts.db,
    logger: opts.logger,
    sessions: new SessionRegistry(),
    launchers: new LauncherRegistry(opts.launchers ?? DEFAULT_LAUNCHERS),
    metrics: opts.metrics ?? noopMetrics,
    clock: opts.clock ?? Date.now,
    ids: { generateId: opts.ids?.generateId ?? generateId, ulid: opts.ids?.ulid ?? ulid },
  };
}
