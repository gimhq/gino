import type { Metrics } from "./metrics";
import type { HostOptions, HostOptionsInput } from "./options";
// HOST — per-instance context (replaces the process-wide singletons).
//
// One `HostContext` per host instance: it owns the live session registry
// (M3), the launcher registry (M3), the metrics sink, the resolved options +
// blob store, and the clock/id seams.
// The standalone app builds one in `bootstrap` and threads it through the
// hono context; tests and embedding hosts build their own.
import type { Launcher } from "@/agents/launchers";
import type { BlobStore } from "@/configstore";
import type { AppDatabase } from "@/db";
import type { Logger } from "@/shared/lib/logger";
import { DEFAULT_LAUNCHERS, LauncherRegistry } from "@/agents/launchers";
import { SessionRegistry } from "@/agents/sessions";
import { createStore } from "@/configstore";
import { generateId, ulid } from "@/platform/ids";
import { noopMetrics } from "./metrics";
import { resolveHostOptions } from "./options";

export interface HostContext {
  readonly db: AppDatabase;
  readonly logger: Logger;
  readonly sessions: SessionRegistry;
  readonly launchers: LauncherRegistry;
  readonly metrics: Metrics;
  /** Resolved per-instance options (defaults mirror the env schema). */
  readonly options: HostOptions;
  /** The unified blob store rooted at `options.storeDir`, built once per context. */
  readonly store: BlobStore;
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
  /** Per-instance options; missing keys fall back to `DEFAULT_HOST_OPTIONS`. */
  readonly options?: HostOptionsInput;
  /** Clock seam; defaults to `Date.now`. */
  readonly clock?: () => number;
  /** Id generators; default to the platform `generateId` / `ulid`. */
  readonly ids?: Partial<HostContext["ids"]>;
}

/** Build a fresh context: every call gets its own session + launcher registries. */
export function createHostContext(opts: CreateHostContextOptions): HostContext {
  const options = resolveHostOptions(opts.options);
  return {
    db: opts.db,
    logger: opts.logger,
    sessions: new SessionRegistry(),
    launchers: new LauncherRegistry(opts.launchers ?? DEFAULT_LAUNCHERS),
    metrics: opts.metrics ?? noopMetrics,
    options,
    store: createStore(options.storeDir),
    clock: opts.clock ?? Date.now,
    ids: { generateId: opts.ids?.generateId ?? generateId, ulid: opts.ids?.ulid ?? ulid },
  };
}
