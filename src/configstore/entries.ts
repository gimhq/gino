// M8 — CONFIG ENTRIES (the desired set per scope + per-engine target resolution).
//
// `config_entries(scope, name, sha256, target, kind)` is the DESIRED set the
// host declares for a scope — `scope ∈ {'global', <task_id>}`, UNIQUE(scope,
// target). The host resolves the per-engine landing `target` (claude →
// `CLAUDE.md` + `.claude/skills/<n>`, codex → `AGENTS.md`); the AGENT stays
// engine-agnostic — it only ever sees `target`/`kind`, never "claude vs codex".
// A multi-engine standard lands as two rows (a `CLAUDE.md` + an `AGENTS.md`
// target); on delivery we hand each agent only the rows its engine lands.
import type { AppDatabase } from "@/db";
import type { ConfigEntry as ConfigEntryRow } from "@/db/schema";
import type { ConfigEntry } from "@/protocol/frames";
import { and, eq } from "drizzle-orm";
import { configEntries, routerState, tasks } from "@/db/schema";

/** The per-agent (non-task) scope name. */
export const GLOBAL_SCOPE = "global" as const;

/** `router_state` key holding the monotonic GLOBAL config version. */
const GLOBAL_CONFIG_VERSION_KEY = "global_config_version" as const;

/** Default engine when none is supplied (the common single-engine deployment). */
export const DEFAULT_ENGINE = "claude" as const;

/** Where an engine lands an instruction file + skill bundles in the agent project dir. */
export interface EngineTargets {
  /** The engine's instruction-file landing name. */
  instruction: string;
  /** The engine's skill-bundle parent dir; a skill `<n>` lands at `<skillDir>/<n>`. */
  skillDir: string;
}

/** The default-engine landing (also the fallback for an unknown engine). */
const CLAUDE_TARGETS: EngineTargets = { instruction: "CLAUDE.md", skillDir: ".claude/skills" };

/**
 * Per-engine landing map — the meta-driven `target` resolution lives here. The
 * agent never sees this map; it only receives resolved targets.
 */
export const ENGINE_TARGETS: Record<string, EngineTargets> = {
  claude: CLAUDE_TARGETS,
  codex: { instruction: "AGENTS.md", skillDir: ".codex/skills" },
};

/** Names that mean "the engine's instruction file" (resolved to the engine landing). */
const INSTRUCTION_ALIASES = new Set(["claude.md", "agents.md", "instructions", "instruction", "instructions.md"]);

function targetsFor(engine: string): EngineTargets {
  return ENGINE_TARGETS[engine] ?? CLAUDE_TARGETS;
}

/**
 * Resolve the engine-specific landing `target` for an entry. An explicit
 * `target` is honored verbatim (the admin already resolved it — e.g. when
 * seeding a multi-engine pair). Otherwise: a `dir` lands under the engine skill
 * dir; an instruction-role `file` lands as the engine instruction file; any
 * other `file` lands at its literal name (engine-agnostic, e.g. `design.md`).
 */
export function resolveTarget(engine: string, name: string, kind: string, explicit?: string): string {
  if (explicit && explicit.length > 0) {
    return explicit;
  }
  const t = targetsFor(engine);
  if (kind === "dir") {
    return `${t.skillDir}/${name}`;
  }
  return INSTRUCTION_ALIASES.has(name.toLowerCase()) ? t.instruction : name;
}

/**
 * Which engine OWNS a resolved target, or `null` when it is engine-agnostic
 * (a literal file every engine lands). Used to filter a scope's entries down to
 * one agent's engine on delivery.
 */
export function targetEngine(target: string): string | null {
  for (const [engine, t] of Object.entries(ENGINE_TARGETS)) {
    if (target === t.instruction || target.startsWith(`${t.skillDir}/`)) {
      return engine;
    }
  }
  return null;
}

/** An entry as supplied to the admin surface (target optional → host-resolved). */
export interface EntryInput {
  name: string;
  sha256: string;
  kind: string;
  target?: string;
}

/** Resolve a list of entry inputs against an engine into wire `ConfigEntry`s. */
export function resolveEntries(engine: string, inputs: EntryInput[]): ConfigEntry[] {
  return inputs.map(i => ({
    name: i.name,
    sha256: i.sha256,
    kind: i.kind,
    target: resolveTarget(engine, i.name, i.kind, i.target),
  }));
}

/** Last-write-wins dedup by `target` (the UNIQUE(scope, target) invariant). */
function dedupeByTarget(entries: ConfigEntry[]): ConfigEntry[] {
  const byTarget = new Map<string, ConfigEntry>();
  for (const e of entries) {
    byTarget.set(e.target, e);
  }
  return [...byTarget.values()];
}

/**
 * The host-assigned monotonic GLOBAL config version (restart-durable, kept in
 * `router_state`). The agent reconciles the global scope only when this
 * advances (agent-architecture §9b); `0` means the global set was never written.
 */
export function getGlobalConfigVersion(db: AppDatabase): number {
  const row = db.select().from(routerState).where(eq(routerState.key, GLOBAL_CONFIG_VERSION_KEY)).get();
  const parsed = row ? Number.parseInt(row.value, 10) : 0;
  return Number.isFinite(parsed) ? parsed : 0;
}

/**
 * REPLACE the desired set for a scope (delete-then-insert): the new list IS the
 * scope's complete desired state (an empty list clears it). Entries are deduped
 * by `target` to honor UNIQUE(scope, target). The delete+insert pair runs in
 * ONE transaction — a crash or insert error can never leave the scope wiped
 * (all-or-nothing). A GLOBAL write also bumps the monotonic config version in
 * the same transaction, so the version advances iff the set actually changed
 * durably. Returns the persisted set.
 */
export function setEntries(db: AppDatabase, scope: string, entries: ConfigEntry[]): ConfigEntry[] {
  const next = dedupeByTarget(entries);
  db.transaction((tx) => {
    tx.delete(configEntries).where(eq(configEntries.scope, scope)).run();
    if (next.length > 0) {
      tx
        .insert(configEntries)
        .values(next.map(e => ({ scope, name: e.name, sha256: e.sha256, target: e.target, kind: e.kind })))
        .run();
    }
    if (scope === GLOBAL_SCOPE) {
      const row = tx.select().from(routerState).where(eq(routerState.key, GLOBAL_CONFIG_VERSION_KEY)).get();
      const current = row ? Number.parseInt(row.value, 10) : 0;
      const value = String((Number.isFinite(current) ? current : 0) + 1);
      tx
        .insert(routerState)
        .values({ key: GLOBAL_CONFIG_VERSION_KEY, value })
        .onConflictDoUpdate({ target: routerState.key, set: { value } })
        .run();
    }
  });
  return next;
}

/** Raw `config_entries` rows for a scope (admin/inspection). */
export function getEntries(db: AppDatabase, scope: string): ConfigEntryRow[] {
  return db.select().from(configEntries).where(eq(configEntries.scope, scope)).all();
}

/**
 * Whether a content `sha256` is in an agent's DESIRED SET — declared in the
 * GLOBAL scope (delivered to every agent) OR in a task scope whose task is PLACED
 * on this agent (`tasks.agent_id`). This is the read-authorization for the byte
 * channel's serve-by-sha of CONFIG ARTIFACTS: an agent may fetch only the config
 * bytes the host actually declared for it — never arbitrary store bytes by sha
 * (the IDOR fix; the desired set is the host-authoritative provisioning surface).
 */
export function isShaInAgentDesiredSet(db: AppDatabase, agentId: string, sha256: string): boolean {
  const global = db
    .select({ scope: configEntries.scope })
    .from(configEntries)
    .where(and(eq(configEntries.scope, GLOBAL_SCOPE), eq(configEntries.sha256, sha256)))
    .get();
  if (global) {
    return true;
  }
  // Task scope: an entry whose `scope` is a task id placed on this agent. The
  // inner join naturally excludes the GLOBAL scope ('global' matches no task id).
  const taskScoped = db
    .select({ scope: configEntries.scope })
    .from(configEntries)
    .innerJoin(tasks, eq(tasks.id, configEntries.scope))
    .where(and(eq(configEntries.sha256, sha256), eq(tasks.agentId, agentId)))
    .get();
  return taskScoped != null;
}

/**
 * The desired set for a scope, filtered to one engine's targets — the DELIVERY
 * shape handed to an agent (register/config `config.entries`, or
 * `task_upsert.data.entries`). Engine-agnostic targets are delivered to every
 * engine; engine-owned targets only to their engine.
 */
export function resolveEntriesForEngine(db: AppDatabase, scope: string, engine: string): ConfigEntry[] {
  return getEntries(db, scope)
    .filter((r) => {
      const owner = targetEngine(r.target);
      return owner === null || owner === engine;
    })
    .map(r => ({ name: r.name, sha256: r.sha256, target: r.target, kind: r.kind }));
}
