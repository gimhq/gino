// M8 — CONFIG ENTRIES (the desired set per scope + per-engine target resolution).
//
// `config_entries(scope, name, sha256, target, kind)` is the DESIRED set the
// host declares for a scope — `scope ∈ {'global', <task_id>}`, UNIQUE(scope,
// target). The host resolves the per-engine landing `target` (claude →
// `CLAUDE.md` + `.claude/skills/<n>`, codex → `AGENTS.md`); the AGENT stays
// engine-agnostic — it only ever sees `target`/`kind`, never "claude vs codex".
// A multi-engine standard lands as two rows (a `CLAUDE.md` + an `AGENTS.md`
// target); on delivery we hand each agent only the rows its engines land.
import type { ConfigEntryRow, Storage } from "@/host/ports/storage";
import type { ConfigEntry } from "@/protocol/frames";
import { ValidationError } from "@/shared/lib/errors";
import { getArtifactBySha } from "./artifacts";
import { isSafeName } from "./store";

/** The per-agent (non-task) scope name. */
export const GLOBAL_SCOPE = "global" as const;

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

/** 64 lowercase hex chars — the canonical shape `sha256Hex` produces. */
const RE_SHA256_HEX = /^[0-9a-f]{64}$/;

/** Windows drive prefix (`C:...`) — an absolute landing shape we never accept. */
const RE_DRIVE_PREFIX = /^[a-z]:/i;

/**
 * True iff `target` is a safe RELATIVE landing path: no absolute / drive / UNC
 * shapes, no backslashes, and no `.` / `..` / empty segments — so the
 * agent-side join `<project dir>/<target>` can never escape the project root
 * (HOST-M8-08).
 */
function isSafeTarget(target: string): boolean {
  if (target.length === 0 || target.includes("\\") || RE_DRIVE_PREFIX.test(target)) {
    return false;
  }
  return target.split("/").every(seg => seg !== "" && seg !== "." && seg !== "..");
}

/**
 * Write-time validation of a scope's desired set (HOST-M8-07 / HOST-M8-08):
 * `scope` must be `'global'` or an existing task id; every entry must carry a
 * safe single-segment `name`, a safe relative `target`, and a canonical sha256
 * that an ingested `config_artifacts` row actually holds. The host never
 * declares bytes it cannot serve and never delivers a landing path that could
 * escape the agent-side project root. Throws a 422 `ValidationError` whose
 * details name every offending entry.
 */
function assertValidEntries(storage: Storage, scope: string, entries: ConfigEntry[]): void {
  const problems: string[] = [];
  if (scope !== GLOBAL_SCOPE) {
    const task = storage.tasks.get(scope);
    if (!task) {
      problems.push(`scope ${JSON.stringify(scope)} is neither 'global' nor an existing task id`);
    }
  }
  for (const e of entries) {
    if (!isSafeName(e.name)) {
      problems.push(`entry ${JSON.stringify(e.name)}: name is not a single safe path segment`);
    }
    if (!RE_SHA256_HEX.test(e.sha256)) {
      problems.push(`entry ${JSON.stringify(e.name)}: sha256 must be 64 lowercase hex chars`);
    }
    else if (!getArtifactBySha(storage, e.sha256)) {
      problems.push(`entry ${JSON.stringify(e.name)}: no ingested artifact holds sha256 ${e.sha256}`);
    }
    if (!isSafeTarget(e.target)) {
      problems.push(`entry ${JSON.stringify(e.name)}: target ${JSON.stringify(e.target)} must be a relative path with no traversal`);
    }
  }
  if (problems.length > 0) {
    throw new ValidationError("config entries rejected", { problems });
  }
}

/**
 * The host-assigned monotonic GLOBAL config version (restart-durable, kept in
 * `router_state`). The agent reconciles the global scope only when this
 * advances (agent-architecture §9b); `0` means the global set was never written.
 */
export function getGlobalConfigVersion(storage: Storage): number {
  return storage.config.globalVersion();
}

/**
 * REPLACE the desired set for a scope (delete-then-insert): the new list IS the
 * scope's complete desired state (an empty list clears it). Entries are deduped
 * by `target` to honor UNIQUE(scope, target). The delete+insert pair runs in
 * ONE atomic unit (`ConfigRepo.replaceScope`) — a crash or insert error can
 * never leave the scope wiped (all-or-nothing). A GLOBAL write also bumps the
 * monotonic config version in the same unit, so the version advances iff the
 * set actually changed durably. Every write is validated first (HOST-M8-07 /
 * HOST-M8-08) — an unknown scope, an undeclarable sha256, or a
 * traversal-shaped name/target is rejected with a 422 before anything is
 * touched. Returns the persisted set.
 */
export function setEntries(storage: Storage, scope: string, entries: ConfigEntry[]): ConfigEntry[] {
  const next = dedupeByTarget(entries);
  assertValidEntries(storage, scope, next);
  storage.config.replaceScope(scope, next.map(e => ({ name: e.name, sha256: e.sha256, target: e.target, kind: e.kind })));
  return next;
}

/** Raw `config_entries` rows for a scope (admin/inspection). */
export function getEntries(storage: Storage, scope: string): ConfigEntryRow[] {
  return storage.config.entriesFor(scope);
}

/**
 * Drop a scope's desired set entirely — the task lifecycle cleanup
 * (HOST-M8-10): a closed task's `config_entries` rows leave the desired set so
 * they stop pinning artifacts in the GC root set. Task scopes carry no config
 * version, so nothing is bumped.
 */
export function clearEntries(storage: Storage, scope: string): void {
  storage.config.clearScope(scope);
}

/**
 * Whether a content `sha256` is in an agent's DESIRED SET — declared in the
 * GLOBAL scope (delivered to every agent) OR in a task scope whose task is PLACED
 * on this agent (`tasks.agent_id`). This is the read-authorization for the byte
 * channel's serve-by-sha of CONFIG ARTIFACTS: an agent may fetch only the config
 * bytes the host actually declared for it — never arbitrary store bytes by sha
 * (the IDOR fix; the desired set is the host-authoritative provisioning surface).
 */
export function isShaInAgentDesiredSet(storage: Storage, agentId: string, sha256: string): boolean {
  const scopes = storage.config.scopesWithSha(sha256);
  if (scopes.includes(GLOBAL_SCOPE)) {
    return true;
  }
  // Task scope: an entry whose `scope` is a task id placed on this agent. The
  // task lookup naturally excludes the GLOBAL scope ('global' matches no task id).
  return scopes.some(scope => storage.tasks.get(scope)?.agentId === agentId);
}

/**
 * The desired set for a scope, filtered to a set of engines — the DELIVERY
 * shape handed to an agent (register/config `config.entries`, live
 * `config_update`). A multi-engine agent receives the UNION of all its
 * declared engines' targets (HOST-M8-03), never just its first capability.
 * Engine-agnostic targets are delivered to every engine; an engine-owned
 * target only when the agent declared that engine. An empty list falls back to
 * the default engine.
 */
export function resolveEntriesForEngines(storage: Storage, scope: string, engines: string[]): ConfigEntry[] {
  const declared = new Set(engines.length > 0 ? engines : [DEFAULT_ENGINE]);
  return getEntries(storage, scope)
    .filter((r) => {
      const owner = targetEngine(r.target);
      return owner === null || declared.has(owner);
    })
    .map(r => ({ name: r.name, sha256: r.sha256, target: r.target, kind: r.kind }));
}

/**
 * Single-engine delivery shape (`task_upsert.data.entries` — a task carries
 * exactly one engine). For an agent's global scope use
 * `resolveEntriesForEngines` with its full capability list.
 */
export function resolveEntriesForEngine(storage: Storage, scope: string, engine: string): ConfigEntry[] {
  return resolveEntriesForEngines(storage, scope, [engine]);
}
