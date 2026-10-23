import type { ControlDeps, ControlResult, LaunchAgentResult, LaunchInput, RemoveResult } from "../agents/control";
import type { InspectState, Launcher } from "../agents/launchers";
import type { PtySession } from "../agents/pty";
import type { EnrollInput, EnrollResult, RemoteOpsFlags } from "../agents/registry";
import type { GetFileResult } from "../agents/remote";
import type { EntryInput, IngestArtifactInput } from "../configstore";
import type { Logger } from "../logger";
import type { TranscriptReadOptions, TranscriptStats } from "../msgstore";
import type { ConfigEntry, FsOpData, FsResponseData, PtyOpenData } from "../protocol/frames";
import type { DispatchResult } from "../tasks";
import type { CronUpsertInput, CronUpsertResult, MessageInput, SendMessageResult } from "../tasks/orchestrator";
import type { CreateTaskInput, ListTasksFilter } from "../tasks/store";
import type { HostContext } from "./context";
import type { HostEvents } from "./events";
import type { Metrics } from "./metrics";
import type { HostOptions, HostOptionsInput } from "./options";
import type {
  AgentRow,
  ConfigArtifactRow,
  ConfigEntryRow,
  CronRow,
  MessageRow,
  OutboxRow,
  Storage,
  TaskRow,
  ThreadRunRow,
} from "./ports/storage";
// HOST — `createHost` (docs/host-sdk-design.md §3.1): the embedder's entry point.
//
// A `Host` is a `HostContext` plus the public namespaces (`agents`, `tasks`,
// `crons`, `transcript`, `config`). Each namespace is a plain object of arrow
// functions binding the existing domain functions to this instance's
// `storage` / `sessions` / `launchers` / `events` — no logic lives here, so an
// embedder and the standalone edge routers go through exactly the same code.
// Return types are the domain's own; errors are the existing `AppError`
// subclasses so an adapter can map them to HTTP without the SDK knowing HTTP.
import {
  grantAgentOwner,
  inspectAgent,
  launchAgent,
  removeAgentControl,
  restartAgent,
  revokeAgentOwner,
  setAgentAccessControl,
  setAgentRemoteOpsControl,
  stopAgent,
} from "../agents/control";
import { enrollAgent, getAgent, listAgents } from "../agents/registry";
import {
  clearEntries,
  getEntries,
  getGlobalConfigVersion,
  GLOBAL_SCOPE,
  ingestArtifact,
  resolveEntriesForEngines,
  setEntries,
} from "../configstore";
import { getTaskThreads, getThreadRun, getTranscript, getTranscriptStats } from "../msgstore";
import { NotFoundError } from "../shared/lib/errors";
import {
  assignTaskToAgent,
  closeTask,
  createTask,

  emitCronDelete,
  emitCronUpsert,
  emitInterrupt,
  getTask,
  listTaskCrons,
  listTasks,
  placeTask,
  requireTask,
  sendTaskMessage,
  setTaskEntries,
  transitionTaskStatus,
} from "../tasks";
import { createHostContext } from "./context";

/**
 * Everything `createHost` accepts. Only `storage` and `logger` are REQUIRED;
 * every other field has a default, and the option groups are flattened onto
 * `HostOptions` (see `DEFAULT_HOST_OPTIONS` for the resolved values).
 */
export interface CreateHostOptions {
  /** REQUIRED. The `Storage` port (repositories + blob store); `host.stop()` closes it. */
  readonly storage: Storage;
  /** REQUIRED. Pass `noopLogger` to discard every line. */
  readonly logger: Logger;
  /** Launchers to register; defaults to manual only (apps add `createDockerLauncher({ spawn })`). */
  readonly launchers?: readonly Launcher[];
  /** Metrics sink; defaults to `noopMetrics`. */
  readonly metrics?: Metrics;
  /** Event emitter to share/inject; defaults to a fresh `createHostEvents(logger)`. */
  readonly events?: HostEvents;
  /** Id generator seams (`generateId`, `ulid`); default to the platform ones. Override for deterministic tests. */
  readonly ids?: Partial<HostContext["ids"]>;
  /** Clock seam; defaults to `Date.now`. */
  readonly clock?: () => number;
  /**
   * Protocol credentials + liveness cadence (the only secrets the SDK owns).
   * Defaults: `registerToken: ""` (register fails closed), `registrationEnabled:
   * true`, `heartbeatIntervalMs: 30_000`, `offlineGraceMs: 90_000`.
   */
  readonly agents?: Partial<Pick<HostOptions, "registerToken" | "registrationEnabled" | "heartbeatIntervalMs" | "offlineGraceMs">>;
  /** Byte-ingest body cap; defaults to 64 MiB. */
  readonly attachments?: Partial<Pick<HostOptions, "maxUploadBytes">>;
  /** Root of the content-addressed blob store (config artifacts + attachments); defaults to `data/store`. */
  readonly storeDir?: string;
  /** Unreferenced-artifact GC; defaults to disabled (hourly, 7-day age when enabled). */
  readonly configGc?: HostOptionsInput["configGc"];
  /** Webhook delivery policy read by `@gino/host-channels`; defaults to 10 s / 5 attempts / 15 s retry. */
  readonly webhooks?: HostOptionsInput["webhooks"];
}

/**
 * Agent identity, access policy and out-of-band runtime control.
 *
 * Every method throws `NotFoundError` for an unknown agent; `stop`, `restart`,
 * `grant`, `revoke` and `setAccess` additionally throw `AppError` with status
 * `410` for a `removed` agent, which is terminal. `remove` is idempotent —
 * repeating it re-applies the same state rather than failing.
 */
export interface AgentsApi {
  /**
   * Mint an agent identity: a ULID id plus a per-agent connect token, returned
   * in plaintext ONCE (only its hash is stored). Prefer letting the agent
   * enroll itself through `host.wire.register`, which applies the register
   * token gate — this method is the trusted-caller path and takes `access`
   * at face value.
   */
  readonly enroll: (input: EnrollInput) => EnrollResult;
  /** Every agent including `removed` ones, newest first. */
  readonly list: () => AgentRow[];
  /** One agent, or `undefined` when the id is unknown. */
  readonly get: (agentId: string) => AgentRow | undefined;
  /**
   * Tear down the runtime, then do the durable bookkeeping unconditionally:
   * revoke the connect token, orphan the agent's tasks (kept as history),
   * purge its undeliverable outbox rows and force-close its live socket.
   * IDEMPOTENT — repeating it on a `removed` agent re-runs the local cleanup
   * and reports `orphanedTasks: 0`. A launcher failure degrades to the
   * WebSocket-close path instead of aborting.
   * @throws NotFoundError unknown agent.
   */
  readonly remove: (agentId: string) => Promise<RemoveResult>;
  /**
   * Stop the runtime out-of-band via its launcher, or fall back to a durable
   * `close` frame when the launcher cannot (the `manual` default). Tasks and
   * crons survive; the agent resumes them on reconnect.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly stop: (agentId: string) => Promise<ControlResult>;
  /**
   * Restart the runtime via its launcher, or fall back to a `close` frame and
   * let the agent's own supervisor bring it back.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly restart: (agentId: string) => Promise<ControlResult>;
  /**
   * Ask the launcher for the runtime's coarse state. `"unknown"` whenever the
   * launcher cannot inspect — which is always true for `manual`.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly inspect: (agentId: string) => Promise<InspectState>;
  /**
   * Permit an owner on a `dedicated` agent. Idempotent on `(agentId, ownerId)`.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly grant: (agentId: string, ownerId: string) => void;
  /**
   * Withdraw an owner's grant. Idempotent; already-placed tasks keep their pin.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly revoke: (agentId: string, ownerId: string) => void;
  /**
   * Switch the access policy: `dedicated` (granted owners only) or `public`
   * (any owner may be placed here). An agent never sets this itself — it is
   * always an operator decision.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly setAccess: (agentId: string, access: "dedicated" | "public") => void;
  /**
   * Enable or disable engine-free operation groups on an agent. All groups are
   * off until an operator turns them on here; the agent cannot enable them
   * itself, and once enabled it cannot refuse. Returns the stored switches.
   * @throws NotFoundError unknown agent; `AppError` 410 `AGENT_REMOVED`.
   */
  readonly setRemoteOps: (agentId: string, flags: RemoteOpsFlags) => Required<RemoteOpsFlags>;
  /** Open a live terminal, resolving only after the agent confirms readiness. */
  readonly openPty: (agentId: string, options?: PtyOpenData, signal?: AbortSignal) => Promise<PtySession>;
  /**
   * Run one metadata operation (list, stat, mkdir, remove, rename) on a
   * connected agent without an engine. Paths are NOT confined: a relative path
   * resolves against the agent's workspace, an absolute one is used as given.
   * Live-only — never queued or replayed. Resolves with the agent's answer,
   * including a refusal (`ok: false`, e.g. `disabled` while the group is off).
   * @throws NotFoundError unknown agent; `AppError` 409 `AGENT_OFFLINE`,
   * 504 `AGENT_TIMEOUT`, 502 `AGENT_DISCONNECTED`.
   */
  readonly fs: (agentId: string, request: FsOpData) => Promise<FsResponseData>;
  /**
   * Write a whole file on a connected agent. The bytes travel over the
   * attachment byte channel, are verified against their sha256 and replace the
   * destination atomically. Same path rules, answer and errors as `fs`.
   */
  readonly putFile: (agentId: string, path: string, bytes: Uint8Array) => Promise<FsResponseData>;
  /**
   * Read a whole file from a connected agent over the attachment byte channel.
   * Resolves with the verified bytes, or the agent's refusal (`ok: false`).
   * Same path rules and errors as `fs`.
   */
  readonly getFile: (agentId: string, path: string) => Promise<GetFileResult>;
  /**
   * Start a runtime through a launcher and record a pending launch keyed by a
   * single-use nonce. The agent's later `register` echoes the nonce, which
   * binds the new agent id to this launch. Returns before the agent exists —
   * correlation happens on register, and an uncorrelated launch is reaped by
   * TTL.
   * @throws AppError 422 `LAUNCH_UNSUPPORTED` when the launcher cannot launch.
   */
  readonly launch: (input: LaunchInput) => Promise<LaunchAgentResult>;
}

/**
 * Task identity, placement and the host→agent command path.
 *
 * A task lives on exactly ONE agent for life: placement is set-once and there
 * is no migration — moving work means creating a new task. Every command
 * method requires a placed task and throws `AppError` 409 `TASK_NOT_PLACED`
 * otherwise. Frames ride the durable outbox, so `send`, `interrupt` and the
 * cron commands do not require the agent to be online, with the single stated
 * exception of `interrupt`.
 */
export interface TasksApi {
  /**
   * Mint a task (ULID id) at `pending-assignment` with no placement. Any
   * `entries` are persisted as the task's config scope with the per-engine
   * `target` resolved from `input.engine`.
   */
  readonly create: (input: CreateTaskInput) => TaskRow;
  /** One task, or `undefined` when the id is unknown. */
  readonly get: (taskId: string) => TaskRow | undefined;
  /** Tasks matching every given filter key, newest first. */
  readonly list: (filter?: ListTasksFilter) => TaskRow[];
  /**
   * Manual placement: pin a SPECIFIC agent, which must exist, not be `removed`,
   * hold the task's engine, and have the task's owner permitted on it.
   * SET-ONCE and idempotent — re-assigning the same agent is a no-op reported
   * as `already-placed` and touches nothing, so a retry never revives a closed
   * task. Emits `task.placed` on a fresh pin.
   * @throws NotFoundError unknown task; `AppError` 422 (`AGENT_UNAVAILABLE`,
   * `ENGINE_MISMATCH`, `NOT_PERMITTED`); `AppError` 409 `TASK_ALREADY_PLACED`
   * when the task is already pinned to a different agent.
   */
  readonly assign: (taskId: string, agentId: string) => DispatchResult;
  /**
   * Auto placement: rank the agents this owner may use — dedicated before
   * public, then spare capacity, then least loaded, then online — and pin the
   * best. Capacity orders candidates but never spills work to a different
   * agent. With no permitted agent the task stays `pending-assignment` and the
   * result reports `no-permitted-agent` rather than throwing. Emits
   * `task.placed` on a fresh pin.
   * @throws NotFoundError unknown task.
   */
  readonly place: (taskId: string) => DispatchResult;
  /**
   * Deliver input to the task. The FIRST send also seeds the agent with a
   * `task_upsert` carrying the task definition and its config entries; later
   * sends deliver a `message` only. The thread is the task's current one, or a
   * fresh one on the first send or when `newThread` is set. Both frames are
   * enqueued durably and replay on reconnect, so this works while the agent is
   * offline. Delivery is confirmed per row by `msg_ack`, never by a per-task
   * cursor.
   * @throws NotFoundError unknown task; `AppError` 409 `TASK_NOT_PLACED`.
   */
  readonly send: (taskId: string, input: MessageInput) => SendMessageResult;
  /**
   * Cancel the task's current turn, leaving the engine alive. LIVE-ONLY: it is
   * never queued and never replayed, because a stale cancel must not fire
   * against some arbitrary future turn. The frame carries `taskId`, so another
   * task's in-flight work is never killed.
   * @throws NotFoundError unknown task; `AppError` 409 `TASK_NOT_PLACED`;
   * `AppError` 409 `AGENT_OFFLINE`.
   */
  readonly interrupt: (taskId: string) => OutboxRow;
  /**
   * Mark the task `closed` and drop its config scope, so its artifacts stop
   * pinning the GC root set. Re-opening does not resurrect them. No frame is
   * emitted — pair it with `interrupt` when the agent should also stop working.
   * @throws NotFoundError unknown task.
   */
  readonly close: (taskId: string) => TaskRow;
  /**
   * REPLACE the task's config/skill desired set, resolving each entry's
   * per-engine `target`. An empty list clears it. The set is re-materialized
   * into `task_upsert.data.entries` on the next seed.
   * @throws NotFoundError unknown task.
   */
  readonly setEntries: (taskId: string, entries: EntryInput[]) => void;
  /**
   * Record an externally decided status. Status is free-form and the host never
   * auto-terminates a task; `closed` additionally drops the config scope.
   * @throws NotFoundError unknown task.
   */
  readonly transition: (taskId: string, status: string) => TaskRow;
}

/**
 * Scheduled work. Crons LIVE ON THE AGENT: the host emits commands and keeps a
 * read-only mirror of what the agent reports back, and schedules nothing
 * itself. Every method requires a placed task.
 */
export interface CronsApi {
  /**
   * Emit a `cron_upsert` command — create when `cronId` is omitted (the host
   * mints one so it can address later commands), update when it is given.
   * `data.prompt` is required. Seeds a never-seeded task first, so the agent
   * has the task the cron references. The prompt is written into the mirror
   * here because `cron_state` never echoes it back.
   * @throws NotFoundError unknown task; `AppError` 409 `TASK_NOT_PLACED`.
   */
  readonly upsert: (taskId: string, input: CronUpsertInput) => CronUpsertResult;
  /**
   * Emit a `cron_delete` command, removing the trigger from the agent.
   * @throws NotFoundError unknown task; `AppError` 409 `TASK_NOT_PLACED`.
   */
  readonly delete: (taskId: string, cronId: string) => OutboxRow;
  /** The read-only `cron_state` mirror for a task, most recently updated first. */
  readonly list: (taskId: string) => CronRow[];
}

/**
 * The durable record — read this, not the events. Ordered by `seq`, never by
 * arrival, and re-readable by cursor after a missed event or a restart.
 */
export interface TranscriptApi {
  /**
   * A thread's transcript ordered by `seq`. Page with `afterSeq`; `limit`
   * defaults to and is capped at 1000 rows. An unknown thread reads empty
   * rather than throwing.
   */
  readonly get: (threadId: string, opts?: TranscriptReadOptions) => MessageRow[];
  /** Row count and highest `seq` for a thread — the cheap completeness check. */
  readonly stats: (threadId: string) => TranscriptStats;
  /** A task's thread ids, earliest first. */
  readonly threads: (taskId: string) => string[];
  /** A thread's terminal outcome mirror, or `undefined` while it has not finished. */
  readonly run: (threadId: string) => ThreadRunRow | undefined;
}

/**
 * The content-addressed config/skill store: immutable artifacts, plus a
 * per-scope desired set the agent reconciles against. `global` is the scope
 * delivered on register and config; a task id is a task-local scope.
 */
export interface ConfigApi {
  /**
   * Store bytes content-addressed and index them. Idempotent on
   * `(name, sha256)` — a repeat ingest of identical bytes rewrites only `kind`
   * and `contentType`.
   * @throws ValidationError an unsafe artifact name.
   */
  readonly putArtifact: (input: IngestArtifactInput) => ConfigArtifactRow;
  /**
   * REPLACE a scope's desired set atomically and return what was stored. A
   * write to `global` also bumps the global config version by one in the same
   * unit, which is what makes agents re-fetch. Entries are deduped by `target`,
   * last write wins. On any failure the previous set and the version are intact.
   * @throws ValidationError unknown sha, unsafe name, or unknown task scope.
   */
  readonly setEntries: (scope: string, entries: ConfigEntry[]) => ConfigEntry[];
  /** One scope's stored entries, in insertion order. */
  readonly entries: (scope: string) => ConfigEntryRow[];
  /** Drop a scope's set WITHOUT bumping the global version. Idempotent. */
  readonly clear: (scope: string) => void;
  /**
   * The `global` desired set filtered to the engines an agent advertises —
   * exactly what `host.wire.register` and `host.wire.configFor` deliver.
   * Entries whose target no engine owns are included for every engine; an
   * empty `engines` list falls back to the default engine.
   */
  readonly resolveFor: (engines: string[]) => ConfigEntry[];
  /** The monotonic global config version; `0` before the global set is first written. */
  readonly version: () => number;
}

/**
 * An embeddable control plane: a `HostContext` (storage, sessions, launchers,
 * events, wire, lifecycle) plus the five operator namespaces. Build one with
 * `createHost`, call `start()` to run the background loops and `stop()` to
 * wind everything down.
 */
export interface Host extends HostContext {
  readonly agents: AgentsApi;
  readonly tasks: TasksApi;
  readonly crons: CronsApi;
  readonly transcript: TranscriptApi;
  readonly config: ConfigApi;
}

/** The §3.1 option groups flattened onto the per-instance `HostOptions`. */
function hostOptionsInput(opts: CreateHostOptions): HostOptionsInput {
  return {
    ...opts.agents,
    ...opts.attachments,
    ...(opts.storeDir !== undefined ? { storeDir: opts.storeDir } : {}),
    ...(opts.configGc ? { configGc: opts.configGc } : {}),
    ...(opts.webhooks ? { webhooks: opts.webhooks } : {}),
  };
}

function agentsApi(ctx: HostContext): AgentsApi {
  const { storage, sessions, launchers, logger, events } = ctx;
  const deps: ControlDeps = { storage, sender: sessions, sessions, resolveLauncher: launchers.resolve, logger, events };
  return {
    enroll: input => enrollAgent(storage, input),
    list: () => listAgents(storage),
    get: agentId => getAgent(storage, agentId),
    remove: agentId => removeAgentControl(deps, agentId),
    stop: agentId => stopAgent(deps, agentId),
    restart: agentId => restartAgent(deps, agentId),
    inspect: agentId => inspectAgent(deps, agentId),
    grant: (agentId, ownerId) => grantAgentOwner(deps, agentId, ownerId),
    revoke: (agentId, ownerId) => revokeAgentOwner(deps, agentId, ownerId),
    setAccess: (agentId, access) => setAgentAccessControl(deps, agentId, access),
    setRemoteOps: (agentId, flags) => setAgentRemoteOpsControl(deps, agentId, flags),
    openPty: async (agentId, options = {}, signal) => {
      if (!getAgent(storage, agentId))
        throw new NotFoundError("agent", agentId);
      return ctx.remote.openPty(agentId, options, signal);
    },
    fs: async (agentId, request) => {
      if (!getAgent(storage, agentId)) {
        throw new NotFoundError("agent", agentId);
      }
      return ctx.remote.fs(agentId, request);
    },
    putFile: async (agentId, path, bytes) => {
      if (!getAgent(storage, agentId)) {
        throw new NotFoundError("agent", agentId);
      }
      return ctx.remote.putFile(agentId, path, bytes);
    },
    getFile: async (agentId, path) => {
      if (!getAgent(storage, agentId)) {
        throw new NotFoundError("agent", agentId);
      }
      return ctx.remote.getFile(agentId, path);
    },
    launch: input => launchAgent(deps, input),
  };
}

function tasksApi(ctx: HostContext): TasksApi {
  const { storage, sessions, events } = ctx;
  return {
    create: input => createTask(storage, input),
    get: taskId => getTask(storage, taskId),
    list: filter => listTasks(storage, filter),
    assign: (taskId, agentId) => assignTaskToAgent(storage, taskId, agentId, events),
    place: taskId => placeTask(storage, taskId, events),
    send: (taskId, input) => sendTaskMessage(storage, sessions, requireTask(storage, taskId), input),
    interrupt: taskId => emitInterrupt(storage, sessions, requireTask(storage, taskId)),
    close: taskId => closeTask(storage, taskId),
    setEntries: (taskId, entries) => setTaskEntries(storage, taskId, entries),
    transition: (taskId, status) => transitionTaskStatus(storage, taskId, status),
  };
}

function cronsApi(ctx: HostContext): CronsApi {
  const { storage, sessions } = ctx;
  return {
    upsert: (taskId, input) => emitCronUpsert(storage, sessions, requireTask(storage, taskId), input),
    delete: (taskId, cronId) => emitCronDelete(storage, sessions, requireTask(storage, taskId), cronId),
    list: taskId => listTaskCrons(storage, taskId),
  };
}

function transcriptApi({ storage }: HostContext): TranscriptApi {
  return {
    get: (threadId, opts) => getTranscript(storage, threadId, opts),
    stats: threadId => getTranscriptStats(storage, threadId),
    threads: taskId => getTaskThreads(storage, taskId),
    run: threadId => getThreadRun(storage, threadId),
  };
}

function configApi({ storage }: HostContext): ConfigApi {
  return {
    putArtifact: input => ingestArtifact(storage, input),
    setEntries: (scope, entries) => setEntries(storage, scope, entries),
    entries: scope => getEntries(storage, scope),
    clear: scope => clearEntries(storage, scope),
    resolveFor: engines => resolveEntriesForEngines(storage, GLOBAL_SCOPE, engines),
    version: () => getGlobalConfigVersion(storage),
  };
}

/**
 * Build a host: a fresh `HostContext` — its own session and launcher
 * registries, event emitter, resolved options and wire — plus the five
 * namespaces bound to it. Nothing global is touched, so several hosts can
 * coexist in one process as long as each has its own storage.
 *
 * Construction is pure: no timers run and no connection is opened until you
 * call `start()`, and `stop()` closes the storage you passed in.
 * @example
 * const host = createHost({
 *   storage: memoryStorage(),
 *   logger: noopLogger,
 *   agents: { registerToken: process.env.REGISTER_TOKEN ?? "" },
 * });
 * host.start();
 */
export function createHost(opts: CreateHostOptions): Host {
  const ctx = createHostContext({
    storage: opts.storage,
    logger: opts.logger,
    options: hostOptionsInput(opts),
    ...(opts.launchers ? { launchers: opts.launchers } : {}),
    ...(opts.metrics ? { metrics: opts.metrics } : {}),
    ...(opts.events ? { events: opts.events } : {}),
    ...(opts.ids ? { ids: opts.ids } : {}),
    ...(opts.clock ? { clock: opts.clock } : {}),
  });
  return {
    ...ctx,
    agents: agentsApi(ctx),
    tasks: tasksApi(ctx),
    crons: cronsApi(ctx),
    transcript: transcriptApi(ctx),
    config: configApi(ctx),
  };
}
