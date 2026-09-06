import type { ControlDeps, ControlResult, LaunchAgentResult, LaunchInput, RemoveResult } from "../agents/control";
import type { InspectState, Launcher } from "../agents/launchers";
import type { EnrollInput, EnrollResult } from "../agents/registry";
import type { EntryInput, IngestArtifactInput } from "../configstore";
import type { Logger } from "../logger";
import type { TranscriptReadOptions, TranscriptStats } from "../msgstore";
import type { ConfigEntry } from "../protocol/frames";
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

export interface CreateHostOptions {
  /** The `Storage` port (repositories + blob store); `host.stop()` closes it. */
  readonly storage: Storage;
  readonly logger: Logger;
  /** Launchers to register; defaults to manual only (apps add `createDockerLauncher({ spawn })`). */
  readonly launchers?: readonly Launcher[];
  /** Metrics sink; defaults to a no-op. */
  readonly metrics?: Metrics;
  /** Event emitter to share/inject; defaults to a fresh one. */
  readonly events?: HostEvents;
  readonly ids?: Partial<HostContext["ids"]>;
  readonly clock?: () => number;
  /** Protocol credentials + liveness cadence (the only secrets the SDK owns). */
  readonly agents?: Partial<Pick<HostOptions, "registerToken" | "registrationEnabled" | "heartbeatIntervalMs" | "offlineGraceMs">>;
  readonly attachments?: Partial<Pick<HostOptions, "maxUploadBytes">>;
  /** Root of the content-addressed blob store (config artifacts + attachments). */
  readonly storeDir?: string;
  readonly configGc?: HostOptionsInput["configGc"];
  readonly webhooks?: HostOptionsInput["webhooks"];
}

export interface AgentsApi {
  readonly enroll: (input: EnrollInput) => EnrollResult;
  readonly list: () => AgentRow[];
  readonly get: (agentId: string) => AgentRow | undefined;
  readonly remove: (agentId: string) => Promise<RemoveResult>;
  readonly stop: (agentId: string) => Promise<ControlResult>;
  readonly restart: (agentId: string) => Promise<ControlResult>;
  readonly inspect: (agentId: string) => Promise<InspectState>;
  readonly grant: (agentId: string, ownerId: string) => void;
  readonly revoke: (agentId: string, ownerId: string) => void;
  readonly setAccess: (agentId: string, access: "dedicated" | "public") => void;
  readonly launch: (input: LaunchInput) => Promise<LaunchAgentResult>;
}

export interface TasksApi {
  readonly create: (input: CreateTaskInput) => TaskRow;
  readonly get: (taskId: string) => TaskRow | undefined;
  readonly list: (filter?: ListTasksFilter) => TaskRow[];
  /** Manual placement: pin a specific permitted agent (set-once). */
  readonly assign: (taskId: string, agentId: string) => DispatchResult;
  /** Auto placement: owner → permitted agent (dedicated preferred, public fallback). */
  readonly place: (taskId: string) => DispatchResult;
  /** Deliver input (`task_upsert` on the first send, then `message`) via the outbox. */
  readonly send: (taskId: string, input: MessageInput) => SendMessageResult;
  /** Live-only cancel of the task's current turn (409 when the agent is offline). */
  readonly interrupt: (taskId: string) => OutboxRow;
  /** Mark the task `closed` (its config scope is dropped); no frame is emitted. */
  readonly close: (taskId: string) => TaskRow;
  readonly setEntries: (taskId: string, entries: EntryInput[]) => void;
  readonly transition: (taskId: string, status: string) => TaskRow;
}

export interface CronsApi {
  readonly upsert: (taskId: string, input: CronUpsertInput) => CronUpsertResult;
  readonly delete: (taskId: string, cronId: string) => OutboxRow;
  /** The read-only `cron_state` mirror for a task (OD14). */
  readonly list: (taskId: string) => CronRow[];
}

export interface TranscriptApi {
  readonly get: (threadId: string, opts?: TranscriptReadOptions) => MessageRow[];
  readonly stats: (threadId: string) => TranscriptStats;
  readonly threads: (taskId: string) => string[];
  readonly run: (threadId: string) => ThreadRunRow | undefined;
}

export interface ConfigApi {
  readonly putArtifact: (input: IngestArtifactInput) => ConfigArtifactRow;
  readonly setEntries: (scope: string, entries: ConfigEntry[]) => ConfigEntry[];
  readonly entries: (scope: string) => ConfigEntryRow[];
  readonly clear: (scope: string) => void;
  /** The GLOBAL desired set resolved for an agent's engines (what `register`/`config` deliver). */
  readonly resolveFor: (engines: string[]) => ConfigEntry[];
  readonly version: () => number;
}

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

/** Build a host: a fresh `HostContext` plus the namespaces bound to it. */
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
