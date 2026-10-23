// HOST PORTS — plain row + insert types for the Storage port (host-sdk-design §8.3).
//
// Every `*Row` here mirrors the corresponding `$inferSelect` type in
// `src/db/schema.ts` KEY FOR KEY (same camelCase names, same nullability, same
// JSON column shapes); every `New*Row` mirrors `$inferInsert` (optional where
// the schema has a default or the column is nullable). They are written out by
// hand on purpose: the port must not depend on drizzle or the sqlite schema, so
// a non-SQL adapter can implement it without either. Keep the two in lockstep —
// a schema migration that adds/renames a column changes the row here too.
//
// Conventions (schema.ts): timestamps are epoch-milliseconds; `createdAt` /
// `updatedAt` default to the write time when an insert omits them.

// ─── M3 — agents / pending_launches / agent_grants ───

/** What an agent advertises at register: the engines it can run, and an optional thread cap. The engine list is open — a newer agent build may advertise names this host has never heard of. */
export interface AgentCapabilitiesJson {
  engines: string[];
  max_threads?: number;
  /** Host-set switches for engine-free operations; absent = all off. Never taken from the agent. */
  remote_ops?: { fs?: boolean; pty?: boolean };
}

/** A stored agent: identity, capabilities, access policy, liveness and its launcher binding. Only the connect token's HASH is ever persisted. */
export interface AgentRow {
  id: string;
  name: string | null;
  source: string;
  access: string;
  connectTokenHash: string;
  registerTokenRef: string | null;
  capabilities: AgentCapabilitiesJson;
  status: string;
  launcherKind: string | null;
  launchRef: string | null;
  launchNonce: string | null;
  activeThreads: number | null;
  lastSeen: number | null;
  createdAt: number;
}

/** Insert shape for `AgentRepo.insert`; `createdAt` and `activeThreads` default on write. */
export interface NewAgentRow {
  id: string;
  name?: string | null;
  source: string;
  access: string;
  connectTokenHash: string;
  registerTokenRef?: string | null;
  capabilities: AgentCapabilitiesJson;
  status: string;
  launcherKind?: string | null;
  launchRef?: string | null;
  launchNonce?: string | null;
  activeThreads?: number | null;
  lastSeen?: number | null;
  createdAt?: number;
}

/** A launch waiting for its agent to register, keyed by the single-use nonce. */
export interface PendingLaunchRow {
  nonce: string;
  launcherKind: string;
  launchRef: string | null;
  access: string;
  createdAt: number;
}

/** Insert shape for `AgentRepo.recordPending`; `access` defaults to `dedicated`. */
export interface NewPendingLaunchRow {
  nonce: string;
  launcherKind: string;
  launchRef?: string | null;
  access?: string;
  createdAt?: number;
}

/** One owner permitted on one `dedicated` agent. */
export interface AgentGrantRow {
  agentId: string;
  ownerId: string;
  createdAt: number;
}

// ─── M4 — tasks ───

/** A stored task: its definition, its set-once placement, and the thread a follow-up message continues. */
export interface TaskRow {
  id: string;
  ownerId: string;
  engine: string;
  model: string;
  workdir: string | null;
  meta: Record<string, unknown> | null;
  name: string | null;
  kind: string | null;
  status: string;
  agentId: string | null;
  currentThreadId: string | null;
  assignedAt: number | null;
  seededAt: number | null;
  createdAt: number;
  updatedAt: number;
}

/** Insert shape for `TaskRepo.insert`; both timestamps default on write. */
export interface NewTaskRow {
  id: string;
  ownerId: string;
  engine: string;
  model: string;
  workdir?: string | null;
  meta?: Record<string, unknown> | null;
  name?: string | null;
  kind?: string | null;
  status: string;
  agentId?: string | null;
  currentThreadId?: string | null;
  assignedAt?: number | null;
  seededAt?: number | null;
  createdAt?: number;
  updatedAt?: number;
}

// ─── M5 — read-only mirrors ───

/** The mirrored state of one agent-owned cron, plus the host-owned `prompt`. Read-only — the host schedules nothing. */
export interface CronRow {
  id: string;
  taskId: string;
  kind: string;
  prompt: string;
  scheduleType: string | null;
  scheduleValue: string | null;
  continuity: string;
  nextRun: number | null;
  status: string | null;
  lastStatus: string | null;
  roundCount: number | null;
  skippedCount: number | null;
  updatedAt: number;
}

/** The mirrored outcome of one thread: terminal status, engine session handle, duration and final `seq`. */
export interface ThreadRunRow {
  threadId: string;
  taskId: string;
  cronId: string | null;
  source: string;
  status: string;
  failureReason: string | null;
  engineSessionId: string | null;
  durationMs: number | null;
  finalSeq: number | null;
  startedAt: number | null;
  finishedAt: number | null;
  createdAt: number;
}

/** Insert/upsert shape for `ThreadRunRepo.upsert`; only the mutable subset is applied after the first write. */
export interface NewThreadRunRow {
  threadId: string;
  taskId: string;
  cronId?: string | null;
  source: string;
  status: string;
  failureReason?: string | null;
  engineSessionId?: string | null;
  durationMs?: number | null;
  finalSeq?: number | null;
  startedAt?: number | null;
  finishedAt?: number | null;
  createdAt?: number;
}

/** One transcript event. `id` is the monotonic egress cursor key; `seq` is the per-thread order the transcript is read by. */
export interface MessageRow {
  id: number;
  taskId: string;
  threadId: string;
  seq: number;
  kind: string;
  role: string | null;
  content: string | null;
  tool: string | null;
  input: unknown;
  output: unknown;
  /** Engine id of a `tool_use` block, echoed on the matching `tool_result`. */
  toolUseId: string | null;
  /** NULL = the main agent's own event; non-NULL = produced inside that sub-agent (`Task`) call. */
  parentToolUseId: string | null;
  createdAt: number;
}

/** Insert shape for `TranscriptRepo.insertIfAbsent`; `id` is storage-generated and `createdAt` defaults on write. */
export interface NewMessageRow {
  taskId: string;
  threadId: string;
  seq: number;
  kind: string;
  role?: string | null;
  content?: string | null;
  tool?: string | null;
  input?: unknown;
  output?: unknown;
  toolUseId?: string | null;
  parentToolUseId?: string | null;
  createdAt?: number;
}

/** A stored attachment descriptor: what the bytes are, which task/thread they belong to, and where they live in the blob store. */
export interface AttachmentRow {
  id: string;
  taskId: string;
  threadId: string;
  name: string;
  contentType: string;
  size: number;
  sha256: string;
  ref: string;
  direction: string;
  createdAt: number;
}

/** Insert shape for `AttachmentRepo.insertIfAbsent`; `createdAt` defaults on write. */
export interface NewAttachmentRow {
  id: string;
  taskId: string;
  threadId: string;
  name: string;
  contentType: string;
  size: number;
  sha256: string;
  ref: string;
  direction: string;
  createdAt?: number;
}

// ─── Plumbing — outbox ───

/** One queued host→agent frame. `id` is the wire `msgId`; `ackedAt` is null until the agent confirms it. */
export interface OutboxRow {
  id: number;
  agentId: string;
  type: string;
  taskId: string | null;
  threadId: string | null;
  cronId: string | null;
  dataJson: Record<string, unknown>;
  createdAt: number;
  ackedAt: number | null;
}

/** Insert shape for `OutboxRepo.append`; `id` is storage-generated and must come back on the returned row. */
export interface NewOutboxRow {
  agentId: string;
  type: string;
  taskId?: string | null;
  threadId?: string | null;
  cronId?: string | null;
  dataJson: Record<string, unknown>;
  createdAt?: number;
  ackedAt?: number | null;
}

// ─── M6 — channels / bindings / cursors ───

/** A distribution channel: its type, owner, opaque config and optional message filter. */
export interface ChannelRow {
  id: string;
  name: string;
  type: string;
  ownerId: string;
  config: Record<string, unknown> | null;
  msgFilter: Record<string, unknown> | null;
  createdAt: number;
}

/** Insert shape for `ChannelRepo.insert`; `createdAt` defaults on write. */
export interface NewChannelRow {
  id: string;
  name: string;
  type: string;
  ownerId: string;
  config?: Record<string, unknown> | null;
  msgFilter?: Record<string, unknown> | null;
  createdAt?: number;
}

/** The mapping from one external conversation to the task that serves it. */
export interface ChannelBindingRow {
  channelId: string;
  conversationRef: string;
  taskId: string;
  createdAt: number;
}

/** Insert shape for `ChannelRepo.bindIfAbsent`; `createdAt` defaults on write. */
export interface NewChannelBindingRow {
  channelId: string;
  conversationRef: string;
  taskId: string;
  createdAt?: number;
}

/** How far one channel has delivered one task's transcript, as a `messages.id` high-water mark. */
export interface ChannelCursorRow {
  channelId: string;
  taskId: string;
  lastSeq: number;
  updatedAt: number;
}

// ─── M8 — config_artifacts / config_entries ───

/** One immutable content-addressed artifact: a skill bundle (`dir`) or a single file. `unreferencedSince` is the GC aging mark. */
export interface ConfigArtifactRow {
  name: string;
  sha256: string;
  kind: string;
  size: number;
  contentType: string | null;
  storeRef: string;
  unreferencedSince: number | null;
  createdAt: number;
}

/** Insert shape for `ConfigRepo.upsertArtifact`; a repeat write updates only `kind` and `contentType`. */
export interface NewConfigArtifactRow {
  name: string;
  sha256: string;
  kind: string;
  size: number;
  contentType?: string | null;
  storeRef: string;
  unreferencedSince?: number | null;
  createdAt?: number;
}

/** One stored desired-set entry: which artifact a scope wants, at which engine-resolved `target`. */
export interface ConfigEntryRow {
  scope: string;
  name: string;
  sha256: string;
  target: string;
  kind: string;
  updatedAt: number;
}

/** One desired-set entry as handed to `ConfigRepo.replaceScope` (the scope is the call's argument). */
export interface ConfigEntryInput {
  name: string;
  sha256: string;
  target: string;
  kind: string;
}
