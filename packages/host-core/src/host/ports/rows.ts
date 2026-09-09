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

export interface AgentCapabilitiesJson {
  engines: string[];
  max_threads?: number;
}

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

export interface PendingLaunchRow {
  nonce: string;
  launcherKind: string;
  launchRef: string | null;
  access: string;
  createdAt: number;
}

export interface NewPendingLaunchRow {
  nonce: string;
  launcherKind: string;
  launchRef?: string | null;
  access?: string;
  createdAt?: number;
}

export interface AgentGrantRow {
  agentId: string;
  ownerId: string;
  createdAt: number;
}

// ─── M4 — tasks ───

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

export interface ChannelRow {
  id: string;
  name: string;
  type: string;
  ownerId: string;
  config: Record<string, unknown> | null;
  msgFilter: Record<string, unknown> | null;
  createdAt: number;
}

export interface NewChannelRow {
  id: string;
  name: string;
  type: string;
  ownerId: string;
  config?: Record<string, unknown> | null;
  msgFilter?: Record<string, unknown> | null;
  createdAt?: number;
}

export interface ChannelBindingRow {
  channelId: string;
  conversationRef: string;
  taskId: string;
  createdAt: number;
}

export interface NewChannelBindingRow {
  channelId: string;
  conversationRef: string;
  taskId: string;
  createdAt?: number;
}

export interface ChannelCursorRow {
  channelId: string;
  taskId: string;
  lastSeq: number;
  updatedAt: number;
}

// ─── M8 — config_artifacts / config_entries ───

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
