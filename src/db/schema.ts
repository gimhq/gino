// v2 host schema (docs/host-v2-design.md §6 — "Data model (clean v2)").
//
// This is the host's authoritative store. There are NO v1 legacy tables here
// (no scheduled_tasks / task_run_logs / sessions / registered_groups, and the
// `agents` table carries no nullable legacy container columns). Clean from the
// first migration.
//
// Conventions:
//   - snake_case column names; camelCase TS keys.
//   - Host-generated string PKs (`id`) are text ULIDs (see shared/lib/id.ts).
//     The exceptions are `outbox.id` and `messages.id` (integer autoincrement)
//     and `messages.seq` (per-thread integer serial, unique with `thread_id`).
//   - Timestamps are integer epoch-milliseconds. `created_at` / `updated_at`
//     default to `Date.now()` at insert time (host = sole writer); optional
//     timestamps are nullable integers.
//   - Structured columns are stored as TEXT with `{ mode: "json" }`.
import { sql } from "drizzle-orm";
import { index, integer, primaryKey, sqliteTable, text, unique } from "drizzle-orm/sqlite-core";

export const SCHEMA_VERSION = 2 as const;

// ---------------------------------------------------------------------------
// M3 — Agent Management (host-private)
// ---------------------------------------------------------------------------

export const agents = sqliteTable("agents", {
  id: text("id").primaryKey(), // == wire agentId (ULID)
  name: text("name"),
  source: text("source").notNull(), // manual | docker | vm | ...
  access: text("access").notNull(), // 'dedicated' (granted owners) | 'public' (any owner) — OD13
  connectTokenHash: text("connect_token_hash").notNull(), // per-agent connect token; revoked on remove (OD12)
  registerTokenRef: text("register_token_ref"), // which shared register token enrolled it
  capabilities: text("capabilities", { mode: "json" })
    .$type<{ engines: string[]; max_threads?: number }>()
    .notNull(), // { engines:[…], max_threads } (max_threads host-overridable, DP3)
  status: text("status").notNull(), // enrolled | online | offline | removed
  launcherKind: text("launcher_kind"), // OOB routing
  launchRef: text("launch_ref"),
  launchNonce: text("launch_nonce").unique(), // OD17 idempotency key: a register RETRY with the same nonce reuses this row (HOST-M3-06)
  activeThreads: integer("active_threads").default(0),
  lastSeen: integer("last_seen"),
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
});

// Launch↔register correlation (OD17), PERSISTED (HOST-M3-05): a pending launch
// must survive a host restart so a late registration still binds its
// `launcher_kind`/`launch_ref`, and so the TTL reaper can tear down a runtime
// whose agent never registered (the in-memory map lost both on restart).
export const pendingLaunches = sqliteTable("pending_launches", {
  nonce: text("nonce").primaryKey(), // the per-launch GINO_LAUNCH_NONCE
  launcherKind: text("launcher_kind").notNull(),
  launchRef: text("launch_ref"), // runtime handle (container id); null if the launcher started nothing
  access: text("access").notNull().default("dedicated"), // admin-allowlisted access applied at correlate (HOST-M3-09)
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
});

export const agentGrants = sqliteTable(
  "agent_grants",
  {
    agentId: text("agent_id").notNull().references(() => agents.id),
    ownerId: text("owner_id").notNull(),
    createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
  },
  t => [primaryKey({ columns: [t.agentId, t.ownerId] })],
);

// ---------------------------------------------------------------------------
// M4 — Task Management (host owns task identity + placement)
// ---------------------------------------------------------------------------

export const tasks = sqliteTable("tasks", {
  id: text("id").primaryKey(),
  ownerId: text("owner_id").notNull(), // owner (user identity) → permitted agent (OD13)
  engine: text("engine").notNull(), // OD15: task carries engine + model + workdir + meta
  model: text("model").notNull(),
  workdir: text("workdir"),
  meta: text("meta", { mode: "json" }).$type<Record<string, unknown>>(),
  name: text("name"),
  kind: text("kind"),
  status: text("status").notNull(), // user/agent-marked (no auto-terminal, DP4)
  agentId: text("agent_id").references(() => agents.id), // placement, SET ONCE; NULL = pending-assignment
  currentThreadId: text("current_thread_id"), // ongoing thread (resume target)
  assignedAt: integer("assigned_at"),
  seededAt: integer("seeded_at"), // first task_upsert seed enqueued (durable — survives outbox GC)
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
  updatedAt: integer("updated_at").notNull().$defaultFn(() => Date.now()),
});

// ---------------------------------------------------------------------------
// M5 — Read-only mirrors (sink; written from agent frames)
// ---------------------------------------------------------------------------

export const crons = sqliteTable("crons", {
  id: text("id").primaryKey(),
  taskId: text("task_id").notNull().references(() => tasks.id),
  kind: text("kind").notNull(),
  prompt: text("prompt").notNull(),
  scheduleType: text("schedule_type"),
  scheduleValue: text("schedule_value"),
  continuity: text("continuity").notNull(),
  nextRun: integer("next_run"),
  status: text("status"),
  lastStatus: text("last_status"),
  roundCount: integer("round_count").default(0),
  skippedCount: integer("skipped_count").default(0),
  updatedAt: integer("updated_at").notNull().$defaultFn(() => Date.now()),
});

export const threadRuns = sqliteTable("thread_runs", {
  threadId: text("thread_id").primaryKey(),
  taskId: text("task_id").notNull().references(() => tasks.id),
  cronId: text("cron_id").references(() => crons.id),
  source: text("source").notNull(),
  status: text("status").notNull(),
  failureReason: text("failure_reason"),
  engineSessionId: text("engine_session_id"),
  durationMs: integer("duration_ms"),
  finalSeq: integer("final_seq"),
  startedAt: integer("started_at"),
  finishedAt: integer("finished_at"),
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
});

export const messages = sqliteTable(
  "messages",
  {
    // Stable store position (the M6 egress cursor key, HOST-M5M6-03). An
    // explicit INTEGER PRIMARY KEY is a rowid alias, so VACUUM never renumbers
    // it; AUTOINCREMENT additionally forbids value reuse after deletes, so a
    // persisted cursor can never silently point at a different row.
    id: integer("id").primaryKey({ autoIncrement: true }),
    taskId: text("task_id").notNull(),
    threadId: text("thread_id").notNull(),
    seq: integer("seq").notNull(), // per-thread serial
    kind: text("kind").notNull(),
    role: text("role"),
    content: text("content"),
    tool: text("tool"),
    input: text("input", { mode: "json" }).$type<unknown>(),
    output: text("output", { mode: "json" }).$type<unknown>(),
    createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
  },
  t => [
    unique("messages_thread_seq_unq").on(t.threadId, t.seq), // idempotent-write key (protocol §6)
    index("messages_task_thread_seq_idx").on(t.taskId, t.threadId, t.seq),
  ],
);

export const attachments = sqliteTable("attachments", {
  id: text("id").primaryKey(),
  taskId: text("task_id").notNull(),
  threadId: text("thread_id").notNull(),
  name: text("name").notNull(),
  contentType: text("content_type").notNull(),
  size: integer("size").notNull(),
  sha256: text("sha256").notNull(),
  ref: text("ref").notNull(),
  direction: text("direction").notNull(),
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
});

// ---------------------------------------------------------------------------
// Plumbing (host-private)
// ---------------------------------------------------------------------------

export const outbox = sqliteTable("outbox", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  agentId: text("agent_id").notNull().references(() => agents.id),
  type: text("type").notNull(),
  taskId: text("task_id"),
  threadId: text("thread_id"),
  cronId: text("cron_id"),
  dataJson: text("data_json", { mode: "json" }).$type<Record<string, unknown>>().notNull(), // at-least-once host→agent
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
  ackedAt: integer("acked_at"),
}, t => [
  // Reconnect replay scan (`agent_id = ? AND acked_at IS NULL`) — partial, so
  // only in-flight rows are indexed and acked history costs nothing.
  // (`msg_ack{msgId}` keys on the integer PK directly — a message's delivery
  // id IS its row id — so no dedicated msg_ack index is needed.)
  index("outbox_agent_unacked_idx").on(t.agentId).where(sql`acked_at IS NULL`),
]);

export const channels = sqliteTable("channels", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  type: text("type").notNull(),
  ownerId: text("owner_id").notNull(), // OD16: registered with an owner (external system)
  config: text("config", { mode: "json" }).$type<Record<string, unknown>>(),
  msgFilter: text("msg_filter", { mode: "json" }).$type<Record<string, unknown>>(),
  createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
});

export const channelBindings = sqliteTable(
  "channel_bindings",
  {
    channelId: text("channel_id").notNull().references(() => channels.id),
    conversationRef: text("conversation_ref").notNull(),
    taskId: text("task_id").notNull().references(() => tasks.id), // OD9: conversation → task
    createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
  },
  t => [primaryKey({ columns: [t.channelId, t.conversationRef] })],
);

export const configArtifacts = sqliteTable(
  "config_artifacts",
  {
    name: text("name").notNull(),
    sha256: text("sha256").notNull(),
    kind: text("kind").notNull(),
    size: integer("size").notNull(),
    contentType: text("content_type"),
    storeRef: text("store_ref").notNull(),
    createdAt: integer("created_at").notNull().$defaultFn(() => Date.now()),
  },
  t => [primaryKey({ columns: [t.name, t.sha256] })], // M8 registry: bytes (host = sole source)
);

export const configEntries = sqliteTable(
  "config_entries",
  {
    scope: text("scope").notNull(), // 'global' | <task_id>
    name: text("name").notNull(),
    sha256: text("sha256").notNull(),
    target: text("target").notNull(),
    kind: text("kind").notNull(),
    updatedAt: integer("updated_at").notNull().$defaultFn(() => Date.now()),
  },
  t => [unique("config_entries_scope_target_unq").on(t.scope, t.target)], // M8 desired set
);

export const channelCursors = sqliteTable(
  "channel_cursors",
  {
    channelId: text("channel_id").notNull().references(() => channels.id),
    taskId: text("task_id").notNull(),
    lastSeq: integer("last_seq").notNull(),
    updatedAt: integer("updated_at").notNull().$defaultFn(() => Date.now()),
  },
  t => [primaryKey({ columns: [t.channelId, t.taskId] })], // M6 egress: tail M5 messages by cursor
);

export const routerState = sqliteTable("router_state", {
  key: text("key").primaryKey(),
  value: text("value").notNull(),
});

// ---------------------------------------------------------------------------
// Inferred row types
// ---------------------------------------------------------------------------

export type Agent = typeof agents.$inferSelect;
export type NewAgent = typeof agents.$inferInsert;
export type PendingLaunchRow = typeof pendingLaunches.$inferSelect;
export type NewPendingLaunchRow = typeof pendingLaunches.$inferInsert;
export type AgentGrant = typeof agentGrants.$inferSelect;
export type NewAgentGrant = typeof agentGrants.$inferInsert;
export type Task = typeof tasks.$inferSelect;
export type NewTask = typeof tasks.$inferInsert;
export type Cron = typeof crons.$inferSelect;
export type NewCron = typeof crons.$inferInsert;
export type ThreadRun = typeof threadRuns.$inferSelect;
export type NewThreadRun = typeof threadRuns.$inferInsert;
export type Message = typeof messages.$inferSelect;
export type NewMessage = typeof messages.$inferInsert;
export type Attachment = typeof attachments.$inferSelect;
export type NewAttachment = typeof attachments.$inferInsert;
export type Outbox = typeof outbox.$inferSelect;
export type NewOutbox = typeof outbox.$inferInsert;
export type Channel = typeof channels.$inferSelect;
export type NewChannel = typeof channels.$inferInsert;
export type ChannelBinding = typeof channelBindings.$inferSelect;
export type NewChannelBinding = typeof channelBindings.$inferInsert;
export type ConfigArtifact = typeof configArtifacts.$inferSelect;
export type NewConfigArtifact = typeof configArtifacts.$inferInsert;
export type ConfigEntry = typeof configEntries.$inferSelect;
export type NewConfigEntry = typeof configEntries.$inferInsert;
export type ChannelCursor = typeof channelCursors.$inferSelect;
export type NewChannelCursor = typeof channelCursors.$inferInsert;
export type RouterState = typeof routerState.$inferSelect;
export type NewRouterState = typeof routerState.$inferInsert;
