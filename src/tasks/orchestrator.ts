// M4 — ORCHESTRATOR (the host→agent SOURCE for a task).
//
// Every unit of work the host hands an agent flows through the M2 outbox
// (`enqueue` — durable, at-least-once, acked); NOTHING bypasses it. The
// orchestrator builds the host→agent COMMANDS against the M2 frame catalog and
// enqueues them for the task's pinned agent:
//   • task_upsert   — seed/modify the task (engine/model/workdir/meta + M8 config entries)
//   • message       — deliver user/system input (carries `msgId`; acked by `msg_ack`)
//   • cron_upsert    /  cron_delete — scheduling COMMANDS (the agent owns + fires + persists
//                     the cron; the host schedules nothing, OD14 — it only mirrors `cron_state`)
//   • interrupt / close / config_update — control
//
// Threading (OD7+OD9): a task IS a conversation. The FIRST message opens the
// conversation's one ongoing thread (`tasks.current_thread_id`); continuing
// appends to it (the engine `--resume`s its session). A brand-new conversation
// is a brand-new task (not handled here). A new thread WITHIN a task is the
// rarer explicit case (`newThread`); cron isolated fires add their own threads
// agent-side (the host does not generate those ids).
import type { AppDatabase } from "@/db";
import type { Outbox, Task } from "@/db/schema";
import type { ConfigEntry, CronUpsertData } from "@/protocol/frames";
import type { FrameSender } from "@/protocol/outbox";
import { eq } from "drizzle-orm";
import { listAgents } from "@/agents/registry";
import { DEFAULT_ENGINE, getGlobalConfigVersion, GLOBAL_SCOPE, resolveEntriesForEngine } from "@/configstore";
import { tasks } from "@/db/schema";
import { writeCronPrompt } from "@/msgstore";
import { ulid } from "@/platform";
import { enqueue, enqueueLiveOnly } from "@/protocol/outbox";
import { AppError } from "@/shared/lib/errors";
import { markSeeded, setCurrentThread } from "./store";

/** A task with no `agent_id` has no SOURCE target — work cannot be emitted yet. */
function requirePlacement(task: Task): string {
  if (task.agentId == null) {
    throw new AppError(
      `task ${task.id} has no agent (pending-assignment) — assign it before sending work`,
      409,
      "TASK_NOT_PLACED",
    );
  }
  return task.agentId;
}

/**
 * True once a `task_upsert` has been emitted for this task (so we seed only
 * once). Reads the durable `tasks.seeded_at` flag — NOT outbox history, which
 * the retention GC deletes once acked.
 */
export function hasSeededTask(db: AppDatabase, taskId: string): boolean {
  const row = db
    .select({ seededAt: tasks.seededAt })
    .from(tasks)
    .where(eq(tasks.id, taskId))
    .get();
  return row?.seededAt != null;
}

/**
 * Seed/modify the task on its agent (`task_upsert`). Carries the OD15 payload
 * (engine/model/workdir/meta) from the host `tasks` row plus the task-scope M8
 * config `entries` — resolved from `config_entries WHERE scope = task.id` for
 * the task's engine (B6 fills B3's passthrough seam). An explicit `opts.entries`
 * overrides the resolution. Idempotent on the agent (it upserts by `taskId`);
 * re-emit on a definition or config change so a re-placed task re-materializes
 * its desired set (no migration). The agent reconciles; the host only declares.
 */
export function emitTaskUpsert(
  db: AppDatabase,
  sender: FrameSender,
  task: Task,
  opts: { entries?: ConfigEntry[] } = {},
): Outbox {
  const agentId = requirePlacement(task);
  const data: Record<string, unknown> = {
    status: task.status,
    engine: task.engine,
    model: task.model,
  };
  if (task.name != null) {
    data.name = task.name;
  }
  if (task.kind != null) {
    data.kind = task.kind;
  }
  if (task.workdir != null) {
    data.workdir = task.workdir;
  }
  if (task.meta != null) {
    data.meta = task.meta;
  }
  const entries = opts.entries ?? resolveEntriesForEngine(db, task.id, task.engine);
  if (entries.length > 0) {
    data.entries = entries;
  }
  const row = enqueue(db, sender, { agentId, type: "task_upsert", taskId: task.id, data });
  // Durable seed flag AFTER the frame is enqueued: a crash between the two
  // re-seeds on the next message (the agent upserts idempotently) — safe;
  // the reverse order could mark a task seeded whose frame never existed.
  markSeeded(db, task.id);
  return row;
}

export interface MessageInput {
  text: string;
  attachments?: unknown[];
  /** Open a NEW thread within the task instead of continuing the current one. */
  newThread?: boolean;
}

export interface MessageResult {
  /** The `message` outbox row (carries `msgId`, acked by `msg_ack`). */
  message: Outbox;
  /** The thread the message was delivered on (the task's ongoing thread). */
  threadId: string;
}

/**
 * Deliver input to a task (`message`). Resolves the target thread per OD7+OD9:
 * continue the task's `current_thread_id`, or open a fresh thread (first message
 * on the task, or an explicit `newThread`) and record it as current. The
 * `message` frame carries a `msgId` (set by the outbox) so the host advances its
 * delivery cursor only once the agent `msg_ack`s durable storage.
 */
export function emitMessage(db: AppDatabase, sender: FrameSender, task: Task, input: MessageInput): MessageResult {
  const agentId = requirePlacement(task);

  let threadId = task.currentThreadId ?? null;
  if (threadId == null || input.newThread) {
    threadId = ulid(); // host opens the conversation's thread (globally unique, §6)
    setCurrentThread(db, task.id, threadId);
  }

  const data: Record<string, unknown> = { text: input.text };
  if (input.attachments && input.attachments.length > 0) {
    data.attachments = input.attachments;
  }
  const message = enqueue(db, sender, { agentId, type: "message", taskId: task.id, threadId, data });
  return { message, threadId };
}

export interface SendMessageResult extends MessageResult {
  /** The `task_upsert` seeded on the first message, if any. */
  taskUpsert?: Outbox;
}

/**
 * High-level "do work" for the admin surface: on the FIRST message to a placed
 * task, seed it (`task_upsert`) and THEN deliver the `message`; on subsequent
 * messages, deliver only. This is the create→pin→SEND step of the end-to-end
 * flow (both frames ride the outbox to the connected agent).
 */
export function sendTaskMessage(
  db: AppDatabase,
  sender: FrameSender,
  task: Task,
  input: MessageInput,
  opts: { entries?: ConfigEntry[] } = {},
): SendMessageResult {
  requirePlacement(task);
  const taskUpsert = hasSeededTask(db, task.id) ? undefined : emitTaskUpsert(db, sender, task, opts);
  const sent = emitMessage(db, sender, task, input);
  return { ...sent, ...(taskUpsert ? { taskUpsert } : {}) };
}

export interface CronUpsertInput {
  /** Reuse an existing cron id (update) or omit to mint a new one (create). */
  cronId?: string;
  data: CronUpsertData;
}

export interface CronUpsertResult {
  cron: Outbox;
  cronId: string;
}

/**
 * Emit a `cron_upsert` COMMAND. The cron then LIVES ON THE AGENT (OD14): the
 * agent owns, fires, and persists it; the host schedules nothing and keeps only
 * the `cron_state` mirror. `data` carries the schedule + `continuity` (+ the
 * `prompt` work command — required, the agent rejects a promptless command).
 * The host mints the `cronId` on create so it can address later upsert/delete
 * commands. The `prompt` is persisted into the `crons` mirror here — the
 * `cron_state` frame never echoes it (HOST-M5M6-09). Like the first message,
 * the first cron SEEDS a never-seeded task (`task_upsert` before the command)
 * so the agent has the task row the cron FK-references (HOST-M4-02).
 */
export function emitCronUpsert(db: AppDatabase, sender: FrameSender, task: Task, input: CronUpsertInput): CronUpsertResult {
  const agentId = requirePlacement(task);
  if (!hasSeededTask(db, task.id)) {
    emitTaskUpsert(db, sender, task);
  }
  const cronId = input.cronId ?? ulid();
  const cron = enqueue(db, sender, {
    agentId,
    type: "cron_upsert",
    taskId: task.id,
    cronId,
    data: input.data,
  });
  writeCronPrompt(db, { taskId: task.id, cronId, prompt: input.data.prompt });
  return { cron, cronId };
}

/** Emit a `cron_delete` COMMAND — remove a trigger from the agent (OD14). */
export function emitCronDelete(db: AppDatabase, sender: FrameSender, task: Task, cronId: string): Outbox {
  const agentId = requirePlacement(task);
  return enqueue(db, sender, { agentId, type: "cron_delete", taskId: task.id, cronId });
}

/**
 * Emit an `interrupt` — graceful cancel of the task's current turn (the engine
 * stays alive). Carries `taskId`, which §5 makes NORMATIVE: the agent cancels
 * only if this task owns the in-flight turn, so another task's work is never
 * killed (HOST-M1M2-03). The signal is LIVE-ONLY: it requires the agent online
 * (409 otherwise) and is never replayed on reconnect — a stale cancel must not
 * fire against an arbitrary future turn (HOST-M4-03).
 */
export function emitInterrupt(db: AppDatabase, sender: FrameSender, task: Task): Outbox {
  const agentId = requirePlacement(task);
  if (!sender.isOnline(agentId)) {
    throw new AppError(
      `agent ${agentId} is offline — interrupt is a live-only signal and cannot be queued`,
      409,
      "AGENT_OFFLINE",
    );
  }
  return enqueueLiveOnly(db, sender, { agentId, type: "interrupt", taskId: task.id });
}

/**
 * Emit a `close` — RUNNER-LEVEL wind-down: the agent runner exits on it. NOT
 * used for per-task close (that is a task-scoped `interrupt` +
 * `task_upsert{status:"closed"}`); reserve this for genuine runner shutdown.
 */
export function emitClose(db: AppDatabase, sender: FrameSender, task: Task): Outbox {
  const agentId = requirePlacement(task);
  return enqueue(db, sender, { agentId, type: "close", taskId: task.id });
}

/** Emit a `config_update` — live config overlay for the task (opaque keys). */
export function emitConfigUpdate(db: AppDatabase, sender: FrameSender, task: Task, data: Record<string, unknown> = {}): Outbox {
  const agentId = requirePlacement(task);
  return enqueue(db, sender, { agentId, type: "config_update", taskId: task.id, data });
}

/**
 * Re-push the GLOBAL desired set live after a global config change (M8
 * delivery): a `config_update` carrying the resolved `entries` (per the agent's
 * primary engine) + the new monotonic `version` to every ONLINE agent — the
 * agent's §9b version gate makes the apply idempotent. Offline agents converge
 * on their next `GET /agent/config` pull (which carries the same version), so
 * no frame is queued for them. Returns the enqueued rows.
 */
export function broadcastGlobalConfig(db: AppDatabase, sender: FrameSender): Outbox[] {
  const version = getGlobalConfigVersion(db);
  const rows: Outbox[] = [];
  for (const agent of listAgents(db)) {
    if (agent.status === "removed" || !sender.isOnline(agent.id)) {
      continue;
    }
    const engine = agent.capabilities.engines[0] ?? DEFAULT_ENGINE;
    const entries = resolveEntriesForEngine(db, GLOBAL_SCOPE, engine);
    rows.push(enqueue(db, sender, { agentId: agent.id, type: "config_update", data: { entries, version } }));
  }
  return rows;
}
