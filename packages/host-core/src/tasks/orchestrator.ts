// M4 — ORCHESTRATOR (the host→agent SOURCE for a task).
//
// Every unit of work the host hands an agent flows through the M2 outbox
// (`enqueue` — durable, at-least-once, acked); NOTHING bypasses it. The
// orchestrator builds the host→agent COMMANDS against the M2 frame catalog and
// enqueues them for the task's pinned agent:
//   • task_upsert   — seed/modify the task (engine/model/workdir/meta + M8 config entries)
//   • message       — deliver user/system input (carries `msgId`; acked by `msg_ack`)
//   • cron_upsert    /  cron_delete — scheduling COMMANDS (the agent owns + fires + persists
//                     the cron; the host schedules nothing — it only mirrors `cron_state`)
//   • interrupt / close / config_update — control
//
// Threading: a task IS a conversation. The FIRST message opens the
// conversation's one ongoing thread (`tasks.current_thread_id`); continuing
// appends to it (the engine `--resume`s its session). A brand-new conversation
// is a brand-new task (not handled here). A new thread WITHIN a task is the
// rarer explicit case (`newThread`); cron isolated fires add their own threads
// agent-side (the host does not generate those ids).
import type { OutboxRow, Storage, TaskRow } from "../host/ports/storage";
import type { AgentTransport } from "../host/ports/transport";
import type { AttachmentDescriptor, ConfigEntry, CronUpsertData } from "../protocol/frames";
import type { EnqueuedOutbox } from "../protocol/outbox";
import { listAgents } from "../agents/registry";
import { getGlobalConfigVersion, GLOBAL_SCOPE, resolveEntriesForEngine, resolveEntriesForEngines } from "../configstore";
import { wireAttachmentRef, writeAttachmentDescriptors, writeCronPrompt } from "../msgstore";
import { ulid } from "../platform";
import { enqueue, enqueueLiveOnly } from "../protocol/outbox";
import { AppError } from "../shared/lib/errors";
import { markSeeded, setCurrentThread } from "./store";

/** A task with no `agent_id` has no SOURCE target — work cannot be emitted yet. */
function requirePlacement(task: TaskRow): string {
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
 * True once a `task_upsert` has been emitted for this task's CURRENT placement
 * (so we seed once per placement, not once per task): `setPlacement` clears
 * `seeded_at` when an orphaned task is re-placed, so the new agent gets a
 * fresh seed. Reads the durable `tasks.seeded_at` flag — NOT outbox history,
 * which the retention GC deletes once acked.
 */
export function hasSeededTask(storage: Storage, taskId: string): boolean {
  return storage.tasks.get(taskId)?.seededAt != null;
}

/**
 * Seed/modify the task on its agent (`task_upsert`). Carries the payload
 * (engine/model/workdir/meta) from the host `tasks` row plus the task-scope M8
 * config `entries` — resolved from `config_entries WHERE scope = task.id` for
 * the task's engine (B6 fills B3's passthrough seam). An explicit `opts.entries`
 * overrides the resolution. Idempotent on the agent (it upserts by `taskId`);
 * re-emit on a definition or config change so a re-placed task re-materializes
 * its desired set (no migration). The agent reconciles; the host only declares.
 */
export function emitTaskUpsert(
  storage: Storage,
  sender: AgentTransport,
  task: TaskRow,
  opts: { entries?: ConfigEntry[] } = {},
): OutboxRow {
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
  const entries = opts.entries ?? resolveEntriesForEngine(storage, task.id, task.engine);
  if (entries.length > 0) {
    data.entries = entries;
  }
  const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: task.id, data });
  // Durable seed flag AFTER the frame is enqueued: a crash between the two
  // re-seeds on the next message (the agent upserts idempotently) — safe;
  // the reverse order could mark a task seeded whose frame never existed.
  markSeeded(storage, task.id);
  return row;
}

/** One unit of input for a task. */
export interface MessageInput {
  text: string;
  attachments?: AttachmentDescriptor[];
  /** Open a NEW thread within the task instead of continuing the current one. */
  newThread?: boolean;
}

/** What a delivered `message` produced: the queued row and the thread it landed on. */
export interface MessageResult {
  /** The `message` outbox row (carries the derived `msgId`, acked by `msg_ack`). */
  message: EnqueuedOutbox;
  /** The thread the message was delivered on (the task's ongoing thread). */
  threadId: string;
}

/**
 * Deliver input to a task (`message`). Resolves the target thread:
 * continue the task's `current_thread_id`, or open a fresh thread (first message
 * on the task, or an explicit `newThread`) and record it as current. The
 * `message` frame carries a `msgId` (the outbox row id); the agent replies
 * `msg_ack{msgId}` once it has durably stored the input, which marks THAT row
 * delivered. Delivery is tracked PER ROW — there is no per-task delivery
 * cursor; unacked rows are simply replayed on reconnect (at-least-once, the
 * agent dedupes by `msgId`).
 */
export function emitMessage(storage: Storage, sender: AgentTransport, task: TaskRow, input: MessageInput): MessageResult {
  const agentId = requirePlacement(task);

  let threadId = task.currentThreadId ?? null;
  if (threadId == null || input.newThread) {
    // The HOST mints conversation thread ids (§5): the
    // agent creates-on-first-sight, so the binding is recorded durable BEFORE
    // the frame is emitted — a crash between the two re-sends under the same id.
    threadId = ulid();
    setCurrentThread(storage, task.id, threadId);
  }

  const data: Record<string, unknown> = { text: input.text };
  if (input.attachments && input.attachments.length > 0) {
    // Register the descriptors under this task/thread (idempotent on `id`),
    // then put the RESOLVABLE byte-channel ref on the wire: the agent joins
    // `ref` to its host base URL and fetches (agent-architecture §9), so the
    // store-internal ref must never leave the host.
    writeAttachmentDescriptors(storage, {
      taskId: task.id,
      threadId,
      descriptors: input.attachments.map(d => ({ ...d, direction: d.direction ?? "inbound" })),
    });
    data.attachments = input.attachments.map(d => ({ ...d, ref: wireAttachmentRef(d.id) }));
  }
  const message = enqueue(storage, sender, { agentId, type: "message", taskId: task.id, threadId, data });
  return { message, threadId };
}

/** A `host.tasks.send` outcome; `taskUpsert` is present only on the send that seeded the task. */
export interface SendMessageResult extends MessageResult {
  /** The `task_upsert` seeded on the first message, if any. */
  taskUpsert?: OutboxRow;
}

/**
 * High-level "do work" for the admin surface: on the FIRST message to a placed
 * task, seed it (`task_upsert`) and THEN deliver the `message`; on subsequent
 * messages, deliver only. This is the create→pin→SEND step of the end-to-end
 * flow (both frames ride the outbox to the connected agent).
 */
export function sendTaskMessage(
  storage: Storage,
  sender: AgentTransport,
  task: TaskRow,
  input: MessageInput,
  opts: { entries?: ConfigEntry[] } = {},
): SendMessageResult {
  requirePlacement(task);
  const taskUpsert = hasSeededTask(storage, task.id) ? undefined : emitTaskUpsert(storage, sender, task, opts);
  const sent = emitMessage(storage, sender, task, input);
  return { ...sent, ...(taskUpsert ? { taskUpsert } : {}) };
}

/** A cron command: omit `cronId` to create, give it to update. */
export interface CronUpsertInput {
  /** Reuse an existing cron id (update) or omit to mint a new one (create). */
  cronId?: string;
  data: CronUpsertData;
}

/** The queued `cron_upsert` row and the cron id to address it by later. */
export interface CronUpsertResult {
  cron: OutboxRow;
  cronId: string;
}

/**
 * Emit a `cron_upsert` COMMAND. The cron then LIVES ON THE AGENT: the
 * agent owns, fires, and persists it; the host schedules nothing and keeps only
 * the `cron_state` mirror. `data` carries the schedule + `continuity` (+ the
 * `prompt` work command — required, the agent rejects a promptless command).
 * The host mints the `cronId` on create so it can address later upsert/delete
 * commands. The `prompt` is persisted into the `crons` mirror here — the
 * `cron_state` frame never echoes it. Like the first message,
 * the first cron SEEDS a never-seeded task (`task_upsert` before the command)
 * so the agent has the task row the cron FK-references.
 */
export function emitCronUpsert(storage: Storage, sender: AgentTransport, task: TaskRow, input: CronUpsertInput): CronUpsertResult {
  const agentId = requirePlacement(task);
  if (!hasSeededTask(storage, task.id)) {
    emitTaskUpsert(storage, sender, task);
  }
  const cronId = input.cronId ?? ulid();
  const cron = enqueue(storage, sender, {
    agentId,
    type: "cron_upsert",
    taskId: task.id,
    cronId,
    data: input.data,
  });
  writeCronPrompt(storage, { taskId: task.id, cronId, prompt: input.data.prompt });
  return { cron, cronId };
}

/** Emit a `cron_delete` COMMAND — remove a trigger from the agent. */
export function emitCronDelete(storage: Storage, sender: AgentTransport, task: TaskRow, cronId: string): OutboxRow {
  const agentId = requirePlacement(task);
  return enqueue(storage, sender, { agentId, type: "cron_delete", taskId: task.id, cronId });
}

/**
 * Emit an `interrupt` — graceful cancel of the task's current turn (the engine
 * stays alive). Carries `taskId`, which §5 makes NORMATIVE: the agent cancels
 * only if this task owns the in-flight turn, so another task's work is never
 * killed. The signal is LIVE-ONLY: it requires the agent online
 * (409 otherwise) and is never replayed on reconnect — a stale cancel must not
 * fire against an arbitrary future turn.
 */
export function emitInterrupt(storage: Storage, sender: AgentTransport, task: TaskRow): OutboxRow {
  const agentId = requirePlacement(task);
  if (!sender.isOnline(agentId)) {
    throw new AppError(
      `agent ${agentId} is offline — interrupt is a live-only signal and cannot be queued`,
      409,
      "AGENT_OFFLINE",
    );
  }
  return enqueueLiveOnly(storage, sender, { agentId, type: "interrupt", taskId: task.id });
}

/**
 * Emit a `close` — RUNNER-LEVEL wind-down: the agent runner exits on it. NOT
 * used for per-task close (that is a task-scoped `interrupt` +
 * `task_upsert{status:"closed"}`); reserve this for genuine runner shutdown.
 */
export function emitClose(storage: Storage, sender: AgentTransport, task: TaskRow): OutboxRow {
  const agentId = requirePlacement(task);
  return enqueue(storage, sender, { agentId, type: "close", taskId: task.id });
}

/** Emit a `config_update` — live config overlay for the task (opaque keys). */
export function emitConfigUpdate(storage: Storage, sender: AgentTransport, task: TaskRow, data: Record<string, unknown> = {}): OutboxRow {
  const agentId = requirePlacement(task);
  return enqueue(storage, sender, { agentId, type: "config_update", taskId: task.id, data });
}

/**
 * Re-push the GLOBAL desired set live after a global config change (M8
 * delivery): a `config_update` carrying the resolved `entries` (covering ALL
 * the agent's declared engines) + the new monotonic `version` to
 * every ONLINE agent — the agent's §9b version gate makes the apply
 * idempotent. Offline agents converge on their next `GET /agent/config` pull
 * (which carries the same version), so no frame is queued for them. Returns
 * the enqueued rows.
 */
export function broadcastGlobalConfig(storage: Storage, sender: AgentTransport): OutboxRow[] {
  const version = getGlobalConfigVersion(storage);
  const rows: OutboxRow[] = [];
  for (const agent of listAgents(storage)) {
    if (agent.status === "removed" || !sender.isOnline(agent.id)) {
      continue;
    }
    const entries = resolveEntriesForEngines(storage, GLOBAL_SCOPE, agent.capabilities.engines);
    rows.push(enqueue(storage, sender, { agentId: agent.id, type: "config_update", data: { entries, version } }));
  }
  return rows;
}
