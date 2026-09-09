import type { EntryInput } from "../configstore";
// M4 — TASK STORE (host-authoritative task identity + placement).
//
// The host OWNS task identity: it generates the `task_id` (D5, a ULID) and
// holds the task's definition — `owner_id`, `engine`, `model`, `workdir?`,
// `meta?`, `name?`, `kind?`, `status` — plus its placement (`agent_id`, SET
// ONCE) and the conversation binding (`current_thread_id`). This is the *only*
// module that writes the `tasks` table.
//
// `crons` / `thread_runs` / `messages` are READ-ONLY MIRRORS written by M5 from
// inbound agent frames — never authored here. This module may *read* the mirror
// (`listTaskCrons`) for the admin surface, but it never writes it.
//
// task-scope config/skill entries (`config_entries` WHERE scope = <task_id>) are
// M8/B6 — `meta` carries an optional passthrough seam (no config storage here).
import type { CronRow, NewTaskRow, Storage, TaskRow } from "../host/ports/storage";
import { resolveEntries, setEntries } from "../configstore";
import { ulid } from "../platform";
import { AppError, NotFoundError } from "../shared/lib/errors";

/** A freshly-created, unplaced task is `pending-assignment` until dispatch pins it (§3). */
export const STATUS_PENDING_ASSIGNMENT = "pending-assignment" as const;

/** The task definition an operator supplies; the host mints the id. */
export interface CreateTaskInput {
  /** Owner (user identity) → resolved to a permitted agent by dispatch (OD13). */
  ownerId: string;
  /** OD15 — a task carries `engine` + `model` + `workdir?` + `meta?`. */
  engine: string;
  model: string;
  workdir?: string;
  meta?: Record<string, unknown>;
  name?: string;
  kind?: string;
  /** Initial status (user/agent-marked); defaults to `pending-assignment`. */
  status?: string;
  /** M8 — task-scope config/skill desired set, persisted to `config_entries WHERE scope = task.id`. */
  entries?: EntryInput[];
}

/**
 * Create a task: the host mints the ULID `task_id` and persists the definition
 * at `pending-assignment` (no placement yet). Dispatch (`dispatch.ts`) then
 * resolves the owner to a permitted agent and pins `agent_id` once. Any M8
 * task-scope `entries` are persisted to `config_entries` (scope = the new
 * `task.id`) with the host resolving the per-engine `target`.
 */
export function createTask(storage: Storage, input: CreateTaskInput): TaskRow {
  const values: NewTaskRow = {
    id: ulid(),
    ownerId: input.ownerId,
    engine: input.engine,
    model: input.model,
    workdir: input.workdir ?? null,
    meta: input.meta ?? null,
    name: input.name ?? null,
    kind: input.kind ?? null,
    status: input.status ?? STATUS_PENDING_ASSIGNMENT,
  };
  const task = storage.tasks.insert(values);
  if (input.entries && input.entries.length > 0) {
    setTaskEntries(storage, task.id, input.entries);
  }
  return task;
}

/**
 * Persist (REPLACE) a task's M8 desired set — `config_entries WHERE scope =
 * taskId` — resolving the per-engine `target` from the task's engine. The
 * orchestrator re-materializes this set into `task_upsert.data.entries` on
 * (re-)dispatch; the agent reconciles it. An empty list clears the task's set.
 */
export function setTaskEntries(storage: Storage, taskId: string, entries: EntryInput[]): void {
  const task = requireTask(storage, taskId);
  setEntries(storage, taskId, resolveEntries(task.engine, entries));
}

export function getTask(storage: Storage, id: string): TaskRow | undefined {
  return storage.tasks.get(id);
}

/** Like `getTask` but throws the standard 404 for the admin surface. */
export function requireTask(storage: Storage, id: string): TaskRow {
  const task = getTask(storage, id);
  if (!task) {
    throw new NotFoundError("task", id);
  }
  return task;
}

/** Equality filter for `host.tasks.list`; omitted keys are unconstrained. */
export interface ListTasksFilter {
  ownerId?: string;
  agentId?: string;
  status?: string;
}

/** List tasks (admin observe), newest first, optionally filtered. */
export function listTasks(storage: Storage, filter: ListTasksFilter = {}): TaskRow[] {
  return storage.tasks.list({
    ...(filter.ownerId != null ? { ownerId: filter.ownerId } : {}),
    ...(filter.agentId != null ? { agentId: filter.agentId } : {}),
    ...(filter.status != null ? { status: filter.status } : {}),
  });
}

/**
 * Pin a task to an agent — SET ONCE (no migration, §3 hard rule). The first
 * placement records `agent_id` + `assigned_at`; re-pinning the SAME agent is an
 * idempotent no-op; re-pinning a DIFFERENT agent is rejected (409) because a
 * task lives on exactly one agent for life. "Moving agents" is a brand-new task.
 *
 * The pin also RESETS the placement-scoped state — `seeded_at` and
 * `current_thread_id` — so an orphaned task (agent removed → `agent_id` nulled)
 * re-placed on a NEW agent is re-seeded (`task_upsert`) and opens a fresh
 * thread instead of dragging the removed agent's dead thread along. Both
 * columns are always NULL on a first placement, so the reset only bites on
 * re-placement.
 */
export function setPlacement(storage: Storage, taskId: string, agentId: string): TaskRow {
  const task = requireTask(storage, taskId);
  if (task.agentId != null) {
    if (task.agentId === agentId) {
      return task; // idempotent: already pinned to this agent
    }
    throw new AppError(
      `task ${taskId} is already placed on agent ${task.agentId} — placement is set-once (no migration)`,
      409,
      "TASK_ALREADY_PLACED",
    );
  }
  storage.tasks.setPlacementOnce(taskId, agentId, Date.now());
  return requireTask(storage, taskId);
}

/** Set the task's status (user/agent-marked; the host never auto-terminals, DP4). */
export function setStatus(storage: Storage, taskId: string, status: string): TaskRow {
  const task = storage.tasks.setStatus(taskId, status, Date.now());
  if (!task) {
    throw new NotFoundError("task", taskId);
  }
  return task;
}

/**
 * Record that the task's `task_upsert` seed frame has been enqueued
 * (`seeded_at`). Durable on the task row — outbox retention GC cannot un-seed
 * a task by deleting its acked history. Set once (an already-set timestamp is
 * kept); re-placement work clears it elsewhere, never here.
 */
export function markSeeded(storage: Storage, taskId: string): void {
  storage.tasks.markSeeded(taskId, Date.now());
}

/**
 * Track the conversation's ongoing thread (`current_thread_id`, the resume
 * target). The orchestrator advances this when it opens a thread for a message
 * (a continue reuses it; a new conversation is a new task, OD7+OD9).
 */
export function setCurrentThread(storage: Storage, taskId: string, threadId: string): TaskRow {
  const task = storage.tasks.setCurrentThread(taskId, threadId, Date.now());
  if (!task) {
    throw new NotFoundError("task", taskId);
  }
  return task;
}

/**
 * Read the cron mirror for a task (READ-ONLY — crons live on the agent, OD14;
 * M5 writes this from `cron_state`). Used by the `/admin/tasks/:id/crons` list;
 * this module never writes the `crons` table.
 */
export function listTaskCrons(storage: Storage, taskId: string): CronRow[] {
  return storage.crons.listByTask(taskId);
}
