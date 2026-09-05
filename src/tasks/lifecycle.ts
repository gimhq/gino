// M4 — LIFECYCLE (task status transitions; the no-migration / no-auto-terminal
// rules made explicit).
//
// Task status is USER/AGENT-MARKED. The host NEVER auto-terminates a task (DP4):
// an agent going `offline` is an AGENT state — its tasks/crons survive on the
// agent and resume on reconnect. There is therefore no host-driven terminal
// transition; `transitionTaskStatus` simply records the externally-decided
// status. The only host-side terminal-ish bookkeeping is agent REMOVAL, which
// ORPHANS the agent's tasks (placement cleared, mirror kept as history) — and
// that already lives in `registry.removeAgent` (B2a), not here.
//
// MIGRATION = A NEW TASK. A task lives on exactly one agent for life
// (`tasks.agent_id` set-once). "Moving" work to another agent is NOT a
// reassignment; it is a brand-new, unrelated task created on the new agent with
// no continuity carried — the old task stays as history. So there is no
// `migrate(task)` here by design: callers just `createTask` + `placeTask` anew.
import type { Storage, TaskRow } from "@/host/ports/storage";
import { clearEntries } from "@/configstore";
import { setStatus } from "./store";

/**
 * Well-known task statuses. Status is free-form (user/agent-marked) — these are
 * the host-recognised values used by dispatch + the admin surface, not a closed
 * enum the agent must honour.
 */
export const TASK_STATUS = {
  /** Created, not yet placed on an agent (dispatch found none, or pre-dispatch). */
  pendingAssignment: "pending-assignment",
  /** Placed on an agent and live. */
  active: "active",
  /** Wound down by the user/admin (`/close`). NOT host-auto — DP4. */
  closed: "closed",
} as const;

export type TaskStatus = (typeof TASK_STATUS)[keyof typeof TASK_STATUS];

/**
 * Record a user/agent-decided status transition. This is the ONLY status writer
 * the admin surface uses; the host never forces a terminal state on its own
 * (DP4). Returns the updated task (404 if unknown).
 *
 * A transition to `closed` also drops the task's scope from the M8 desired set
 * (HOST-M8-10): its `config_entries` rows stop pinning artifacts in the GC
 * root set. Re-opening a task does not resurrect them — re-declare if needed.
 */
export function transitionTaskStatus(storage: Storage, taskId: string, status: string): TaskRow {
  const task = setStatus(storage, taskId, status);
  if (status === TASK_STATUS.closed) {
    clearEntries(storage, taskId);
  }
  return task;
}

/** Mark a task `closed` (user/admin `/close`); pairs with a task-scoped `interrupt` + `task_upsert{status:"closed"}`. */
export function closeTask(storage: Storage, taskId: string): TaskRow {
  return transitionTaskStatus(storage, taskId, TASK_STATUS.closed);
}
