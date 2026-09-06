// M4 — DISPATCH (placement: task → agent, owner-pinned). "分任务到 agent".
//
// Placement is rule-based and owner-pinned (§3). An agent is a RESOURCE with an
// access policy; an owner (user) is placed on an agent it is PERMITTED to use —
// a `dedicated` agent granted to that owner, or a `public` (shared) agent
// (OD13). The chosen agent must also hold the task's `engine` capability.
//
// Hard rules (§3):
//   • One task : one agent, for life — `tasks.agent_id` is SET ONCE, never
//     reassigned (no migration). `setPlacement` enforces this.
//   • Capacity (DP3) is a PREFERENCE among candidates and back-pressure WITHIN
//     the chosen agent — it NEVER spills a task to a different agent. So a busy
//     permitted agent is still pinned (the agent queues the thread); capacity
//     only breaks ties / orders candidates.
//   • None permitted → `pending-assignment` (surfaced in /admin/tasks); a
//     launcher/admin grants or starts one later (deferred phase).
import type { HostEvents } from "../host/events";
import type { AgentRow, Storage, TaskRow } from "../host/ports/storage";
import { getCapacity, isOwnerPermitted, listAgents } from "../agents/registry";
import { AppError } from "../shared/lib/errors";
import { requireTask, setPlacement, setStatus } from "./store";

/** A task placed by dispatch becomes `active`; pin + status flip in one step. */
export const STATUS_ACTIVE = "active" as const;
export const STATUS_PENDING_ASSIGNMENT = "pending-assignment" as const;

/**
 * Statuses from which placement may flip a task `active`. Anything else
 * (`closed`, `failed`, …) is user/agent-marked lifecycle state that a
 * placement or a retried assign must never resurrect (the host never
 * auto-terminals and never auto-revives, DP4).
 */
const ASSIGNABLE_STATUSES: readonly string[] = [STATUS_PENDING_ASSIGNMENT, STATUS_ACTIVE];

/**
 * Status half of a placement: flip to `active` ONLY from a status where
 * assignment is legal; a closed/terminal task keeps its status even when the
 * pin itself is (re-)recorded. Returns the resulting status.
 */
function activateForPlacement(storage: Storage, task: TaskRow): string {
  if (!ASSIGNABLE_STATUSES.includes(task.status)) {
    return task.status;
  }
  return setStatus(storage, task.id, STATUS_ACTIVE).status;
}

/** A placement precondition failure (admin `/assign`) → HTTP 422. */
export class DispatchError extends AppError {
  constructor(message: string, code: string) {
    super(message, 422, code);
  }
}

export interface DispatchResult {
  taskId: string;
  placed: boolean;
  agentId: string | null;
  status: string;
  /** Why dispatch resolved the way it did (observability). */
  reason: "placed" | "already-placed" | "no-permitted-agent";
}

/** A scored placement candidate (lower sort key = preferred). */
interface Candidate {
  agent: AgentRow;
  /** 0 = dedicated (the owner's own agent), 1 = public (shared). */
  tier: number;
  /** 0 = has spare capacity now, 1 = at/over capacity (back-pressure within). */
  roomRank: number;
  /** Agent-reported in-flight threads — least-loaded preferred. */
  load: number;
  /** 0 = online, 1 = offline/enrolled — online preferred (immediate delivery). */
  onlineRank: number;
}

/**
 * Resolve `task.owner_id` → the candidate agents it may use, scored for
 * preference. A candidate is: not `removed`, holds the task's `engine`, and the
 * owner is permitted on it (OD13). Sorted dedicated-before-public, then
 * spare-capacity, then least-loaded, then online, then stable by `created_at`.
 */
function rankCandidates(storage: Storage, task: TaskRow): Candidate[] {
  const candidates: Candidate[] = [];
  for (const agent of listAgents(storage)) {
    if (agent.status === "removed") {
      continue;
    }
    if (!agent.capabilities.engines.includes(task.engine)) {
      continue; // an agent must hold the task's engine capability (§3)
    }
    if (!isOwnerPermitted(storage, agent.id, task.ownerId)) {
      continue;
    }
    const capacity = getCapacity(storage, agent.id);
    candidates.push({
      agent,
      tier: agent.access === "dedicated" ? 0 : 1,
      roomRank: capacity?.hasRoom === false ? 1 : 0,
      load: agent.activeThreads ?? 0,
      onlineRank: agent.status === "online" ? 0 : 1,
    });
  }

  return candidates.sort((a, b) =>
    a.tier - b.tier
    || a.roomRank - b.roomRank
    || a.load - b.load
    || a.onlineRank - b.onlineRank
    || a.agent.createdAt - b.agent.createdAt,
  );
}

/**
 * Place a task on a permitted agent (owner-pinned, §3). Idempotent + set-once:
 *   • already placed   → returns the existing pin unchanged (no migration).
 *   • a permitted agent → pins it ONCE (`setPlacement`) + marks the task
 *     `active`. Dedicated is preferred over public; capacity only orders the
 *     candidates (never spills — a busy permitted agent is still chosen).
 *   • none permitted   → leaves the task `pending-assignment`.
 * A fresh pin is announced as `task.placed` on `events` (after the write).
 */
export function placeTask(storage: Storage, taskId: string, events?: HostEvents): DispatchResult {
  const task = requireTask(storage, taskId);
  if (task.agentId != null) {
    return { taskId, placed: true, agentId: task.agentId, status: task.status, reason: "already-placed" };
  }

  const chosen = rankCandidates(storage, task)[0];
  if (!chosen) {
    // No permitted agent — queue for assignment (a launcher/admin acts later).
    if (task.status !== STATUS_PENDING_ASSIGNMENT) {
      setStatus(storage, taskId, STATUS_PENDING_ASSIGNMENT);
    }
    return { taskId, placed: false, agentId: null, status: STATUS_PENDING_ASSIGNMENT, reason: "no-permitted-agent" };
  }

  setPlacement(storage, taskId, chosen.agent.id); // SET ONCE
  const status = activateForPlacement(storage, task);
  events?.emit("task.placed", { taskId, agentId: chosen.agent.id });
  return { taskId, placed: true, agentId: chosen.agent.id, status, reason: "placed" };
}

/**
 * Manual placement (admin `/assign {agent_id}`): pin a SPECIFIC permitted agent.
 * Validates the agent exists, is not `removed`, holds the task's `engine`, and
 * the owner is permitted on it — then pins it once + marks `active` (only from
 * an assignable status — a closed task is never resurrected). Re-assigning the
 * SAME agent is a full no-op (`already-placed`); set-once is enforced by
 * `setPlacement` (a second, different agent → 409).
 */
export function assignTaskToAgent(storage: Storage, taskId: string, agentId: string, events?: HostEvents): DispatchResult {
  const task = requireTask(storage, taskId);
  if (task.agentId === agentId) {
    // Idempotent replay of the same assign: report the existing pin and touch
    // NOTHING — a retried assign must not flip a closed task back to `active`.
    return { taskId, placed: true, agentId, status: task.status, reason: "already-placed" };
  }
  const agent = listAgents(storage).find(a => a.id === agentId);
  if (!agent || agent.status === "removed") {
    throw new DispatchError(`agent ${agentId} is not available for placement`, "AGENT_UNAVAILABLE");
  }
  if (!agent.capabilities.engines.includes(task.engine)) {
    throw new DispatchError(`agent ${agentId} does not support engine '${task.engine}'`, "ENGINE_MISMATCH");
  }
  if (!isOwnerPermitted(storage, agentId, task.ownerId)) {
    throw new DispatchError(`owner ${task.ownerId} is not permitted on agent ${agentId}`, "NOT_PERMITTED");
  }

  setPlacement(storage, taskId, agentId); // SET ONCE (409 on a different agent while placed)
  const status = activateForPlacement(storage, task);
  events?.emit("task.placed", { taskId, agentId });
  return { taskId, placed: true, agentId, status, reason: "placed" };
}
