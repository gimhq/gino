// M4 — TASK MANAGEMENT & DISPATCH ★ (task identity + placement; emits work as
// commands). A task ≈ a conversation/session (OD7+OD9).
//
//   store.ts         host-authoritative task identity + placement (set-once) + the conversation binding
//   dispatch.ts      placement: owner → permitted agent (dedicated preferred, public fallback; no migration)
//   orchestrator.ts  emit task_upsert/message/cron_upsert/cron_delete/interrupt/close/config_update via M2
//   lifecycle.ts     user/agent-marked status transitions (no auto-terminal, DP4; migration = a new task)
export const MODULE = "tasks" as const;

export {
  assignTaskToAgent,
  DispatchError,
  type DispatchResult,
  placeTask,
} from "./dispatch";

export {
  closeTask,
  TASK_STATUS,
  type TaskStatus,
  transitionTaskStatus,
} from "./lifecycle";

export {
  emitClose,
  emitConfigUpdate,
  emitCronDelete,
  emitCronUpsert,
  emitInterrupt,
  emitMessage,
  emitTaskUpsert,
  hasSeededTask,
  type MessageResult,
  type SendMessageResult,
  sendTaskMessage,
} from "./orchestrator";

export {
  createTask,
  type CreateTaskInput,
  getTask,
  listTaskCrons,
  listTasks,
  requireTask,
  setCurrentThread,
  setPlacement,
  setStatus,
  setTaskEntries,
  STATUS_PENDING_ASSIGNMENT,
} from "./store";
