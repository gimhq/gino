// M5 — cross-table read models (for M6 egress / admin, later stages).
//
// Storage never calls delivery; these are plain reads M6/admin pull from. The
// per-table reads live with their writers (`getTranscript`, `getThreadRun`,
// `getCron`); this module holds the read that spans tables.
import type { Storage } from "@/host/ports/storage";

/**
 * The thread ids belonging to a task, ordered by first appearance (protocol §6:
 * "threads within a task are ordered by thread.created_at"). A thread surfaces
 * as soon as it has either a transcript event (`messages`) or a run record
 * (`thread_runs`), so both sources are unioned and de-duped — robust to any
 * frame arrival order.
 */
export function getTaskThreads(storage: Storage, taskId: string): string[] {
  const fromMessages = storage.transcript.threads(taskId);
  const fromRuns = storage.threadRuns.listByTask(taskId).map(r => ({ threadId: r.threadId, firstAt: r.createdAt }));

  const earliest = new Map<string, number>();
  for (const row of [...fromMessages, ...fromRuns]) {
    const at = row.firstAt ?? 0;
    const seen = earliest.get(row.threadId);
    if (seen === undefined || at < seen) {
      earliest.set(row.threadId, at);
    }
  }

  return [...earliest.entries()]
    .sort((a, b) => a[1] - b[1])
    .map(([threadId]) => threadId);
}

/**
 * The task a thread is currently recorded under — its `thread_runs.task_id` if a
 * run exists, else the `task_id` of any stored transcript row, else `undefined`
 * (the thread is brand new / unseen). A thread belongs to exactly one task, so M2
 * inbound uses this to BIND a caller-supplied `thread_id` to its task: a write
 * frame may not target a thread already owned by a DIFFERENT task (cross-thread
 * transcript poisoning via a forged `(thread_id, seq)`).
 */
export function getThreadTaskId(storage: Storage, threadId: string): string | undefined {
  const run = storage.threadRuns.get(threadId);
  if (run) {
    return run.taskId;
  }
  return storage.transcript.taskOf(threadId);
}
