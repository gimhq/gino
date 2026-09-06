import type { NewThreadRunRow, ThreadRunRepo, ThreadRunRow } from "../../host/ports/storage";

export function memoryThreadRunRepo(): ThreadRunRepo {
  const runs = new Map<string, ThreadRunRow>();

  return {
    upsert(row: NewThreadRunRow) {
      const existing = runs.get(row.threadId);
      if (existing) {
        // Terminal mirror fields ONLY — context + timestamps are set once.
        existing.status = row.status;
        existing.failureReason = row.failureReason ?? null;
        existing.engineSessionId = row.engineSessionId ?? null;
        existing.durationMs = row.durationMs ?? null;
        existing.finalSeq = row.finalSeq ?? null;
        return;
      }
      runs.set(row.threadId, {
        threadId: row.threadId,
        taskId: row.taskId,
        cronId: row.cronId ?? null,
        source: row.source,
        status: row.status,
        failureReason: row.failureReason ?? null,
        engineSessionId: row.engineSessionId ?? null,
        durationMs: row.durationMs ?? null,
        finalSeq: row.finalSeq ?? null,
        startedAt: row.startedAt ?? null,
        finishedAt: row.finishedAt ?? null,
        createdAt: row.createdAt ?? Date.now(),
      });
    },
    get(threadId) {
      const row = runs.get(threadId);
      return row ? { ...row } : undefined;
    },
    listByTask(taskId) {
      return [...runs.values()].filter(r => r.taskId === taskId).map(r => ({ ...r }));
    },
  };
}
