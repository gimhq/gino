import type { NewTaskRow, TaskRepo, TaskRow } from "../../host/ports/storage";
import { newestFirst, uniqueViolation } from "./shared";

export function memoryTaskRepo(): TaskRepo {
  const tasks = new Map<string, TaskRow>();

  return {
    insert(row: NewTaskRow): TaskRow {
      if (tasks.has(row.id)) {
        throw uniqueViolation("tasks.id");
      }
      const now = Date.now();
      const stored: TaskRow = {
        id: row.id,
        ownerId: row.ownerId,
        engine: row.engine,
        model: row.model,
        workdir: row.workdir ?? null,
        meta: row.meta ?? null,
        name: row.name ?? null,
        kind: row.kind ?? null,
        status: row.status,
        agentId: row.agentId ?? null,
        currentThreadId: row.currentThreadId ?? null,
        assignedAt: row.assignedAt ?? null,
        seededAt: row.seededAt ?? null,
        createdAt: row.createdAt ?? now,
        updatedAt: row.updatedAt ?? now,
      };
      tasks.set(stored.id, stored);
      return { ...stored };
    },
    get(taskId) {
      const row = tasks.get(taskId);
      return row ? { ...row } : undefined;
    },
    list(filter = {}) {
      const matching = [...tasks.values()].filter(t =>
        (filter.ownerId == null || t.ownerId === filter.ownerId)
        && (filter.agentId == null || t.agentId === filter.agentId)
        && (filter.status == null || t.status === filter.status),
      );
      return newestFirst(matching, t => t.createdAt).map(t => ({ ...t }));
    },
    setPlacementOnce(taskId, agentId, now) {
      const row = tasks.get(taskId);
      if (!row || row.agentId != null) {
        return false;
      }
      row.agentId = agentId;
      row.assignedAt = now;
      row.seededAt = null;
      row.currentThreadId = null;
      row.updatedAt = now;
      return true;
    },
    setStatus(taskId, status, now) {
      const row = tasks.get(taskId);
      if (!row) {
        return undefined;
      }
      row.status = status;
      row.updatedAt = now;
      return { ...row };
    },
    setCurrentThread(taskId, threadId, now) {
      const row = tasks.get(taskId);
      if (!row) {
        return undefined;
      }
      row.currentThreadId = threadId;
      row.updatedAt = now;
      return { ...row };
    },
    markSeeded(taskId, now) {
      const row = tasks.get(taskId);
      if (row && row.seededAt == null) {
        row.seededAt = now;
      }
    },
    orphanByAgent(agentId, now) {
      const ids: string[] = [];
      for (const row of tasks.values()) {
        if (row.agentId === agentId) {
          row.agentId = null;
          row.updatedAt = now;
          ids.push(row.id);
        }
      }
      return ids;
    },
  };
}
