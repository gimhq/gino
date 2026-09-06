import type { CronMirrorRepo, CronRow } from "../../host/ports/storage";
import { newestFirst } from "./shared";

export function memoryCronMirrorRepo(): CronMirrorRepo {
  const crons = new Map<string, CronRow>();

  return {
    upsertState(input) {
      const existing = crons.get(input.id);
      if (existing) {
        // Mirror fields only — `prompt` / `taskId` are preserved; `continuity`
        // only when the frame carries it.
        existing.kind = input.kind;
        existing.scheduleType = input.scheduleType;
        existing.scheduleValue = input.scheduleValue;
        existing.nextRun = input.nextRun;
        existing.status = input.status;
        existing.lastStatus = input.lastStatus;
        existing.roundCount = input.roundCount;
        existing.skippedCount = input.skippedCount;
        existing.updatedAt = input.updatedAt;
        if (input.continuity !== undefined) {
          existing.continuity = input.continuity;
        }
        return;
      }
      crons.set(input.id, {
        id: input.id,
        taskId: input.taskId,
        kind: input.kind,
        prompt: "",
        scheduleType: input.scheduleType,
        scheduleValue: input.scheduleValue,
        continuity: input.continuity ?? "",
        nextRun: input.nextRun,
        status: input.status,
        lastStatus: input.lastStatus,
        roundCount: input.roundCount,
        skippedCount: input.skippedCount,
        updatedAt: input.updatedAt,
      });
    },
    upsertPrompt(input) {
      const existing = crons.get(input.id);
      if (existing) {
        existing.prompt = input.prompt;
        existing.updatedAt = input.updatedAt;
        return;
      }
      crons.set(input.id, {
        id: input.id,
        taskId: input.taskId,
        kind: "",
        prompt: input.prompt,
        scheduleType: null,
        scheduleValue: null,
        continuity: "",
        nextRun: null,
        status: null,
        lastStatus: null,
        roundCount: 0,
        skippedCount: 0,
        updatedAt: input.updatedAt,
      });
    },
    get(cronId) {
      const row = crons.get(cronId);
      return row ? { ...row } : undefined;
    },
    listByTask(taskId) {
      return newestFirst([...crons.values()].filter(c => c.taskId === taskId), c => c.updatedAt).map(c => ({ ...c }));
    },
  };
}
