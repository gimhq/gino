import type { AppDatabase } from "@/db";
import type { Storage } from "@/host/ports/storage";
import type { DecodedFrame } from "@/protocol/envelope";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent } from "@/agents/registry";
import { createDb } from "@/db";
import { getThreadRun, writeCronState } from "@/msgstore";
import { applyFrame, ingestHttpFrame } from "@/protocol/inbound";
import { sqliteStorage } from "@/store/sqlite";
import { createTask, setPlacement } from "@/tasks";

// HOST-M1M2-06 / HOST-M5M6-06 — `thread_result` dispatch context is derived at
// write time from the agent's cron-minted thread-id shape
// (`<cronId>@<scheduled_epoch_millis>` isolated / `<cronId>@shared` shared):
// a thread whose id prefix names a known cron of the same task lands in
// `thread_runs` with `source='cron'` + `cron_id`; every other thread —
// interactive, unknown prefix, malformed suffix, or another task's cron —
// stays `source='chat'`, `cron_id=NULL`.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** Enroll a fresh agent, create a task, and place it on that agent. */
function placedAgentTask(): { agentId: string; taskId: string } {
  const agentId = enrollAgent(storage, { capabilities: { engines: ["claude"] } }).agent.id;
  const taskId = createTask(db, { ownerId: "owner", engine: "claude", model: "sonnet", status: "active" }).id;
  setPlacement(db, taskId, agentId);
  return { agentId, taskId };
}

/** Seed the crons mirror with one cron for `taskId` (as a `cron_state` would). */
function seedCron(taskId: string, cronId: string): void {
  writeCronState(storage, { taskId, cronId, data: { kind: "interval", round_count: 0, skipped_count: 0 } });
}

/** A valid `thread_result` frame for the given ids. */
function resultFrame(taskId: string, threadId: string, outboxId: number): DecodedFrame {
  return { type: "thread_result", outboxId, taskId, threadId, data: { status: "succeeded", duration_ms: 5, final_seq: 0 } };
}

describe("HOST-M1M2-06/M5M6-06 — thread_result derives source/cron_id from the thread-id shape", () => {
  it("records an isolated cron fire (`<cronId>@<epoch_millis>`) as source='cron' with the owning cron_id", () => {
    const { agentId, taskId } = placedAgentTask();
    seedCron(taskId, "cr-1");

    const ack = applyFrame({ storage, agentId }, resultFrame(taskId, "cr-1@1718000000000", 1));
    expect(ack).toEqual({ type: "ack", id: 1 });
    expect(getThreadRun(storage, "cr-1@1718000000000")).toMatchObject({ source: "cron", cronId: "cr-1", taskId });
  });

  it("records a shared-continuity fire (`<cronId>@shared`) as source='cron'", () => {
    const { agentId, taskId } = placedAgentTask();
    seedCron(taskId, "cr-2");

    applyFrame({ storage, agentId }, resultFrame(taskId, "cr-2@shared", 2));
    expect(getThreadRun(storage, "cr-2@shared")).toMatchObject({ source: "cron", cronId: "cr-2" });
  });

  it("leaves an interactive thread unchanged: source='chat', cron_id NULL", () => {
    const { agentId, taskId } = placedAgentTask();
    seedCron(taskId, "cr-3"); // a cron exists, but this thread is not its fire

    applyFrame({ storage, agentId }, resultFrame(taskId, "thr-chat-1", 3));
    expect(getThreadRun(storage, "thr-chat-1")).toMatchObject({ source: "chat", cronId: null });
  });

  it("treats a cron-shaped id whose prefix is not a known cron as chat", () => {
    const { agentId, taskId } = placedAgentTask();

    applyFrame({ storage, agentId }, resultFrame(taskId, "ghost@1718000000000", 4));
    expect(getThreadRun(storage, "ghost@1718000000000")).toMatchObject({ source: "chat", cronId: null });
  });

  it("does not attribute a run to ANOTHER task's cron (stays chat)", () => {
    const { agentId, taskId } = placedAgentTask();
    const otherTask = placedAgentTask();
    seedCron(otherTask.taskId, "cr-other");

    applyFrame({ storage, agentId }, resultFrame(taskId, "cr-other@1718000000000", 5));
    expect(getThreadRun(storage, "cr-other@1718000000000")).toMatchObject({ source: "chat", cronId: null });
  });

  it("survives malformed cron-ish thread ids without crashing ingest (all land as chat)", () => {
    const { agentId, taskId } = placedAgentTask();
    seedCron(taskId, "cr-4");
    const malformed = ["@", "@1718000000000", "cr-4@", "cr-4@not-a-number", "cr-4@12x34", "cr-4@SHARED"];

    for (const [i, threadId] of malformed.entries()) {
      const ack = applyFrame({ storage, agentId }, resultFrame(taskId, threadId, 10 + i));
      expect(ack).toEqual({ type: "ack", id: 10 + i }); // ingest never crashes
      expect(getThreadRun(storage, threadId)).toMatchObject({ source: "chat", cronId: null });
    }
  });

  it("derives the same context on the HTTP `/agent/output` fallback path", () => {
    const { agentId, taskId } = placedAgentTask();
    seedCron(taskId, "cr-5");
    const raw = JSON.stringify({ type: "thread_result", outboxId: 30, taskId, threadId: "cr-5@shared", data: { status: "succeeded", duration_ms: 5, final_seq: 0 } });

    expect(ingestHttpFrame({ storage, agentId }, raw)).toEqual({ type: "ack", id: 30 });
    expect(getThreadRun(storage, "cr-5@shared")).toMatchObject({ source: "cron", cronId: "cr-5" });
  });

  it("keeps source/cron_id stable across an at-least-once replay (set once at creation)", () => {
    const { agentId, taskId } = placedAgentTask();
    seedCron(taskId, "cr-6");
    const frame = resultFrame(taskId, "cr-6@1718000000000", 31);

    applyFrame({ storage, agentId }, frame);
    applyFrame({ storage, agentId }, frame); // replay is a safe no-op
    expect(getThreadRun(storage, "cr-6@1718000000000")).toMatchObject({ source: "cron", cronId: "cr-6" });
  });
});
