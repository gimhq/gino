import type { AppDatabase } from "@/db";
import type { Task } from "@/db/schema";
import type { FrameSender } from "@/protocol/outbox";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent } from "@/agents/registry";
import { createDb } from "@/db";
import {
  emitClose,
  emitCronDelete,
  emitCronUpsert,
  emitInterrupt,
  emitMessage,
  emitTaskUpsert,
  sendTaskMessage,
} from "@/tasks/orchestrator";
import { createTask, getTask, setPlacement } from "@/tasks/store";

// M4 orchestrator — the host→agent SOURCE: every command rides the M2 outbox.

let db: AppDatabase;

/** Records every frame pushed; reports the agent online so enqueue delivers. */
class FakeSender implements FrameSender {
  readonly online = new Set<string>();
  readonly sent: Array<{ agentId: string; frame: Record<string, unknown> }> = [];
  isOnline(agentId: string): boolean {
    return this.online.has(agentId);
  }

  send(agentId: string, frame: Record<string, unknown>): boolean {
    this.sent.push({ agentId, frame });
    return true;
  }
}

let sender: FakeSender;
let agentId: string;

beforeEach(() => {
  db = createDb(":memory:");
  sender = new FakeSender();
  agentId = enrollAgent(db, { capabilities: { engines: ["claude"] } }).agent.id;
  sender.online.add(agentId);
});

afterEach(() => {
  db.close();
});

/** A placed task (orchestrator requires a SOURCE target). */
function placedTask(): Task {
  const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet", name: "demo", workdir: "/w", meta: { x: 1 } });
  setPlacement(db, task.id, agentId);
  return getTask(db, task.id)!;
}

describe("M4 orchestrator — task_upsert", () => {
  it("seeds the OD15 payload (engine/model/workdir/meta) and rides the outbox", () => {
    const task = placedTask();
    const row = emitTaskUpsert(db, sender, task);

    expect(row.type).toBe("task_upsert");
    expect(row.taskId).toBe(task.id);
    expect(row.dataJson).toMatchObject({ engine: "claude", model: "sonnet", workdir: "/w", meta: { x: 1 }, name: "demo", status: task.status });

    const wire = sender.sent.at(-1)!.frame;
    expect(wire.type).toBe("task_upsert");
    expect(wire.taskId).toBe(task.id);
    expect(wire.outboxId).toBe(row.id);
  });

  it("carries task-scope config entries when supplied (M8 passthrough)", () => {
    const task = placedTask();
    const entries = [{ name: "CLAUDE.md", sha256: "abc", target: "CLAUDE.md", kind: "file" }];
    const row = emitTaskUpsert(db, sender, task, { entries });
    expect((row.dataJson as { entries?: unknown[] }).entries).toEqual(entries);
  });

  it("refuses to emit for an unplaced task (409)", () => {
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    let code: number | undefined;
    try {
      emitTaskUpsert(db, sender, task);
    }
    catch (err) {
      code = (err as { statusCode?: number }).statusCode;
    }
    expect(code).toBe(409);
  });
});

describe("M4 orchestrator — message threading (OD7+OD9)", () => {
  it("opens a thread on the first message + records it as current; continue reuses it", () => {
    const task = placedTask();
    const first = emitMessage(db, sender, task, { text: "hello" });
    expect(first.threadId).toMatch(/^[0-9a-z]{26}$/);
    expect(first.message.type).toBe("message");
    expect(first.message.msgId).toBe(first.message.id); // message carries msgId
    expect(getTask(db, task.id)?.currentThreadId).toBe(first.threadId);

    // Continue: re-fetch the (now-threaded) task → same thread.
    const continued = emitMessage(db, sender, getTask(db, task.id)!, { text: "again" });
    expect(continued.threadId).toBe(first.threadId);

    // The wire message frame carries msgId + threadId (not outboxId).
    const wire = sender.sent.at(-1)!.frame;
    expect(wire.type).toBe("message");
    expect(wire.threadId).toBe(first.threadId);
    expect(wire.msgId).toBe(continued.message.id);
    expect(wire.outboxId).toBeUndefined();
  });

  it("newThread opens a fresh thread within the task", () => {
    const task = placedTask();
    const first = emitMessage(db, sender, task, { text: "a" });
    const fresh = emitMessage(db, sender, getTask(db, task.id)!, { text: "b", newThread: true });
    expect(fresh.threadId).not.toBe(first.threadId);
    expect(getTask(db, task.id)?.currentThreadId).toBe(fresh.threadId);
  });
});

describe("M4 orchestrator — sendTaskMessage seeds task_upsert once", () => {
  it("first message seeds task_upsert + message; subsequent messages do not re-seed", () => {
    const task = placedTask();
    const first = sendTaskMessage(db, sender, task, { text: "go" });
    expect(first.taskUpsert).toBeDefined();
    expect(first.taskUpsert!.type).toBe("task_upsert");

    const second = sendTaskMessage(db, sender, getTask(db, task.id)!, { text: "more" });
    expect(second.taskUpsert).toBeUndefined();

    // Exactly one task_upsert frame went out.
    expect(sender.sent.filter(s => s.frame.type === "task_upsert")).toHaveLength(1);
    expect(sender.sent.filter(s => s.frame.type === "message")).toHaveLength(2);
  });
});

describe("M4 orchestrator — cron commands + control", () => {
  it("emitCronUpsert mints a cronId + emits the command; cron_delete addresses it", () => {
    const task = placedTask();
    const { cron, cronId } = emitCronUpsert(db, sender, task, {
      data: { kind: "schedule", continuity: "isolated", schedule_type: "cron", schedule_value: "0 9 * * *", prompt: "daily" },
    });
    expect(cronId).toMatch(/^[0-9a-z]{26}$/);
    expect(cron.type).toBe("cron_upsert");
    expect(cron.cronId).toBe(cronId);
    expect(cron.dataJson).toMatchObject({ kind: "schedule", continuity: "isolated", prompt: "daily" });

    const del = emitCronDelete(db, sender, task, cronId);
    expect(del.type).toBe("cron_delete");
    expect(del.cronId).toBe(cronId);
  });

  it("interrupt + close are task-scoped control frames", () => {
    const task = placedTask();
    expect(emitInterrupt(db, sender, task).type).toBe("interrupt");
    const close = emitClose(db, sender, task);
    expect(close.type).toBe("close");
    expect(close.taskId).toBe(task.id);
  });
});
