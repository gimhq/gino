import type { AppDatabase } from "@/db";
import type { Task } from "@/db/schema";
import type { FrameSender } from "@/protocol/outbox";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent } from "@/agents/registry";
import { GLOBAL_SCOPE, resolveEntries, setEntries } from "@/configstore";
import { createDb } from "@/db";
import { outbox } from "@/db/schema";
import { getCron } from "@/msgstore";
import { gcOutbox, OUTBOX_RETENTION_MS, replayUnacked } from "@/protocol/outbox";
import {
  broadcastGlobalConfig,
  emitClose,
  emitCronDelete,
  emitCronUpsert,
  emitInterrupt,
  emitMessage,
  emitTaskUpsert,
  hasSeededTask,
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

  it("seed state survives outbox retention GC (durable tasks.seeded_at flag)", () => {
    const task = placedTask();
    const first = sendTaskMessage(db, sender, task, { text: "go" });
    expect(first.taskUpsert).toBeDefined();

    // Ack the whole history and GC far past the retention window: the seed
    // frame's outbox row is gone, but the durable flag keeps the task seeded.
    db.update(outbox).set({ ackedAt: Date.now() }).run();
    expect(gcOutbox(db, { now: () => Date.now() + OUTBOX_RETENTION_MS + 1000 })).toBeGreaterThan(0);
    expect(db.select().from(outbox).all()).toHaveLength(0);
    expect(hasSeededTask(db, task.id)).toBe(true);

    const second = sendTaskMessage(db, sender, getTask(db, task.id)!, { text: "more" });
    expect(second.taskUpsert).toBeUndefined();
    expect(sender.sent.filter(s => s.frame.type === "task_upsert")).toHaveLength(1);
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

  it("emitCronUpsert persists the prompt into the crons mirror (HOST-M5M6-09)", () => {
    const task = placedTask();
    const { cronId } = emitCronUpsert(db, sender, task, {
      data: { kind: "schedule", continuity: "isolated", schedule_type: "cron", schedule_value: "0 9 * * *", prompt: "daily digest" },
    });
    expect(getCron(db, cronId)?.prompt).toBe("daily digest");

    // A later upsert with a new prompt overwrites it.
    emitCronUpsert(db, sender, task, { cronId, data: { kind: "schedule", continuity: "isolated", prompt: "weekly digest" } });
    expect(getCron(db, cronId)?.prompt).toBe("weekly digest");
  });

  it("first cron on a never-seeded task seeds task_upsert before the command (HOST-M4-02)", () => {
    const task = placedTask();
    expect(hasSeededTask(db, task.id)).toBe(false);

    const { cron } = emitCronUpsert(db, sender, task, {
      data: { kind: "schedule", continuity: "isolated", schedule_value: "0 9 * * *", prompt: "daily" },
    });

    // The seed frame exists, and it rides the outbox BEFORE the cron command.
    const rows = db.select().from(outbox).all();
    const seed = rows.find(r => r.type === "task_upsert");
    expect(seed).toBeDefined();
    expect(seed!.taskId).toBe(task.id);
    expect(seed!.id).toBeLessThan(cron.id);
    expect(hasSeededTask(db, task.id)).toBe(true);

    // A second cron does not re-seed.
    emitCronUpsert(db, sender, task, { data: { kind: "schedule", continuity: "isolated", prompt: "weekly" } });
    expect(db.select().from(outbox).all().filter(r => r.type === "task_upsert")).toHaveLength(1);
  });

  it("interrupt + close are task-scoped control frames", () => {
    const task = placedTask();
    expect(emitInterrupt(db, sender, task).type).toBe("interrupt");
    const close = emitClose(db, sender, task);
    expect(close.type).toBe("close");
    expect(close.taskId).toBe(task.id);
  });

  it("interrupt is live-only: refused while the agent is offline (HOST-M4-03)", () => {
    const task = placedTask();
    sender.online.delete(agentId);
    let code: string | undefined;
    try {
      emitInterrupt(db, sender, task);
    }
    catch (err) {
      code = (err as { code?: string }).code;
    }
    expect(code).toBe("AGENT_OFFLINE");
    expect(db.select().from(outbox).all().filter(r => r.type === "interrupt")).toHaveLength(0);
  });

  it("interrupt is never replayed on reconnect — no stale cancel (HOST-M4-03)", () => {
    const task = placedTask();
    const row = emitInterrupt(db, sender, task);
    expect(row.ackedAt).not.toBeNull(); // expired at enqueue: at-most-once
    expect(sender.sent.filter(s => s.frame.type === "interrupt")).toHaveLength(1); // live push happened

    // Reconnect replay pushes nothing for the interrupt.
    const replaySender = new FakeSender();
    replaySender.online.add(agentId);
    replayUnacked(db, replaySender, agentId);
    expect(replaySender.sent.filter(s => s.frame.type === "interrupt")).toHaveLength(0);
  });
});

describe("M4 orchestrator — broadcastGlobalConfig (HOST-M8-01 live re-push)", () => {
  it("pushes config_update{entries, version} to online agents, per their engine", () => {
    const codexId = enrollAgent(db, { capabilities: { engines: ["codex"] } }).agent.id;
    sender.online.add(codexId);
    setEntries(db, GLOBAL_SCOPE, [
      ...resolveEntries("claude", [{ name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" }]),
      ...resolveEntries("codex", [{ name: "instructions", sha256: "a".repeat(64), kind: "file" }]),
    ]);

    const rows = broadcastGlobalConfig(db, sender);
    expect(rows).toHaveLength(2);
    expect(rows.every(r => r.type === "config_update" && r.taskId === null)).toBe(true);

    const byAgent = new Map(rows.map(r => [r.agentId, r.dataJson as { entries: { target: string }[]; version: number }]));
    expect(byAgent.get(agentId)!.version).toBe(1);
    expect(byAgent.get(agentId)!.entries.map(e => e.target)).toEqual(["CLAUDE.md"]);
    expect(byAgent.get(codexId)!.entries.map(e => e.target)).toEqual(["AGENTS.md"]);

    // Both frames went out live (the sender reported the agents online).
    expect(sender.sent.filter(s => s.frame.type === "config_update")).toHaveLength(2);
  });

  it("skips offline agents (they converge on the next versioned config pull)", () => {
    enrollAgent(db, { capabilities: { engines: ["claude"] } }); // never marked online
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" }]));

    const rows = broadcastGlobalConfig(db, sender);
    expect(rows.map(r => r.agentId)).toEqual([agentId]);
  });
});
