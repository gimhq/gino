import type { Storage } from "@gino/host-core";
import type { AppDatabase } from "@gino/host-store-sqlite";
import type { HostEnv } from "../../src/env";
import { createHost, noopLogger } from "@gino/host-core";
import { createTask, enrollAgent, getCron, getTask, setPlacement, writeThreadMessage, writeThreadResult } from "@gino/host-core/domain";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { outbox } from "@gino/host-store-sqlite/schema";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { Hono } from "hono";
import { adminTaskRoutes } from "../../src/admin/tasks";
import { errorHandler } from "../../src/errors";

// M4 admin surface — close lifecycle, cron addressing,
// live-only interrupt, and the bounded task
// detail view.

const logger = noopLogger;

let db: AppDatabase;
let storage: Storage;
let host: ReturnType<typeof createHost>;
const attached: string[] = [];

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
  host = createHost({ storage, logger });
});

afterEach(() => {
  for (const id of attached.splice(0)) {
    host.sessions.detach(id);
  }
  db.close();
});

/** The admin task router with the test host injected (as app.ts does). */
function taskApp() {
  const app = new Hono<HostEnv>();
  app.onError(errorHandler);
  app.use("*", (c, next) => {
    c.set("host", host);
    return next();
  });
  app.route("/", adminTaskRoutes());
  return app;
}

/** Enroll an agent; optionally attach a fake socket so `sessions` reports it online. */
function enrolledAgent(opts: { online?: boolean } = {}): string {
  const agentId = enrollAgent(storage, { capabilities: { engines: ["claude"] } }).agent.id;
  if (opts.online) {
    host.sessions.attach(agentId, { send: () => {}, close: () => {} });
    attached.push(agentId);
  }
  return agentId;
}

function placedTask(agentId: string) {
  const task = createTask(storage, { ownerId: "o", engine: "claude", model: "sonnet" });
  setPlacement(storage, task.id, agentId);
  return getTask(storage, task.id)!;
}

describe("POST /admin/tasks/:id/close", () => {
  it("marks closed + emits task-scoped interrupt & task_upsert — never a close frame", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);

    const res = await taskApp().request(`/${task.id}/close`, { method: "POST" });
    expect(res.status).toBe(200);

    const body = await res.json() as { data: { task: { status: string }; msgId: string } };
    expect(body.data.task.status).toBe("closed");

    const rows = db.select().from(outbox).where(eq(outbox.taskId, task.id)).all();
    expect(rows.filter(r => r.type === "close")).toHaveLength(0);

    const interrupt = rows.find(r => r.type === "interrupt");
    expect(interrupt).toBeDefined();
    expect(interrupt!.taskId).toBe(task.id);
    expect(body.data.msgId).toBe(interrupt!.id);

    const upsert = rows.find(r => r.type === "task_upsert");
    expect(upsert).toBeDefined();
    expect((upsert!.dataJson as { status?: string }).status).toBe("closed");

    expect(getTask(storage, task.id)?.status).toBe("closed");
  });

  it("closes a task on an OFFLINE agent: no interrupt, durable task_upsert only (HOST-M4-03)", async () => {
    const agentId = enrolledAgent(); // never online in `sessions`
    const task = placedTask(agentId);

    const res = await taskApp().request(`/${task.id}/close`, { method: "POST" });
    expect(res.status).toBe(200);
    const body = await res.json() as { data: { task: { status: string }; msgId: string | null } };
    expect(body.data.task.status).toBe("closed");
    expect(body.data.msgId).toBeNull();

    const rows = db.select().from(outbox).where(eq(outbox.taskId, task.id)).all();
    expect(rows.filter(r => r.type === "interrupt")).toHaveLength(0);
    expect(rows.filter(r => r.type === "task_upsert")).toHaveLength(1);
  });

  it("closes a NEVER-PLACED task: pure status write, no 409, no frames (HOST-M4-09)", async () => {
    const task = createTask(storage, { ownerId: "o", engine: "claude", model: "sonnet" });
    expect(task.agentId ?? null).toBeNull();

    const res = await taskApp().request(`/${task.id}/close`, { method: "POST" });
    expect(res.status).toBe(200);
    const body = await res.json() as { data: { task: { status: string }; msgId: string | null } };
    expect(body.data.task.status).toBe("closed");
    expect(body.data.msgId).toBeNull();

    expect(getTask(storage, task.id)?.status).toBe("closed");
    expect(db.select().from(outbox).all()).toHaveLength(0);
  });
});

describe("POST /admin/tasks/:id/interrupt", () => {
  it("409s while the agent is offline — interrupt is live-only (HOST-M4-03)", async () => {
    const agentId = enrolledAgent();
    const task = placedTask(agentId);

    const res = await taskApp().request(`/${task.id}/interrupt`, { method: "POST" });
    expect(res.status).toBe(409);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("AGENT_OFFLINE");
    expect(db.select().from(outbox).all()).toHaveLength(0);
  });

  it("delivers a task-scoped interrupt to an online agent", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);

    const res = await taskApp().request(`/${task.id}/interrupt`, { method: "POST" });
    expect(res.status).toBe(200);
    const row = db.select().from(outbox).all().find(r => r.type === "interrupt");
    expect(row).toBeDefined();
    expect(row!.taskId).toBe(task.id);
  });
});

describe("cron routes — addressing (HOST-M4-11) + first-cron seeding (HOST-M4-02)", () => {
  it("POST creates a cron and seeds the never-messaged task first", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);

    const res = await taskApp().request(`/${task.id}/crons`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", schedule_value: "0 9 * * *", prompt: "daily digest" }),
    });
    expect(res.status).toBe(201);
    const { data: { cronId } } = await res.json() as { data: { cronId: string } };

    const rows = db.select().from(outbox).where(eq(outbox.taskId, task.id)).all();
    const seed = rows.find(r => r.type === "task_upsert");
    const cron = rows.find(r => r.type === "cron_upsert");
    expect(seed).toBeDefined();
    expect(cron).toBeDefined();
    expect(seed!.id < cron!.id).toBe(true); // task exists agent-side before the cron FK needs it
    expect(getCron(storage, cronId)?.prompt).toBe("daily digest");
  });

  it("POST without a prompt is rejected (the agent requires it — HOST-M4-10)", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);
    const res = await taskApp().request(`/${task.id}/crons`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated" }),
    });
    expect(res.status).toBe(422);
  });

  it("PUT with an unknown cronId 404s instead of minting a second cron", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);

    const res = await taskApp().request(`/${task.id}/crons/01UNKNOWNCRONID0000000000X`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", prompt: "typo target" }),
    });
    expect(res.status).toBe(404);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("CRON_NOT_FOUND");
    expect(db.select().from(outbox).all().filter(r => r.type === "cron_upsert")).toHaveLength(0);
  });

  it("PUT on an existing cron updates it; omitted prompt keeps the current one", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);
    const created = await taskApp().request(`/${task.id}/crons`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", schedule_value: "0 9 * * *", prompt: "daily digest" }),
    });
    const { data: { cronId } } = await created.json() as { data: { cronId: string } };

    const res = await taskApp().request(`/${task.id}/crons/${cronId}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", schedule_value: "0 18 * * *" }),
    });
    expect(res.status).toBe(200);

    // The command still carries the (required) prompt, taken from the mirror.
    const upserts = db.select().from(outbox).all().filter(r => r.type === "cron_upsert");
    expect(upserts).toHaveLength(2);
    expect((upserts[1]!.dataJson as { prompt?: string }).prompt).toBe("daily digest");
    expect(getCron(storage, cronId)?.prompt).toBe("daily digest");
  });

  it("PUT 404s for a cron belonging to another task", async () => {
    const agentId = enrolledAgent({ online: true });
    const taskA = placedTask(agentId);
    const taskB = placedTask(agentId);
    const created = await taskApp().request(`/${taskA.id}/crons`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", prompt: "a's cron" }),
    });
    const { data: { cronId } } = await created.json() as { data: { cronId: string } };

    const res = await taskApp().request(`/${taskB.id}/crons/${cronId}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", prompt: "hijack" }),
    });
    expect(res.status).toBe(404);
  });

  it("DELETE with an unknown cronId 404s (no blind cron_delete)", async () => {
    const agentId = enrolledAgent({ online: true });
    const task = placedTask(agentId);
    const res = await taskApp().request(`/${task.id}/crons/01UNKNOWNCRONID0000000000X`, { method: "DELETE" });
    expect(res.status).toBe(404);
    expect(db.select().from(outbox).all().filter(r => r.type === "cron_delete")).toHaveLength(0);
  });
});

describe("GET /admin/tasks/:id — bounded detail view (HOST-M1M2-16)", () => {
  /** Store `count` transcript events on a thread of the given task. */
  function seedThread(taskId: string, threadId: string, count: number) {
    for (let seq = 1; seq <= count; seq += 1) {
      writeThreadMessage(storage, { taskId, threadId, data: { seq, kind: "assistant_text", content: `msg ${seq}` } });
    }
    writeThreadResult(storage, { taskId, threadId, data: { status: "succeeded", duration_ms: 5, final_seq: count } });
  }

  it("returns thread SUMMARIES (run + counts + last seq), not transcripts", async () => {
    const agentId = enrolledAgent();
    const task = placedTask(agentId);
    seedThread(task.id, "thr-1", 3);

    const res = await taskApp().request(`/${task.id}`);
    expect(res.status).toBe(200);
    const body = await res.json() as { data: { threads: Array<Record<string, unknown>> } };
    expect(body.data.threads).toHaveLength(1);
    const thread = body.data.threads[0]!;
    expect(thread.threadId).toBe("thr-1");
    expect(thread.messageCount).toBe(3);
    expect(thread.lastSeq).toBe(3);
    expect((thread.run as { status: string }).status).toBe("succeeded");
    expect(thread.messages).toBeUndefined(); // the unbounded transcript is gone
  });

  it("pages one transcript via ?thread=&after=&limit= (seq-cursor shape)", async () => {
    const agentId = enrolledAgent();
    const task = placedTask(agentId);
    seedThread(task.id, "thr-1", 5);

    const page1 = await taskApp().request(`/${task.id}?thread=thr-1&limit=2`);
    expect(page1.status).toBe(200);
    const b1 = await page1.json() as { data: { threadId: string; messages: Array<{ seq: number; content: string }> } };
    expect(b1.data.threadId).toBe("thr-1");
    expect(b1.data.messages.map(m => m.seq)).toEqual([1, 2]);

    const page2 = await taskApp().request(`/${task.id}?thread=thr-1&after=2&limit=2`);
    const b2 = await page2.json() as { data: { messages: Array<{ seq: number }> } };
    expect(b2.data.messages.map(m => m.seq)).toEqual([3, 4]);

    const tail = await taskApp().request(`/${task.id}?thread=thr-1&after=4&limit=2`);
    const b3 = await tail.json() as { data: { messages: Array<{ seq: number }> } };
    expect(b3.data.messages.map(m => m.seq)).toEqual([5]);
  });

  it("404s a thread that does not belong to the task", async () => {
    const agentId = enrolledAgent();
    const task = placedTask(agentId);
    const other = placedTask(agentId);
    seedThread(other.id, "thr-other", 1);

    const res = await taskApp().request(`/${task.id}?thread=thr-other`);
    expect(res.status).toBe(404);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("THREAD_NOT_FOUND");
  });

  it("422s garbage paging params instead of coercing them to NaN", async () => {
    const agentId = enrolledAgent();
    const task = placedTask(agentId);
    seedThread(task.id, "thr-1", 1);
    const res = await taskApp().request(`/${task.id}?thread=thr-1&after=latest&limit=zap`);
    expect(res.status).toBe(422);
  });
});
