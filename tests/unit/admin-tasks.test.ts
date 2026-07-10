import type { AppDatabase } from "@/db";
import type { AppEnv } from "@/shared/lib/types";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { Hono } from "hono";
import { enrollAgent } from "@/agents/registry";
import { createDb } from "@/db";
import { outbox } from "@/db/schema";
import { adminTaskRoutes } from "@/edge/admin/tasks";
import { createTask, getTask, setPlacement } from "@/tasks/store";

// M4 admin surface — per-task close. Closing ONE task must never emit the
// global `close` frame (that winds down the whole runner); it interrupts the
// in-flight turn task-scoped and informs the agent via task_upsert
// {status:"closed"}.

let db: AppDatabase;

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

/** The admin task router with the test db injected (as app.ts does). */
function taskApp() {
  const app = new Hono<AppEnv>();
  app.use("*", (c, next) => {
    c.set("db", db);
    return next();
  });
  app.route("/", adminTaskRoutes());
  return app;
}

describe("POST /admin/tasks/:id/close", () => {
  it("marks closed + emits task-scoped interrupt & task_upsert — never a close frame", async () => {
    const agentId = enrollAgent(db, { capabilities: { engines: ["claude"] } }).agent.id;
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);

    const res = await taskApp().request(`/${task.id}/close`, { method: "POST" });
    expect(res.status).toBe(200);

    const body = await res.json() as { data: { task: { status: string }; outboxId: number } };
    expect(body.data.task.status).toBe("closed");

    const rows = db.select().from(outbox).where(eq(outbox.taskId, task.id)).all();
    expect(rows.filter(r => r.type === "close")).toHaveLength(0);

    const interrupt = rows.find(r => r.type === "interrupt");
    expect(interrupt).toBeDefined();
    expect(interrupt!.taskId).toBe(task.id);
    expect(body.data.outboxId).toBe(interrupt!.id);

    const upsert = rows.find(r => r.type === "task_upsert");
    expect(upsert).toBeDefined();
    expect((upsert!.dataJson as { status?: string }).status).toBe("closed");

    expect(getTask(db, task.id)?.status).toBe("closed");
  });
});
