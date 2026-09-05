import type { AppDatabase } from "@/db";
import type { Storage } from "@/host/ports/storage";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent, removeAgent } from "@/agents/registry";
import { createDb } from "@/db";
import { sqliteStorage } from "@/store/sqlite";
import {
  createTask,
  getTask,
  listTaskCrons,
  listTasks,
  markSeeded,
  requireTask,
  setCurrentThread,
  setPlacement,
  setStatus,
} from "@/tasks/store";

// M4 store — host-authoritative task identity + placement (set-once).

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** Enroll a real agent so placement satisfies the `tasks.agent_id` FK. */
function agentId(access = "dedicated"): string {
  return enrollAgent(storage, { access, capabilities: { engines: ["claude"] } }).agent.id;
}

describe("M4 store — createTask", () => {
  it("mints a ULID id + persists the OD15 definition at pending-assignment", () => {
    const task = createTask(db, {
      ownerId: "owner-1",
      engine: "claude",
      model: "sonnet",
      workdir: "/w",
      meta: { k: "v" },
      name: "demo",
      kind: "chat",
    });

    expect(task.id).toMatch(/^[0-9a-z]{26}$/); // ULID
    expect(task.ownerId).toBe("owner-1");
    expect(task.engine).toBe("claude");
    expect(task.model).toBe("sonnet");
    expect(task.workdir).toBe("/w");
    expect(task.meta).toEqual({ k: "v" });
    expect(task.status).toBe("pending-assignment"); // unplaced
    expect(task.agentId).toBeNull();
    expect(task.currentThreadId).toBeNull();
  });
});

describe("M4 store — get / list", () => {
  it("getTask + requireTask (404 on unknown)", () => {
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    expect(getTask(db, task.id)?.id).toBe(task.id);
    expect(getTask(db, "nope")).toBeUndefined();
    expect(requireTask(db, task.id).id).toBe(task.id);
    expect(() => requireTask(db, "nope")).toThrow();
  });

  it("listTasks filters by owner / agent / status", () => {
    const a = agentId();
    const t1 = createTask(db, { ownerId: "alice", engine: "claude", model: "sonnet" });
    createTask(db, { ownerId: "bob", engine: "claude", model: "sonnet" });
    setPlacement(db, t1.id, a);
    setStatus(db, t1.id, "active");

    expect(listTasks(db).length).toBe(2);
    expect(listTasks(db, { ownerId: "alice" }).map(t => t.id)).toEqual([t1.id]);
    expect(listTasks(db, { agentId: a }).map(t => t.id)).toEqual([t1.id]);
    expect(listTasks(db, { ownerId: "bob" }).length).toBe(1);
    expect(listTasks(db, { status: "active" }).map(t => t.id)).toEqual([t1.id]);
    expect(listTasks(db, { status: "pending-assignment" }).length).toBe(1); // only bob's
  });
});

describe("M4 store — setPlacement (set-once)", () => {
  it("pins once; re-pin same agent is idempotent; a different agent is rejected (409)", () => {
    const a = agentId();
    const b = agentId();
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });

    const pinned = setPlacement(db, task.id, a);
    expect(pinned.agentId).toBe(a);
    expect(pinned.assignedAt).toBeGreaterThan(0);

    // Idempotent on the same agent.
    expect(setPlacement(db, task.id, a).agentId).toBe(a);

    // A different agent is a no-migration violation → 409.
    let code: number | undefined;
    try {
      setPlacement(db, task.id, b);
    }
    catch (err) {
      code = (err as { statusCode?: number }).statusCode;
    }
    expect(code).toBe(409);
    expect(getTask(db, task.id)?.agentId).toBe(a); // unchanged
  });

  it("re-placing an orphaned task resets seeded_at + current_thread_id (HOST-M4-05)", () => {
    const a = agentId();
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, a);
    markSeeded(db, task.id);
    setCurrentThread(db, task.id, "thread-old");

    removeAgent(storage, a); // orphans the task: agent_id → NULL
    expect(getTask(db, task.id)?.agentId).toBeNull();

    // The new pin must not drag the removed agent's seed/thread state along.
    const repinned = setPlacement(db, task.id, agentId());
    expect(repinned.seededAt).toBeNull();
    expect(repinned.currentThreadId).toBeNull();
  });

  it("re-pinning the SAME agent keeps the seed + thread state (no reset)", () => {
    const a = agentId();
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, a);
    markSeeded(db, task.id);
    setCurrentThread(db, task.id, "thread-1");

    const again = setPlacement(db, task.id, a);
    expect(again.seededAt).not.toBeNull();
    expect(again.currentThreadId).toBe("thread-1");
  });
});

describe("M4 store — status + current thread", () => {
  it("setStatus + setCurrentThread mutate the row", () => {
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    expect(setStatus(db, task.id, "active").status).toBe("active");
    expect(setCurrentThread(db, task.id, "thread-1").currentThreadId).toBe("thread-1");
  });
});

describe("M4 store — cron mirror read", () => {
  it("listTaskCrons reads the (M5-owned) mirror, empty until a cron_state arrives", () => {
    const task = createTask(db, { ownerId: "o", engine: "claude", model: "sonnet" });
    expect(listTaskCrons(db, task.id)).toEqual([]);
  });
});
