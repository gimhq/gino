import type { AppDatabase } from "@/db";
import type { Storage } from "@/host/ports/storage";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent, grantOwner, removeAgent, touchLiveness } from "@/agents/registry";
import { createDb } from "@/db";
import { sqliteStorage } from "@/store/sqlite";
import { assignTaskToAgent, placeTask } from "@/tasks/dispatch";
import { createTask, getTask, setStatus } from "@/tasks/store";

// M4 dispatch — owner-pinned placement (§3): dedicated-preferred, public-fallback,
// pending-assignment when none, no-spill, pin-once.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

interface AgentOpts {
  access?: string;
  engines?: string[];
  maxThreads?: number;
  /** Owner to grant on a `dedicated` agent. */
  grant?: string;
  /** Mark this many in-flight threads (capacity). */
  active?: number;
}

function makeAgent(opts: AgentOpts = {}): string {
  const { agent } = enrollAgent(storage, {
    access: opts.access ?? "dedicated",
    capabilities: { engines: opts.engines ?? ["claude"], ...(opts.maxThreads != null ? { max_threads: opts.maxThreads } : {}) },
  });
  if (opts.grant) {
    grantOwner(storage, agent.id, opts.grant);
  }
  if (opts.active != null) {
    touchLiveness(storage, agent.id, opts.active);
  }
  return agent.id;
}

function newTask(ownerId = "owner-1", engine = "claude") {
  return createTask(db, { ownerId, engine, model: "sonnet" });
}

describe("M4 dispatch — owner-permitted placement", () => {
  it("prefers a dedicated granted agent over a public one", () => {
    const dedicated = makeAgent({ access: "dedicated", grant: "owner-1" });
    makeAgent({ access: "public" }); // also permitted, but a fallback

    const r = placeTask(db, newTask().id);
    expect(r.placed).toBe(true);
    expect(r.agentId).toBe(dedicated);
    expect(r.status).toBe("active");
    expect(r.reason).toBe("placed");
  });

  it("falls back to a public agent when no dedicated agent is granted", () => {
    makeAgent({ access: "dedicated" }); // NOT granted to owner-1 → not permitted
    const pub = makeAgent({ access: "public" });

    const r = placeTask(db, newTask().id);
    expect(r.placed).toBe(true);
    expect(r.agentId).toBe(pub);
  });

  it("queues pending-assignment when no agent is permitted", () => {
    makeAgent({ access: "dedicated" }); // not granted to owner-1
    // (engine-mismatched public agent is also not a candidate)
    makeAgent({ access: "public", engines: ["codex"] });

    const task = newTask();
    const r = placeTask(db, task.id);
    expect(r.placed).toBe(false);
    expect(r.agentId).toBeNull();
    expect(r.status).toBe("pending-assignment");
    expect(r.reason).toBe("no-permitted-agent");
    expect(getTask(db, task.id)?.agentId).toBeNull();
  });

  it("excludes agents lacking the task's engine capability", () => {
    makeAgent({ access: "public", engines: ["codex"] });
    const claude = makeAgent({ access: "public", engines: ["claude"] });

    const r = placeTask(db, newTask("owner-1", "claude").id);
    expect(r.agentId).toBe(claude);
  });
});

describe("M4 dispatch — capacity never spills (no-spill)", () => {
  it("pins a full dedicated agent rather than spilling to a public one with room", () => {
    // The owner's dedicated agent is at capacity (1/1)...
    const dedicated = makeAgent({ access: "dedicated", grant: "owner-1", maxThreads: 1, active: 1 });
    // ...and a public agent has room — but capacity is back-pressure WITHIN the
    // chosen agent, never a spill across agents, so dedicated still wins.
    makeAgent({ access: "public", maxThreads: 5, active: 0 });

    const r = placeTask(db, newTask().id);
    expect(r.placed).toBe(true);
    expect(r.agentId).toBe(dedicated);
  });

  it("prefers the agent with spare capacity among same-tier candidates", () => {
    const roomy = makeAgent({ access: "public", maxThreads: 5, active: 0 });
    makeAgent({ access: "public", maxThreads: 1, active: 1 }); // full

    const r = placeTask(db, newTask().id);
    expect(r.agentId).toBe(roomy);
  });
});

describe("M4 dispatch — pin-once (set once, no migration)", () => {
  it("a second placeTask returns the existing pin (already-placed)", () => {
    const a = makeAgent({ access: "public" });
    const task = newTask();

    const first = placeTask(db, task.id);
    expect(first.agentId).toBe(a);
    const second = placeTask(db, task.id);
    expect(second.reason).toBe("already-placed");
    expect(second.agentId).toBe(a);
  });

  it("manual assign to a DIFFERENT agent after placement is rejected (409)", () => {
    const a = makeAgent({ access: "public" });
    const b = makeAgent({ access: "public" });
    const task = newTask();
    placeTask(db, task.id);
    expect(getTask(db, task.id)?.agentId).toBe(a);

    let code: number | undefined;
    try {
      assignTaskToAgent(db, task.id, b);
    }
    catch (err) {
      code = (err as { statusCode?: number }).statusCode;
    }
    expect(code).toBe(409);
  });

  it("re-assigning the SAME agent is a full no-op — a closed task stays closed (HOST-M4-06)", () => {
    const a = makeAgent({ access: "public" });
    const task = newTask();
    expect(assignTaskToAgent(db, task.id, a).status).toBe("active");

    setStatus(db, task.id, "closed"); // task wound down (e.g. POST /tasks/:id/close)
    const replay = assignTaskToAgent(db, task.id, a); // client retry of the assign
    expect(replay.reason).toBe("already-placed");
    expect(replay.agentId).toBe(a);
    expect(replay.status).toBe("closed");
    expect(getTask(db, task.id)?.status).toBe("closed"); // NOT resurrected to active
  });

  it("re-assigning the SAME agent to an active task reports already-placed (HOST-M4-06)", () => {
    const a = makeAgent({ access: "public" });
    const task = newTask();
    assignTaskToAgent(db, task.id, a);

    const replay = assignTaskToAgent(db, task.id, a);
    expect(replay.reason).toBe("already-placed");
    expect(replay.status).toBe("active");
  });

  it("re-placing an orphaned CLOSED task records the pin but does not reactivate it (HOST-M4-06)", () => {
    makeAgent({ access: "public" });
    const task = newTask();
    const first = placeTask(db, task.id);
    setStatus(db, task.id, "closed");
    removeAgent(storage, first.agentId!); // orphans the task (agent_id → NULL)

    const b = makeAgent({ access: "public" });
    const r = placeTask(db, task.id);
    expect(r.agentId).toBe(b);
    expect(r.status).toBe("closed");
    expect(getTask(db, task.id)?.status).toBe("closed");
  });

  it("manual assign validates permission + engine (422 on mismatch)", () => {
    const task = newTask("owner-1", "claude");
    const codexAgent = makeAgent({ access: "public", engines: ["codex"] });

    let code: number | undefined;
    try {
      assignTaskToAgent(db, task.id, codexAgent);
    }
    catch (err) {
      code = (err as { statusCode?: number }).statusCode;
    }
    expect(code).toBe(422);
  });
});
