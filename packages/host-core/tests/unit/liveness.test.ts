import type { AppDatabase } from "@gino/host-store-sqlite";
import type { WsLike } from "../../src/agents/sessions";
import type { Storage } from "../../src/host/ports/storage";
import { createDb, sqliteStorage } from "@gino/host-store-sqlite";
import { tasks } from "@gino/host-store-sqlite/schema";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { OFFLINE_GRACE_MS, sweepLiveness } from "../../src/agents/liveness";
import { enrollAgent, getAgent, setAgentStatus, touchLiveness } from "../../src/agents/registry";
import { SessionRegistry } from "../../src/agents/sessions";

// M3 liveness — the offline-grace sweep (DP4). Marks the AGENT offline; NEVER
// touches its tasks/crons.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** Enroll, bring online, and seed a placed task (the work that must survive). */
function onlineAgentWithTask() {
  const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
  setAgentStatus(storage, agent.id, "online");
  touchLiveness(storage, agent.id, 2);
  db.insert(tasks).values({
    id: "task-1",
    ownerId: "owner-1",
    engine: "claude",
    model: "sonnet",
    status: "running",
    agentId: agent.id,
    currentThreadId: "thread-1",
  }).run();
  return agent.id;
}

describe("M3 liveness sweep", () => {
  it("marks a silent online agent offline WITHOUT touching its tasks", () => {
    const agentId = onlineAgentWithTask();

    // Fake clock: now is past the grace window since the last heartbeat.
    const future = Date.now() + OFFLINE_GRACE_MS + 1_000;
    const count = sweepLiveness({ storage, now: () => future });

    expect(count).toBe(1);
    expect(getAgent(storage, agentId)?.status).toBe("offline");

    // DP4: the task survives — same placement, same status, not deleted.
    const task = db.select().from(tasks).where(eq(tasks.id, "task-1")).get();
    expect(task).toBeDefined();
    expect(task?.agentId).toBe(agentId);
    expect(task?.status).toBe("running");
  });

  it("leaves a freshly-heartbeating agent online", () => {
    const agentId = onlineAgentWithTask();
    // now is just after the heartbeat — well within the grace window.
    const count = sweepLiveness({ storage, now: () => Date.now() + 1_000 });
    expect(count).toBe(0);
    expect(getAgent(storage, agentId)?.status).toBe("online");
  });

  it("never touches an enrolled-but-never-connected agent (last_seen null)", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    expect(agent.lastSeen).toBeNull();
    const count = sweepLiveness({ storage, now: () => Date.now() + OFFLINE_GRACE_MS + 1_000 });
    expect(count).toBe(0);
    expect(getAgent(storage, agent.id)?.status).toBe("enrolled");
  });

  it("is idempotent — an already-offline agent is not re-counted", () => {
    const agentId = onlineAgentWithTask();
    const future = Date.now() + OFFLINE_GRACE_MS + 1_000;
    expect(sweepLiveness({ storage, now: () => future })).toBe(1);
    expect(sweepLiveness({ storage, now: () => future })).toBe(0);
    expect(getAgent(storage, agentId)?.status).toBe("offline");
  });

  it("closes + detaches the swept agent's zombie socket (HOST-M3-14)", () => {
    const agentId = onlineAgentWithTask();
    const sessions = new SessionRegistry();
    const closed: Array<{ code?: number | undefined; reason?: string | undefined }> = [];
    const zombie: WsLike = {
      send: () => {},
      close: (code?: number, reason?: string) => {
        closed.push({ code, reason });
      },
    };
    sessions.attach(agentId, zombie);

    const future = Date.now() + OFFLINE_GRACE_MS + 1_000;
    expect(sweepLiveness({ storage, sessions, now: () => future })).toBe(1);

    // Both views converge: DB says offline AND the session registry is empty.
    expect(getAgent(storage, agentId)?.status).toBe("offline");
    expect(sessions.isOnline(agentId)).toBe(false);
    expect(closed).toHaveLength(1);
    expect(closed[0]?.code).toBe(1001);
  });

  it("sweeps cleanly when the swept agent has no attached session", () => {
    const agentId = onlineAgentWithTask();
    const sessions = new SessionRegistry();
    const future = Date.now() + OFFLINE_GRACE_MS + 1_000;
    expect(sweepLiveness({ storage, sessions, now: () => future })).toBe(1);
    expect(getAgent(storage, agentId)?.status).toBe("offline");
  });

  it("a heartbeat after the sweep restores online — but never out of removed (HOST-M3-03)", () => {
    const agentId = onlineAgentWithTask();
    const future = Date.now() + OFFLINE_GRACE_MS + 1_000;
    expect(sweepLiveness({ storage, now: () => future })).toBe(1);
    expect(getAgent(storage, agentId)?.status).toBe("offline");

    // Heartbeat starvation ends; the still-connected agent resumes beating.
    touchLiveness(storage, agentId, 2);
    expect(getAgent(storage, agentId)?.status).toBe("online");

    // A removed agent is terminal: liveness never resurrects it.
    setAgentStatus(storage, agentId, "removed");
    touchLiveness(storage, agentId, 2);
    expect(getAgent(storage, agentId)?.status).toBe("removed");
  });
});
