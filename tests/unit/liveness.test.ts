import type { AppDatabase } from "@/db";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { OFFLINE_GRACE_MS, sweepLiveness } from "@/agents/liveness";
import { enrollAgent, getAgent, setAgentStatus, touchLiveness } from "@/agents/registry";
import { createDb } from "@/db";
import { tasks } from "@/db/schema";

// M3 liveness — the offline-grace sweep (DP4). Marks the AGENT offline; NEVER
// touches its tasks/crons.

let db: AppDatabase;

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

/** Enroll, bring online, and seed a placed task (the work that must survive). */
function onlineAgentWithTask() {
  const { agent } = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } });
  setAgentStatus(db, agent.id, "online");
  touchLiveness(db, agent.id, 2);
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
    const count = sweepLiveness({ db, now: () => future });

    expect(count).toBe(1);
    expect(getAgent(db, agentId)?.status).toBe("offline");

    // DP4: the task survives — same placement, same status, not deleted.
    const task = db.select().from(tasks).where(eq(tasks.id, "task-1")).get();
    expect(task).toBeDefined();
    expect(task?.agentId).toBe(agentId);
    expect(task?.status).toBe("running");
  });

  it("leaves a freshly-heartbeating agent online", () => {
    const agentId = onlineAgentWithTask();
    // now is just after the heartbeat — well within the grace window.
    const count = sweepLiveness({ db, now: () => Date.now() + 1_000 });
    expect(count).toBe(0);
    expect(getAgent(db, agentId)?.status).toBe("online");
  });

  it("never touches an enrolled-but-never-connected agent (last_seen null)", () => {
    const { agent } = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } });
    expect(agent.lastSeen).toBeNull();
    const count = sweepLiveness({ db, now: () => Date.now() + OFFLINE_GRACE_MS + 1_000 });
    expect(count).toBe(0);
    expect(getAgent(db, agent.id)?.status).toBe("enrolled");
  });

  it("is idempotent — an already-offline agent is not re-counted", () => {
    const agentId = onlineAgentWithTask();
    const future = Date.now() + OFFLINE_GRACE_MS + 1_000;
    expect(sweepLiveness({ db, now: () => future })).toBe(1);
    expect(sweepLiveness({ db, now: () => future })).toBe(0);
    expect(getAgent(db, agentId)?.status).toBe("offline");
  });
});
