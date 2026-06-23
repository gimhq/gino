import type { AppDatabase } from "@/db";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent, getAgent, setAgentStatus, touchLiveness, verifyConnectToken } from "@/agents/registry";
import { createDb } from "@/db";

// M3 registry — identity, the per-agent connect token (hash-at-rest), lifecycle.

let db: AppDatabase;

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

describe("M3 registry — enroll", () => {
  it("mints a ULID id + a connect token, stores only the hash", () => {
    const { agent, connectToken } = enrollAgent(db, {
      name: "alpha",
      source: "docker",
      capabilities: { engines: ["claude"], max_threads: 3 },
    });

    expect(agent.id).toMatch(/^[0-9a-z]{26}$/); // ULID
    expect(connectToken.length).toBeGreaterThanOrEqual(32);
    expect(agent.connectTokenHash).not.toBe(connectToken); // hashed at rest
    expect(agent.status).toBe("enrolled");
    expect(agent.capabilities).toEqual({ engines: ["claude"], max_threads: 3 });
    expect(agent.access).toBe("dedicated"); // default
  });
});

describe("M3 registry — verifyConnectToken", () => {
  it("accepts the right token, rejects wrong/unknown/removed", () => {
    const { agent, connectToken } = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } });

    expect(verifyConnectToken(db, agent.id, connectToken)?.id).toBe(agent.id);
    expect(verifyConnectToken(db, agent.id, "wrong-token")).toBeNull();
    expect(verifyConnectToken(db, "no-such-agent", connectToken)).toBeNull();
    expect(verifyConnectToken(db, agent.id, "")).toBeNull();

    // Removal revokes the token (OD12).
    setAgentStatus(db, agent.id, "removed");
    expect(verifyConnectToken(db, agent.id, connectToken)).toBeNull();
  });
});

describe("M3 registry — lifecycle + liveness", () => {
  it("setAgentStatus advances status", () => {
    const { agent } = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } });
    setAgentStatus(db, agent.id, "online");
    expect(getAgent(db, agent.id)?.status).toBe("online");
    setAgentStatus(db, agent.id, "offline");
    expect(getAgent(db, agent.id)?.status).toBe("offline");
  });

  it("touchLiveness updates last_seen + active_threads", () => {
    const { agent } = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } });
    touchLiveness(db, agent.id, 4);
    const got = getAgent(db, agent.id);
    expect(got?.lastSeen).toBeGreaterThan(0);
    expect(got?.activeThreads).toBe(4);
  });
});
