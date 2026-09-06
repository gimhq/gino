import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Storage } from "../../src/host/ports/storage";
import { createDb, sqliteStorage } from "@gino/host-store-sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent, getAgent, setAgentStatus, touchLiveness, verifyConnectToken } from "../../src/agents/registry";

// M3 registry — identity, the per-agent connect token (hash-at-rest), lifecycle.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

describe("M3 registry — enroll", () => {
  it("mints a ULID id + a connect token, stores only the hash", () => {
    const { agent, connectToken } = enrollAgent(storage, {
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
    const { agent, connectToken } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });

    expect(verifyConnectToken(storage, agent.id, connectToken)?.id).toBe(agent.id);
    expect(verifyConnectToken(storage, agent.id, "wrong-token")).toBeNull();
    expect(verifyConnectToken(storage, "no-such-agent", connectToken)).toBeNull();
    expect(verifyConnectToken(storage, agent.id, "")).toBeNull();

    // Removal revokes the token (OD12).
    setAgentStatus(storage, agent.id, "removed");
    expect(verifyConnectToken(storage, agent.id, connectToken)).toBeNull();
  });
});

describe("M3 registry — lifecycle + liveness", () => {
  it("setAgentStatus advances status", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    setAgentStatus(storage, agent.id, "online");
    expect(getAgent(storage, agent.id)?.status).toBe("online");
    setAgentStatus(storage, agent.id, "offline");
    expect(getAgent(storage, agent.id)?.status).toBe("offline");
  });

  it("removed is terminal — a late status write cannot resurrect the agent", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    setAgentStatus(storage, agent.id, "online");
    setAgentStatus(storage, agent.id, "removed");

    // The WS close the remove itself provoked lands afterwards (HOST-M3-01).
    setAgentStatus(storage, agent.id, "offline");
    expect(getAgent(storage, agent.id)?.status).toBe("removed");
    setAgentStatus(storage, agent.id, "online");
    expect(getAgent(storage, agent.id)?.status).toBe("removed");
  });

  it("touchLiveness updates last_seen + active_threads", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    touchLiveness(storage, agent.id, 4);
    const got = getAgent(storage, agent.id);
    expect(got?.lastSeen).toBeGreaterThan(0);
    expect(got?.activeThreads).toBe(4);
  });

  it("touchLiveness restores online for an offline agent (HOST-M3-03)", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    setAgentStatus(storage, agent.id, "online");
    setAgentStatus(storage, agent.id, "offline");

    // A heartbeat lands on the still-live socket → the one-way flip heals.
    touchLiveness(storage, agent.id, 1);
    expect(getAgent(storage, agent.id)?.status).toBe("online");
  });

  it("touchLiveness never resurrects a removed agent (HOST-M3-03 guard)", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    setAgentStatus(storage, agent.id, "online");
    setAgentStatus(storage, agent.id, "removed");

    touchLiveness(storage, agent.id, 1);
    const got = getAgent(storage, agent.id);
    expect(got?.status).toBe("removed");
    expect(got?.lastSeen).toBeGreaterThan(0); // liveness still recorded
  });

  it("touchLiveness leaves an enrolled agent enrolled (restore is offline-only)", () => {
    const { agent } = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } });
    touchLiveness(storage, agent.id);
    expect(getAgent(storage, agent.id)?.status).toBe("enrolled");
  });
});
