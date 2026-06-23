import type { InspectState, Launcher } from "@/agents/launchers/types";
import type { AppDatabase } from "@/db";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import {
  grantAgentOwner,
  inspectAgent,
  removeAgentControl,
  restartAgent,
  revokeAgentOwner,
  stopAgent,
} from "@/agents/control";
import { enrollAgent, isOwnerPermitted, verifyConnectToken } from "@/agents/registry";
import { createDb } from "@/db";
import { agents, outbox, tasks } from "@/db/schema";

// M3 control — OOB stop/restart/remove/inspect (launcher-delegated, WS-close
// fallback) + grant/revoke. The launcher + the live sender are injected.

let db: AppDatabase;

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

/** A live-sender double recording every host→agent push. */
function fakeSender(online = true) {
  const sent: Array<{ agentId: string; frame: Record<string, unknown> }> = [];
  return {
    sent,
    sender: {
      isOnline: () => online,
      send: (agentId: string, frame: Record<string, unknown>) => {
        sent.push({ agentId, frame });
        return true;
      },
    },
  };
}

interface LauncherCalls {
  stop: string[];
  restart: string[];
  remove: string[];
  inspect: string[];
}

/** A launcher double with full runtime control, recording the refs it was given. */
function capableLauncher(): { launcher: Launcher; calls: LauncherCalls } {
  const calls: LauncherCalls = { stop: [], restart: [], remove: [], inspect: [] };
  const launcher: Launcher = {
    kind: "docker",
    capabilities: { canLaunch: true, canStop: true, canRestart: true, canInspect: true },
    launch: async () => ({ ref: "c-new" }),
    stop: async (ref) => { calls.stop.push(ref); },
    restart: async (ref) => { calls.restart.push(ref); },
    remove: async (ref) => { calls.remove.push(ref); },
    inspect: async (ref): Promise<InspectState> => {
      calls.inspect.push(ref);
      return "running";
    },
  };
  return { launcher, calls };
}

function enrollOnline(access = "dedicated") {
  const { agent, connectToken } = enrollAgent(db, { source: "manual", access, capabilities: { engines: ["claude"] } });
  return { agentId: agent.id, connectToken };
}

function setRuntime(agentId: string, kind: string, ref: string) {
  db.update(agents).set({ launcherKind: kind, launchRef: ref }).where(eq(agents.id, agentId)).run();
}

describe("M3 control — stop", () => {
  it("manual (no capability) falls back to a WS close via the outbox", async () => {
    const { agentId } = enrollOnline();
    const { sender, sent } = fakeSender();

    const res = await stopAgent({ db, sender }, agentId);

    expect(res.via).toBe("ws-close");
    expect(res.launcherKind).toBe("manual");
    // A durable close frame was persisted AND pushed to the live socket.
    const rows = db.select().from(outbox).where(eq(outbox.agentId, agentId)).all();
    expect(rows).toHaveLength(1);
    expect(rows[0]?.type).toBe("close");
    expect(sent).toHaveLength(1);
  });

  it("a capable launcher is delegated to by launch_ref (no WS close)", async () => {
    const { agentId } = enrollOnline();
    setRuntime(agentId, "docker", "c-123");
    const { launcher, calls } = capableLauncher();
    const { sender, sent } = fakeSender();

    const res = await stopAgent({ db, sender, resolveLauncher: () => launcher }, agentId);

    expect(res.via).toBe("launcher");
    expect(calls.stop).toEqual(["c-123"]);
    expect(sent).toHaveLength(0); // no fallback
    expect(db.select().from(outbox).all()).toHaveLength(0);
  });
});

describe("M3 control — restart", () => {
  it("manual → WS close; capable launcher → launcher.restart(ref)", async () => {
    const { agentId } = enrollOnline();
    const manual = await restartAgent({ db, sender: fakeSender().sender }, agentId);
    expect(manual.via).toBe("ws-close");

    setRuntime(agentId, "docker", "c-9");
    const { launcher, calls } = capableLauncher();
    const viaLauncher = await restartAgent({ db, sender: fakeSender().sender, resolveLauncher: () => launcher }, agentId);
    expect(viaLauncher.via).toBe("launcher");
    expect(calls.restart).toEqual(["c-9"]);
  });
});

describe("M3 control — remove", () => {
  it("revokes the connect token + orphans tasks (mirror kept) via WS close", async () => {
    const { agentId, connectToken } = enrollOnline();
    db.insert(tasks).values({
      id: "t-1",
      ownerId: "owner-1",
      engine: "claude",
      model: "sonnet",
      status: "running",
      agentId,
    }).run();
    const { sender } = fakeSender();

    const res = await removeAgentControl({ db, sender }, agentId);

    expect(res.via).toBe("ws-close");
    expect(res.orphanedTasks).toBe(1);
    // Token revoked (OD12): the old connect token no longer authenticates.
    expect(verifyConnectToken(db, agentId, connectToken)).toBeNull();
    expect(db.select().from(agents).where(eq(agents.id, agentId)).get()?.status).toBe("removed");
    // DP4: the task survives as history — row kept, placement cleared.
    const task = db.select().from(tasks).where(eq(tasks.id, "t-1")).get();
    expect(task).toBeDefined();
    expect(task?.agentId).toBeNull();
  });

  it("delegates teardown to a capable launcher", async () => {
    const { agentId } = enrollOnline();
    setRuntime(agentId, "docker", "c-42");
    const { launcher, calls } = capableLauncher();

    const res = await removeAgentControl({ db, sender: fakeSender().sender, resolveLauncher: () => launcher }, agentId);

    expect(res.via).toBe("launcher");
    expect(calls.remove).toEqual(["c-42"]);
  });
});

describe("M3 control — inspect", () => {
  it("manual → 'unknown'; capable launcher → its runtime state", async () => {
    const { agentId } = enrollOnline();
    expect(await inspectAgent({ db, sender: fakeSender().sender }, agentId)).toBe("unknown");

    setRuntime(agentId, "docker", "c-7");
    const { launcher, calls } = capableLauncher();
    expect(await inspectAgent({ db, sender: fakeSender().sender, resolveLauncher: () => launcher }, agentId)).toBe("running");
    expect(calls.inspect).toEqual(["c-7"]);
  });
});

describe("M3 control — grant/revoke enforce OD13 (dedicated vs public)", () => {
  it("dedicated: an owner must be granted; revoke removes access", () => {
    const { agentId } = enrollOnline("dedicated");
    const deps = { db, sender: fakeSender().sender };

    expect(isOwnerPermitted(db, agentId, "owner-x")).toBe(false);
    grantAgentOwner(deps, agentId, "owner-x");
    expect(isOwnerPermitted(db, agentId, "owner-x")).toBe(true);
    expect(isOwnerPermitted(db, agentId, "owner-y")).toBe(false);
    revokeAgentOwner(deps, agentId, "owner-x");
    expect(isOwnerPermitted(db, agentId, "owner-x")).toBe(false);
  });

  it("public: any owner is permitted without a grant", () => {
    const { agentId } = enrollOnline("public");
    expect(isOwnerPermitted(db, agentId, "anyone")).toBe(true);
  });

  it("grant/revoke on an unknown agent → 404 (NotFoundError)", () => {
    const deps = { db, sender: fakeSender().sender };
    expect(() => grantAgentOwner(deps, "no-such-agent", "owner-x")).toThrow();
    expect(() => revokeAgentOwner(deps, "no-such-agent", "owner-x")).toThrow();
  });
});
