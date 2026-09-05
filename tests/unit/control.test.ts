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
import { DockerOpTimeoutError } from "@/agents/launchers";
import { enrollAgent, isOwnerPermitted, verifyConnectToken } from "@/agents/registry";
import { SessionRegistry } from "@/agents/sessions";
import { createDb } from "@/db";
import { agents, outbox, tasks } from "@/db/schema";
import { AppError } from "@/shared/lib/errors";

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
      closeAgent: () => false,
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

describe("M3 control — remove hardening (HOST-M3-04/07/12)", () => {
  function seedTask(agentId: string, id = "t-1") {
    db.insert(tasks).values({
      id,
      ownerId: "owner-1",
      engine: "claude",
      model: "sonnet",
      status: "running",
      agentId,
    }).run();
  }

  it("HOST-M3-04: a failing launcher still revokes the token + orphans tasks (degrades to ws close)", async () => {
    const { agentId, connectToken } = enrollOnline();
    setRuntime(agentId, "docker", "c-dead");
    seedTask(agentId);
    const { launcher } = capableLauncher();
    launcher.remove = async () => {
      throw new Error("docker rm failed (exit 1): daemon on fire");
    };
    const { sender, sent } = fakeSender();

    const res = await removeAgentControl({ db, sender, resolveLauncher: () => launcher }, agentId);

    expect(res.via).toBe("ws-close");
    expect(res.orphanedTasks).toBe(1);
    expect(verifyConnectToken(db, agentId, connectToken)).toBeNull();
    expect(db.select().from(agents).where(eq(agents.id, agentId)).get()?.status).toBe("removed");
    // The fallback close still went out on the live socket.
    expect(sent.some(s => s.frame.type === "close")).toBe(true);
  });

  it("HOST-M3-04: 'no such container' counts as launcher success (no fallback close)", async () => {
    const { agentId, connectToken } = enrollOnline();
    setRuntime(agentId, "docker", "c-gone");
    const { launcher } = capableLauncher();
    launcher.remove = async () => {
      throw new Error("docker rm failed (exit 1): Error: No such container: c-gone");
    };
    const { sender, sent } = fakeSender();

    const res = await removeAgentControl({ db, sender, resolveLauncher: () => launcher }, agentId);

    expect(res.via).toBe("launcher");
    expect(sent).toHaveLength(0);
    expect(verifyConnectToken(db, agentId, connectToken)).toBeNull();
  });

  it("HOST-M3-04: a DockerOpTimeoutError degrades to ws close; bookkeeping proceeds", async () => {
    const { agentId, connectToken } = enrollOnline();
    setRuntime(agentId, "docker", "c-wedged");
    const { launcher } = capableLauncher();
    launcher.remove = async () => {
      throw new DockerOpTimeoutError("rm", 30_000);
    };
    const { sender, sent } = fakeSender();

    const res = await removeAgentControl({ db, sender, resolveLauncher: () => launcher }, agentId);

    expect(res.via).toBe("ws-close");
    expect(sent.some(s => s.frame.type === "close")).toBe(true);
    expect(verifyConnectToken(db, agentId, connectToken)).toBeNull();
  });

  it("HOST-M3-04: remove is idempotent — a repeat on a removed agent is a no-op cleanup", async () => {
    const { agentId } = enrollOnline();
    setRuntime(agentId, "docker", "c-1");
    seedTask(agentId);
    const { launcher, calls } = capableLauncher();
    const deps = { db, sender: fakeSender().sender, resolveLauncher: () => launcher };

    const first = await removeAgentControl(deps, agentId);
    expect(first.orphanedTasks).toBe(1);

    const again = await removeAgentControl(deps, agentId);
    expect(again.orphanedTasks).toBe(0);
    expect(calls.remove).toEqual(["c-1"]); // launcher not re-invoked
    expect(db.select().from(outbox).all()).toHaveLength(0); // no new frames enqueued
  });

  it("HOST-M3-07: remove force-closes the live session host-side", async () => {
    const { agentId } = enrollOnline();
    const registry = new SessionRegistry();
    const closed: Array<{ code: number | undefined; reason: string | undefined }> = [];
    registry.attach(agentId, {
      send: () => {},
      close: (code?: number, reason?: string) => { closed.push({ code, reason }); },
    });
    expect(registry.isOnline(agentId)).toBe(true);

    await removeAgentControl({ db, sender: registry, sessions: registry }, agentId);

    // The socket was closed by the HOST and the session evicted — revocation
    // does not depend on the agent honouring the close frame.
    expect(closed).toHaveLength(1);
    expect(closed[0]?.code).toBe(1008);
    expect(registry.isOnline(agentId)).toBe(false);
  });

  it("HOST-M3-12: remove purges the agent's unacked outbox rows; other agents untouched", async () => {
    const { agentId } = enrollOnline();
    const other = enrollOnline().agentId;
    // Seed undeliverable frames while both agents are offline.
    const { sender } = fakeSender(false);
    await stopAgent({ db, sender }, agentId);
    await stopAgent({ db, sender }, other);
    expect(db.select().from(outbox).where(eq(outbox.agentId, agentId)).all()).toHaveLength(1);

    await removeAgentControl({ db, sender }, agentId);

    expect(db.select().from(outbox).where(eq(outbox.agentId, agentId)).all()).toHaveLength(0);
    expect(db.select().from(outbox).where(eq(outbox.agentId, other)).all()).toHaveLength(1);
  });

  it("HOST-M3-12: stop/restart/grant on a removed agent are rejected with 410; revoke still works", async () => {
    const { agentId } = enrollOnline();
    const deps = { db, sender: fakeSender().sender };
    grantAgentOwner(deps, agentId, "owner-x");
    await removeAgentControl(deps, agentId);

    for (const op of [() => stopAgent(deps, agentId), () => restartAgent(deps, agentId)]) {
      const err = await op().then(() => null, e => e as AppError);
      expect(err).toBeInstanceOf(AppError);
      expect(err?.statusCode).toBe(410);
      expect(err?.code).toBe("AGENT_REMOVED");
    }
    expect(() => grantAgentOwner(deps, agentId, "owner-y")).toThrow("is removed");
    // No frames were fed to the removed agent's queue by the rejected ops.
    expect(db.select().from(outbox).where(eq(outbox.agentId, agentId)).all()).toHaveLength(0);
    // Revoking a stale grant on a removed agent remains allowed (cleanup).
    revokeAgentOwner(deps, agentId, "owner-x");
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
