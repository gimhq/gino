import type { Metrics } from "../../src/host";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { createHostContext } from "../../src/host";
import { noopLogger } from "../../src/logger";
import { ingestHttpFrame } from "../../src/protocol/inbound";

// HOST — per-instance context: two contexts never share sessions/launchers,
// and the domain's counters flow through the injected `Metrics` sink.

const logger = noopLogger;

const fakeSocket = { send: () => {}, close: () => {} };

describe("createHostContext — isolation", () => {
  it("each context owns an independent session registry", () => {
    const db = createDb(":memory:");
    const a = createHostContext({ storage: sqliteStorage(db), logger });
    const b = createHostContext({ storage: sqliteStorage(db), logger });
    a.sessions.attach("agent-1", fakeSocket);
    expect(a.sessions.isOnline("agent-1")).toBe(true);
    expect(b.sessions.isOnline("agent-1")).toBe(false);
    db.close();
  });

  it("each context owns an independent launcher registry", () => {
    const db = createDb(":memory:");
    const a = createHostContext({ storage: sqliteStorage(db), logger });
    const b = createHostContext({ storage: sqliteStorage(db), logger });
    const fake = {
      kind: "fake-vm",
      capabilities: { canLaunch: true, canStop: true, canRestart: true, canInspect: true },
      launch: async () => ({ ref: "vm-1" }),
      stop: async () => {},
      restart: async () => {},
      remove: async () => {},
      inspect: async () => "running" as const,
    };
    a.launchers.register(fake);
    expect(a.launchers.resolve("fake-vm")).toBe(fake);
    expect(b.launchers.resolve("fake-vm").kind).toBe("manual");
    db.close();
  });

  it("defaults: manual launcher only (docker is app-registered), Date.now clock, platform ids", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    // Core ships no docker launcher: an unknown kind falls back to `manual` (D8).
    expect(host.launchers.resolve("docker").kind).toBe("manual");
    expect(host.launchers.resolve("manual").kind).toBe("manual");
    expect(host.clock).toBe(Date.now);
    expect(host.ids.generateId("agent").startsWith("agent_")).toBe(true);
    expect(host.ids.ulid()).toHaveLength(26);
    db.close();
  });

  it("exposes the Storage port it was built on", () => {
    const db = createDb(":memory:");
    const storage = sqliteStorage(db);
    const host = createHostContext({ storage, logger });
    expect(host.storage).toBe(storage);
    db.close();
  });
});

describe("createHostContext — metrics sink", () => {
  const poison = JSON.stringify({ type: "thread_message", taskId: "t", data: {} });

  it("the default sink is a no-op (nothing throws, nothing recorded)", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    expect(() => host.metrics.counterAdd("x", "y")).not.toThrow();
    expect(() => ingestHttpFrame({ storage: host.storage, agentId: "a", metrics: host.metrics }, poison)).not.toThrow();
    db.close();
  });

  it("an injected sink receives the inbound poison counter", () => {
    const db = createDb(":memory:");
    const calls: { name: string; value: number | undefined; labels: Record<string, string | number> | undefined }[] = [];
    const metrics: Metrics = { counterAdd: (name, _help, value, labels) => calls.push({ name, value, labels }) };
    const host = createHostContext({ storage: sqliteStorage(db), logger, metrics });
    expect(host.metrics).toBe(metrics);
    // A thread_message without a threadId is the `missing_ids` poison path — no enrollment needed.
    ingestHttpFrame({ storage: host.storage, agentId: "a", metrics: host.metrics }, poison);
    expect(calls).toEqual([
      { name: "inbound_poison_frames_total", value: 1, labels: { type: "thread_message", reason: "missing_ids" } },
    ]);
    db.close();
  });
});

describe("createHostContext — lifecycle", () => {
  it("start() twice then stop() twice does not throw", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    expect(() => host.start()).not.toThrow();
    expect(() => host.start()).not.toThrow();
    expect(() => host.stop()).not.toThrow();
    expect(() => host.stop()).not.toThrow();
  });

  it("stop() closes attached sockets with 1001 and takes them offline", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    const closed: Array<{ code?: number | undefined; reason?: string | undefined }> = [];
    const ws = { send: () => {}, close: (code?: number, reason?: string) => closed.push({ code, reason }) };
    host.sessions.attach("agent-1", ws);
    host.start();

    host.stop();
    expect(closed).toEqual([{ code: 1001, reason: "host shutting down" }]);
    expect(host.sessions.isOnline("agent-1")).toBe(false);
  });

  it("registered services stop in reverse order, before the db closes", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    const order: string[] = [];
    const dbClose = db.close;
    Object.assign(db, { close: () => {
      order.push("db.close");
      dbClose();
    } });
    host.registerService({ stop: () => order.push("first") });
    host.registerService({ stop: () => order.push("second") });
    host.registerService({ stop: () => {
      order.push("third");
      throw new Error("boom");
    } });
    host.start();

    expect(() => host.stop()).not.toThrow();
    expect(order).toEqual(["third", "second", "first", "db.close"]);
  });

  it("after stop() the db is closed", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    host.start();
    db.run(sql`SELECT 1`);

    host.stop();
    expect(() => db.run(sql`SELECT 1`)).toThrow();
  });

  it("stop() without start() still closes sockets and the db", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ storage: sqliteStorage(db), logger });
    const closed: number[] = [];
    host.sessions.attach("agent-1", { send: () => {}, close: (code?: number) => closed.push(code ?? -1) });

    host.stop();
    expect(closed).toEqual([1001]);
    expect(() => db.run(sql`SELECT 1`)).toThrow();
  });
});
