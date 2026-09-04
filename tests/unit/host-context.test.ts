import type { Metrics } from "@/host";
import { describe, expect, it } from "bun:test";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { createHostContext } from "@/host";
import { ingestHttpFrame } from "@/protocol/inbound";
import { createLogger } from "@/shared/lib/logger";

// HOST — per-instance context: two contexts never share sessions/launchers,
// and the domain's counters flow through the injected `Metrics` sink.

const logger = createLogger(loadConfigStrict({ NODE_ENV: "test", GINO_LOG_LEVEL: "error", GINO_LOG_TO_STDOUT: "true" }));

const fakeSocket = { send: () => {}, close: () => {} };

describe("createHostContext — isolation", () => {
  it("each context owns an independent session registry", () => {
    const db = createDb(":memory:");
    const a = createHostContext({ db, logger });
    const b = createHostContext({ db, logger });
    a.sessions.attach("agent-1", fakeSocket);
    expect(a.sessions.isOnline("agent-1")).toBe(true);
    expect(b.sessions.isOnline("agent-1")).toBe(false);
    db.close();
  });

  it("each context owns an independent launcher registry", () => {
    const db = createDb(":memory:");
    const a = createHostContext({ db, logger });
    const b = createHostContext({ db, logger });
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

  it("defaults: manual + docker launchers, Date.now clock, platform ids", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ db, logger });
    expect(host.launchers.resolve("docker").kind).toBe("docker");
    expect(host.launchers.resolve("manual").kind).toBe("manual");
    expect(host.clock).toBe(Date.now);
    expect(host.ids.generateId("agent").startsWith("agent_")).toBe(true);
    expect(host.ids.ulid()).toHaveLength(26);
    db.close();
  });
});

describe("createHostContext — metrics sink", () => {
  const poison = JSON.stringify({ type: "thread_message", taskId: "t", data: {} });

  it("the default sink is a no-op (nothing throws, nothing recorded)", () => {
    const db = createDb(":memory:");
    const host = createHostContext({ db, logger });
    expect(() => host.metrics.counterAdd("x", "y")).not.toThrow();
    expect(() => ingestHttpFrame({ db, agentId: "a", metrics: host.metrics }, poison)).not.toThrow();
    db.close();
  });

  it("an injected sink receives the inbound poison counter", () => {
    const db = createDb(":memory:");
    const calls: { name: string; value: number | undefined; labels: Record<string, string | number> | undefined }[] = [];
    const metrics: Metrics = { counterAdd: (name, _help, value, labels) => calls.push({ name, value, labels }) };
    const host = createHostContext({ db, logger, metrics });
    expect(host.metrics).toBe(metrics);
    // A thread_message without a threadId is the `missing_ids` poison path — no enrollment needed.
    ingestHttpFrame({ db, agentId: "a", metrics: host.metrics }, poison);
    expect(calls).toEqual([
      { name: "inbound_poison_frames_total", value: 1, labels: { type: "thread_message", reason: "missing_ids" } },
    ]);
    db.close();
  });
});
