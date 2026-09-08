import { describe, expect, it } from "bun:test";
import { createHost } from "../../src/host";
import { noopLogger } from "../../src/logger";
import { memoryStorage } from "../../src/store/memory";

// HOST — `createHost` on the memory adapter: the SDK path an embedder takes
// (docs/host-sdk-design.md §3.1). The namespaces bind the domain to the
// instance; nothing here touches Hono, sqlite, or the process env.

const logger = noopLogger;

function makeHost() {
  return createHost({ storage: memoryStorage(), logger, agents: { registerToken: "reg" } });
}

describe("createHost", () => {
  it("starts and stops idempotently", () => {
    const host = makeHost();
    expect(() => host.start()).not.toThrow();
    expect(() => host.start()).not.toThrow();
    expect(() => host.stop()).not.toThrow();
    expect(() => host.stop()).not.toThrow();
  });

  it("maps the §3.1 option groups onto the per-instance options", () => {
    const host = createHost({
      storage: memoryStorage(),
      logger,
      agents: { registerToken: "reg", offlineGraceMs: 1234 },
      attachments: { maxUploadBytes: 99 },
      storeDir: "/tmp/gino-store",
      configGc: { enabled: true },
    });
    expect(host.options.registerToken).toBe("reg");
    expect(host.options.offlineGraceMs).toBe(1234);
    expect(host.options.maxUploadBytes).toBe(99);
    expect(host.options.storeDir).toBe("/tmp/gino-store");
    expect(host.options.configGc.enabled).toBe(true);
    host.stop();
  });

  it("agents.enroll round-trips through agents.get / agents.list", () => {
    const host = makeHost();
    const { agent, connectToken } = host.agents.enroll({ capabilities: { engines: ["claude"] } });
    expect(connectToken).toHaveLength(64);
    expect(host.agents.get(agent.id)?.status).toBe("enrolled");
    expect(host.agents.list().map(a => a.id)).toEqual([agent.id]);
    host.stop();
  });

  it("tasks.create + tasks.assign pins the task; tasks.send seeds a task_upsert on the outbox", () => {
    const host = makeHost();
    const { agent } = host.agents.enroll({ capabilities: { engines: ["claude"] } });
    host.agents.grant(agent.id, "owner-1");
    const task = host.tasks.create({ ownerId: "owner-1", engine: "claude", model: "m" });

    const dispatch = host.tasks.assign(task.id, agent.id);
    expect(dispatch).toMatchObject({ placed: true, agentId: agent.id, reason: "placed" });
    expect(host.tasks.get(task.id)?.agentId).toBe(agent.id);
    expect(host.tasks.list({ ownerId: "owner-1" }).map(t => t.id)).toEqual([task.id]);

    const sent = host.tasks.send(task.id, { text: "hello" });
    expect(sent.taskUpsert).toBeDefined();
    const unacked = host.storage.outbox.listUnacked(agent.id);
    expect(unacked.map(row => row.type)).toEqual(["task_upsert", "message"]);
    expect(unacked[0]?.taskId).toBe(task.id);
    host.stop();
  });

  it("config.setEntries on the global scope bumps config.version()", () => {
    const host = makeHost();
    expect(host.config.version()).toBe(0);
    const artifact = host.config.putArtifact({ name: "CLAUDE.md", kind: "file", bytes: new TextEncoder().encode("# hi") });
    const entries = host.config.setEntries("global", [{ name: artifact.name, sha256: artifact.sha256, target: "CLAUDE.md", kind: "file" }]);
    expect(entries).toHaveLength(1);
    expect(host.config.version()).toBe(1);
    expect(host.config.entries("global").map(e => e.sha256)).toEqual([artifact.sha256]);
    expect(host.config.resolveFor(["claude"]).map(e => e.target)).toEqual(["CLAUDE.md"]);
    host.stop();
  });

  it("transcript.get on an empty thread returns []", () => {
    const host = makeHost();
    expect(host.transcript.get("thread-none")).toEqual([]);
    expect(host.transcript.stats("thread-none")).toEqual({ messageCount: 0, lastSeq: null });
    expect(host.transcript.run("thread-none")).toBeUndefined();
    host.stop();
  });

  it("transcript.get exposes the sub-agent attribution ids as written", () => {
    const host = makeHost();
    const write = (row: Parameters<typeof host.storage.transcript.insertIfAbsent>[0]) =>
      host.storage.transcript.insertIfAbsent(row);
    write({ taskId: "t1", threadId: "th1", seq: 1, kind: "tool_use", tool: "Task", toolUseId: "toolu_parent" });
    write({ taskId: "t1", threadId: "th1", seq: 2, kind: "assistant_text", content: "inner", parentToolUseId: "toolu_parent" });
    write({ taskId: "t1", threadId: "th1", seq: 3, kind: "assistant_text", content: "main" });

    expect(host.transcript.get("th1").map(r => [r.seq, r.toolUseId, r.parentToolUseId])).toEqual([
      [1, "toolu_parent", null],
      [2, null, "toolu_parent"],
      [3, null, null],
    ]);
    host.stop();
  });
});
