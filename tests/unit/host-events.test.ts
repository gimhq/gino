import type { AppDatabase } from "@/db";
import type { HostEventMap } from "@/host";
import type { Storage } from "@/host/ports/storage";
import type { Logger } from "@/shared/lib/logger";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent, grantOwner } from "@/agents/registry";
import { createDb } from "@/db";
import { createHostEvents } from "@/host";
import { ingestHttpFrame } from "@/protocol/inbound";
import { sqliteStorage } from "@/store/sqlite";
import { createTask, placeTask, setPlacement } from "@/tasks";

// HostEvents — the typed post-write notification seam (host-sdk-design §3.3):
//   • `on` returns an unsubscribe; `off` removes a listener.
//   • a throwing listener is warn-logged and neither stops later listeners nor
//     surfaces to the emitter (the write path never sees it).
//   • the M2 inbound path emits `transcript.message` / `thread.result` /
//     `outbox.acked` AFTER the corresponding durable write.
//   • dispatch emits `task.placed` once per fresh pin, never on the replay.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** A Logger fake that records every warn payload. */
function spyLogger(): { logger: Logger; warns: unknown[][] } {
  const warns: unknown[][] = [];
  const noop = (): void => {};
  const logger = {
    debug: noop,
    info: noop,
    warn: (...args: unknown[]) => warns.push(args),
    error: noop,
    fatal: noop,
    flush: noop,
    reopen: noop,
  } as unknown as Logger;
  return { logger, warns };
}

/** Enroll a fresh agent, create a task, and place it on that agent. */
function placedAgentTask(): { agentId: string; taskId: string } {
  const agentId = enrollAgent(storage, { capabilities: { engines: ["claude"] } }).agent.id;
  const taskId = createTask(storage, { ownerId: "owner", engine: "claude", model: "sonnet", status: "active" }).id;
  setPlacement(storage, taskId, agentId);
  return { agentId, taskId };
}

describe("createHostEvents — subscription", () => {
  it("on() returns an unsubscribe; off() removes a listener", () => {
    const events = createHostEvents();
    const seen: string[] = [];
    const a = (p: HostEventMap["agent.online"]): void => {
      seen.push(`a:${p.agentId}`);
    };
    const b = (p: HostEventMap["agent.online"]): void => {
      seen.push(`b:${p.agentId}`);
    };
    const offA = events.on("agent.online", a);
    events.on("agent.online", b);

    events.emit("agent.online", { agentId: "x" });
    expect(seen).toEqual(["a:x", "b:x"]); // subscription order

    offA();
    events.emit("agent.online", { agentId: "y" });
    expect(seen).toEqual(["a:x", "b:x", "b:y"]);

    events.off("agent.online", b);
    events.off("agent.online", b); // idempotent
    events.emit("agent.online", { agentId: "z" });
    expect(seen).toEqual(["a:x", "b:x", "b:y"]);
  });

  it("a throwing listener is warn-logged and neither blocks later listeners nor throws to the emitter", () => {
    const { logger, warns } = spyLogger();
    const events = createHostEvents(logger);
    const seen: string[] = [];
    events.on("agent.enrolled", () => {
      throw new Error("boom");
    });
    events.on("agent.enrolled", ({ agentId }) => {
      seen.push(agentId);
    });

    expect(() => events.emit("agent.enrolled", { agentId: "a1" })).not.toThrow();
    expect(seen).toEqual(["a1"]);
    expect(warns).toHaveLength(1);
    expect((warns[0]![0] as { event: string }).event).toBe("agent.enrolled");
  });
});

describe("createHostEvents — emission sites", () => {
  it("the inbound path emits transcript.message / thread.result / outbox.acked after each write", () => {
    const { agentId, taskId } = placedAgentTask();
    const events = createHostEvents();
    const messages: HostEventMap["transcript.message"][] = [];
    const results: HostEventMap["thread.result"][] = [];
    const acks: HostEventMap["outbox.acked"][] = [];
    events.on("transcript.message", p => messages.push(p));
    events.on("thread.result", p => results.push(p));
    events.on("outbox.acked", p => acks.push(p));

    ingestHttpFrame({ storage, agentId, events }, JSON.stringify({
      type: "thread_message",
      outboxId: 1,
      taskId,
      threadId: "thr-1",
      data: { seq: 1, kind: "assistant_text", content: "hello" },
    }));
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({ agentId, taskId, threadId: "thr-1", seq: 1, data: { seq: 1, kind: "assistant_text", content: "hello" } });

    ingestHttpFrame({ storage, agentId, events }, JSON.stringify({
      type: "thread_result",
      taskId,
      threadId: "thr-1",
      data: { status: "succeeded", duration_ms: 5, final_seq: 1 },
    }));
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ agentId, taskId, threadId: "thr-1", source: "chat", data: { status: "succeeded", final_seq: 1 } });

    ingestHttpFrame({ storage, agentId, events }, JSON.stringify({ type: "ack", id: 1 }));
    expect(acks).toEqual([{ agentId, id: 1, type: "ack" }]);
  });

  it("placeTask emits task.placed once — not on the already-placed replay", () => {
    const agentId = enrollAgent(storage, { capabilities: { engines: ["claude"] } }).agent.id;
    grantOwner(storage, agentId, "owner"); // a dedicated agent must be granted to the task's owner (OD13)
    const taskId = createTask(storage, { ownerId: "owner", engine: "claude", model: "sonnet" }).id;
    const events = createHostEvents();
    const placed: HostEventMap["task.placed"][] = [];
    events.on("task.placed", p => placed.push(p));

    expect(placeTask(storage, taskId, events).reason).toBe("placed");
    expect(placeTask(storage, taskId, events).reason).toBe("already-placed");
    expect(placed).toEqual([{ taskId, agentId }]);
  });
});
