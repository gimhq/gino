import type { Outbox } from "@gino/host-store-sqlite/schema";
import { describe, expect, it } from "bun:test";
import { decodeFrame, encodeOutbox } from "../../src/protocol/envelope";

// M2 codec — decode reads ONLY the routing envelope (data stays opaque); encode
// rebuilds a host→agent frame from an outbox row. No business parsing here.

const ID = "01jab3x7k2m4n5p6q7r8s9t0vw";

function outboxRow(over: Partial<Outbox>): Outbox {
  return {
    id: ID,
    agentId: "agent-1",
    type: "task_upsert",
    taskId: null,
    threadId: null,
    cronId: null,
    dataJson: {},
    createdAt: 0,
    ackedAt: null,
    ...over,
  };
}

describe("M2 envelope — decode", () => {
  it("extracts the routing ids and passes data through opaque", () => {
    const decoded = decodeFrame(JSON.stringify({
      type: "thread_message",
      outboxId: ID,
      taskId: "t-1",
      threadId: "c-9@1718",
      data: { seq: 3, kind: "assistant_text", content: "hi" },
    }));
    expect(decoded).not.toBeNull();
    expect(decoded!.type).toBe("thread_message");
    expect(decoded!.outboxId).toBe(ID);
    expect(decoded!.taskId).toBe("t-1");
    expect(decoded!.threadId).toBe("c-9@1718");
    expect(decoded!.data).toEqual({ seq: 3, kind: "assistant_text", content: "hi" });
  });

  it("returns null for non-JSON", () => {
    expect(decodeFrame("not json")).toBeNull();
  });

  it("returns null when the envelope has no type", () => {
    expect(decodeFrame(JSON.stringify({ taskId: "t-1" }))).toBeNull();
  });

  it("ignores unknown envelope fields (additive-compatible)", () => {
    const decoded = decodeFrame(JSON.stringify({ type: "ack", id: ID, somethingNew: true }));
    expect(decoded).not.toBeNull();
    expect(decoded!.type).toBe("ack");
    expect(decoded!.id).toBe(ID);
  });

  it("decodes a binary (ArrayBuffer) payload", () => {
    const buf = new TextEncoder().encode(JSON.stringify({ type: "heartbeat", data: { activeThreads: 2 } }));
    const decoded = decodeFrame(buf.buffer);
    expect(decoded!.type).toBe("heartbeat");
    expect(decoded!.data).toEqual({ activeThreads: 2 });
  });
});

describe("M2 envelope — encode", () => {
  it("a host→agent frame carries msgId = row id and no outboxId", () => {
    const frame = encodeOutbox(outboxRow({ type: "task_upsert", taskId: "t-1", dataJson: { name: "job" } }));
    expect(frame).toEqual({ type: "task_upsert", msgId: ID, taskId: "t-1", data: { name: "job" } });
  });

  it("a message frame carries msgId = row id", () => {
    const frame = encodeOutbox(outboxRow({ type: "message", taskId: "t-1", threadId: "c-1", dataJson: { text: "hi" } }));
    expect(frame).toEqual({ type: "message", msgId: ID, taskId: "t-1", threadId: "c-1", data: { text: "hi" } });
  });

  it("a delete frame (no data) is envelope-only", () => {
    const frame = encodeOutbox(outboxRow({ type: "task_delete", taskId: "t-1", dataJson: {} }));
    expect(frame).toEqual({ type: "task_delete", msgId: ID, taskId: "t-1" });
    expect("data" in frame).toBe(false);
  });
});
