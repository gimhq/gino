import type { Outbox } from "@/db/schema";
import { describe, expect, it } from "bun:test";
import { decodeFrame, encodeOutbox } from "@/protocol/envelope";

// M2 codec — decode reads ONLY the routing envelope (data stays opaque); encode
// rebuilds a host→agent frame from an outbox row. No business parsing here.

function outboxRow(over: Partial<Outbox>): Outbox {
  return {
    id: 1,
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
      outboxId: 43,
      taskId: "t-1",
      threadId: "c-9@1718",
      data: { seq: 3, kind: "assistant_text", content: "hi" },
    }));
    expect(decoded).not.toBeNull();
    expect(decoded!.type).toBe("thread_message");
    expect(decoded!.outboxId).toBe(43);
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
    const decoded = decodeFrame(JSON.stringify({ type: "ack", id: 7, somethingNew: true }));
    expect(decoded).not.toBeNull();
    expect(decoded!.type).toBe("ack");
    expect(decoded!.id).toBe(7);
  });

  it("decodes a binary (ArrayBuffer) payload", () => {
    const buf = new TextEncoder().encode(JSON.stringify({ type: "heartbeat", data: { activeThreads: 2 } }));
    const decoded = decodeFrame(buf.buffer);
    expect(decoded!.type).toBe("heartbeat");
    expect(decoded!.data).toEqual({ activeThreads: 2 });
  });
});

describe("M2 envelope — encode", () => {
  it("a generic host→agent frame carries outboxId (acked by ack{id})", () => {
    const frame = encodeOutbox(outboxRow({ id: 5, type: "task_upsert", taskId: "t-1", dataJson: { name: "job" } }));
    expect(frame).toEqual({ type: "task_upsert", outboxId: 5, taskId: "t-1", data: { name: "job" } });
    expect(frame.msgId).toBeUndefined();
  });

  // HOST-M1M2-07 / HOST-M4-07: the delivery id is DERIVED from the row id at
  // encode time (there is no stored msg_id column), so every durable message
  // row encodes to an ackable frame.
  it("a message frame carries msgId = row id (acked by msg_ack), not outboxId", () => {
    const frame = encodeOutbox(outboxRow({ id: 8, type: "message", taskId: "t-1", threadId: "c-1", dataJson: { text: "hi" } }));
    expect(frame).toEqual({ type: "message", msgId: 8, taskId: "t-1", threadId: "c-1", data: { text: "hi" } });
    expect(frame.outboxId).toBeUndefined();
  });

  it("a delete frame (no data) is envelope-only", () => {
    const frame = encodeOutbox(outboxRow({ id: 9, type: "task_delete", taskId: "t-1", dataJson: {} }));
    expect(frame).toEqual({ type: "task_delete", outboxId: 9, taskId: "t-1" });
    expect("data" in frame).toBe(false);
  });
});
