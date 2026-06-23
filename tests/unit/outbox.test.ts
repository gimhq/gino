import type { AppDatabase } from "@/db";
import type { FrameSender } from "@/protocol/outbox";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent } from "@/agents/registry";
import { createDb } from "@/db";
import { ackMessage, ackOutbox, enqueue, replayUnacked } from "@/protocol/outbox";

// M2 outbox — at-least-once host→agent: persist before send; replay unacked on
// reconnect; stop replaying on ack{id} / msg_ack{msgId}.

interface Sent { agentId: string; frame: Record<string, unknown> }

/** A FrameSender that records every push and can be toggled online/offline. */
function fakeSender(online: boolean): FrameSender & { sent: Sent[]; online: boolean } {
  const s = {
    sent: [] as Sent[],
    online,
    isOnline: () => s.online,
    send: (agentId: string, frame: Record<string, unknown>) => {
      if (!s.online) {
        return false;
      }
      s.sent.push({ agentId, frame });
      return true;
    },
  };
  return s;
}

let db: AppDatabase;
let agentId: string;

beforeEach(() => {
  db = createDb(":memory:");
  agentId = enrollAgent(db, { source: "manual", capabilities: { engines: ["claude"] } }).agent.id;
});

afterEach(() => {
  db.close();
});

describe("M2 outbox — enqueue", () => {
  it("persists then sends when online (outboxId on the wire)", () => {
    const sender = fakeSender(true);
    const row = enqueue(db, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    expect(row.id).toBeGreaterThan(0);
    expect(row.ackedAt).toBeNull();
    expect(sender.sent).toHaveLength(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "task_upsert", outboxId: row.id, taskId: "t-1" });
  });

  it("persists but does NOT send when offline; replays later", () => {
    const sender = fakeSender(false);
    const row = enqueue(db, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });
    expect(sender.sent).toHaveLength(0);

    sender.online = true;
    const replayed = replayUnacked(db, sender, agentId);
    expect(replayed).toBe(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "task_upsert", outboxId: row.id });
  });

  it("a message frame gets a stable msgId (= row id)", () => {
    const sender = fakeSender(true);
    const row = enqueue(db, sender, { agentId, type: "message", taskId: "t-1", threadId: "c-1", data: { text: "hi" } });
    expect(row.msgId).toBe(row.id);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "message", msgId: row.id });
    expect(sender.sent[0]!.frame.outboxId).toBeUndefined();
  });
});

describe("M2 outbox — ack stops replay", () => {
  it("ack{id} marks the row acked → no longer replayed", () => {
    const sender = fakeSender(true);
    const row = enqueue(db, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    ackOutbox(db, row.id);
    sender.sent.length = 0;
    expect(replayUnacked(db, sender, agentId)).toBe(0);
  });

  it("msg_ack{msgId} marks a message delivered → no longer replayed", () => {
    const sender = fakeSender(true);
    const row = enqueue(db, sender, { agentId, type: "message", taskId: "t-1", data: { text: "hi" } });

    ackMessage(db, row.msgId!);
    sender.sent.length = 0;
    expect(replayUnacked(db, sender, agentId)).toBe(0);
  });

  it("replays multiple unacked frames in id order", () => {
    const sender = fakeSender(false);
    enqueue(db, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "a" } });
    enqueue(db, sender, { agentId, type: "message", taskId: "t-1", data: { text: "b" } });
    enqueue(db, sender, { agentId, type: "interrupt" });

    sender.online = true;
    expect(replayUnacked(db, sender, agentId)).toBe(3);
    expect(sender.sent.map(s => s.frame.type)).toEqual(["task_upsert", "message", "interrupt"]);
  });
});
