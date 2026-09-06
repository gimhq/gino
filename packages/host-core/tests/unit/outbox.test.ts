import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Storage } from "../../src/host/ports/storage";
import type { AgentTransport } from "../../src/host/ports/transport";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { outbox } from "@gino/host-store-sqlite/schema";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { enrollAgent } from "../../src/agents/registry";
import { ackMessage, ackOutbox, CLOSE_FRAME_TTL_MS, enqueue, gcOutbox, OUTBOX_RETENTION_MS, purgeAgentOutbox, replayUnacked } from "../../src/protocol/outbox";

// M2 outbox — at-least-once host→agent: persist before send; replay unacked on
// reconnect; stop replaying on ack{id} / msg_ack{msgId}.

interface Sent { agentId: string; frame: Record<string, unknown> }

/** An AgentTransport that records every push and can be toggled online/offline. */
function fakeSender(online: boolean): AgentTransport & { sent: Sent[]; online: boolean } {
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
    closeAgent: () => false,
  };
  return s;
}

let db: AppDatabase;
let storage: Storage;
let agentId: string;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
  agentId = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } }).agent.id;
});

afterEach(() => {
  db.close();
});

describe("M2 outbox — enqueue", () => {
  it("persists then sends when online (outboxId on the wire)", () => {
    const sender = fakeSender(true);
    const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    expect(row.id).toBeGreaterThan(0);
    expect(row.ackedAt).toBeNull();
    expect(sender.sent).toHaveLength(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "task_upsert", outboxId: row.id, taskId: "t-1" });
  });

  it("persists but does NOT send when offline; replays later", () => {
    const sender = fakeSender(false);
    const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });
    expect(sender.sent).toHaveLength(0);

    sender.online = true;
    const replayed = replayUnacked(storage, sender, agentId);
    expect(replayed).toBe(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "task_upsert", outboxId: row.id });
  });

  it("a message frame gets a stable msgId (= row id)", () => {
    const sender = fakeSender(true);
    const row = enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", threadId: "c-1", data: { text: "hi" } });
    expect(row.msgId).toBe(row.id);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "message", msgId: row.id });
    expect(sender.sent[0]!.frame.outboxId).toBeUndefined();
  });
});

describe("M2 outbox — ack stops replay", () => {
  it("ack{id} marks the row acked → no longer replayed", () => {
    const sender = fakeSender(true);
    const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    ackOutbox(storage, agentId, row.id);
    sender.sent.length = 0;
    expect(replayUnacked(storage, sender, agentId)).toBe(0);
  });

  it("msg_ack{msgId} marks a message delivered → no longer replayed", () => {
    const sender = fakeSender(true);
    const row = enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "hi" } });

    ackMessage(storage, agentId, row.msgId!);
    sender.sent.length = 0;
    expect(replayUnacked(storage, sender, agentId)).toBe(0);
  });

  it("another agent's ack{id} does NOT ack this agent's frame", () => {
    const sender = fakeSender(false);
    const otherId = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } }).agent.id;
    const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    ackOutbox(storage, otherId, row.id);
    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
  });

  it("another agent's msg_ack{msgId} does NOT mark this agent's message delivered", () => {
    const sender = fakeSender(false);
    const otherId = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } }).agent.id;
    const row = enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "hi" } });

    ackMessage(storage, otherId, row.msgId!);
    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
  });

  it("a plain ack{id} does NOT mark a message delivered — only msg_ack can", () => {
    const sender = fakeSender(false);
    const row = enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "hi" } });

    ackOutbox(storage, agentId, row.id);
    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
  });

  it("replays multiple unacked frames in id order", () => {
    const sender = fakeSender(false);
    enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "a" } });
    enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "b" } });
    enqueue(storage, sender, { agentId, type: "interrupt" });

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(3);
    expect(sender.sent.map(s => s.frame.type)).toEqual(["task_upsert", "message", "interrupt"]);
  });
});

// HOST-M1M2-07 / HOST-M4-07: a `message` row's delivery id is its row id,
// assigned by the single INSERT — there is no insert-then-update window in
// which a durable row exists without an ackable id.
describe("M2 outbox — atomic message delivery id", () => {
  it("the DURABLE message row is born ackable: replay + msg_ack + no re-replay", () => {
    const sender = fakeSender(false);
    const row = enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "hi" } });

    // Replay re-reads the persisted row (not enqueue's in-memory return), so
    // the frame's msgId comes straight from what the single INSERT committed.
    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
    expect(sender.sent[0]!.frame.msgId).toBe(row.id);

    ackMessage(storage, agentId, sender.sent[0]!.frame.msgId as number);
    sender.sent.length = 0;
    expect(replayUnacked(storage, sender, agentId)).toBe(0);
  });

  it("a bare-inserted message row (bypassing enqueue) is still deliverable and ackable", () => {
    // The delivery id is derived from the row id at encode time — there is no
    // msg_id column to back-fill — so ANY durable message row replays WITH a
    // msgId and is ackable, not stuck as an id-less frame.
    const raw = db
      .insert(outbox)
      .values({ agentId, type: "message", taskId: "t-1", dataJson: { text: "orphan" } })
      .returning()
      .get();

    const sender = fakeSender(true);
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "message", msgId: raw.id });

    ackMessage(storage, agentId, raw.id);
    sender.sent.length = 0;
    expect(replayUnacked(storage, sender, agentId)).toBe(0);
  });

  it("msg_ack{id} of a NON-message frame DOES ack it (the runner acks every durable frame via msg_ack)", () => {
    const sender = fakeSender(false);
    const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    ackMessage(storage, agentId, row.id);
    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(0);
  });

  it("msg_ack{id} scoped to another agent's row is a no-op", () => {
    const sender = fakeSender(false);
    const row = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: { name: "job" } });

    ackMessage(storage, "someone-else", row.id);
    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
  });
});

describe("M2 outbox — retention GC", () => {
  /** Backdate a row's acked_at (the GC cutoff is measured against it). */
  function ackAt(id: number, ackedAt: number): void {
    db.update(outbox).set({ ackedAt }).where(eq(outbox.id, id)).run();
  }

  it("deletes acked rows past the retention window; keeps recent acked rows", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    const oldAcked = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: {} });
    const recentAcked = enqueue(storage, sender, { agentId, type: "interrupt", taskId: "t-1" });
    ackAt(oldAcked.id, now - OUTBOX_RETENTION_MS - 1);
    ackAt(recentAcked.id, now - 1000);

    expect(gcOutbox(storage, { now: () => now })).toBe(1);

    const remaining = db.select({ id: outbox.id }).from(outbox).all().map(r => r.id);
    expect(remaining).toEqual([recentAcked.id]);
  });

  it("NEVER deletes unacked rows, however old", () => {
    const sender = fakeSender(false);
    const unacked = enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "hi" } });

    // A year past any retention window: the row is still owed to the agent.
    expect(gcOutbox(storage, { now: () => Date.now() + 365 * 24 * 60 * 60 * 1000 })).toBe(0);

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "message", msgId: unacked.id });
  });

  it("honours a custom retention window", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    const row = enqueue(storage, sender, { agentId, type: "interrupt" });
    ackAt(row.id, now - 5000);

    expect(gcOutbox(storage, { retentionMs: 10_000, now: () => now })).toBe(0);
    expect(gcOutbox(storage, { retentionMs: 1000, now: () => now })).toBe(1);
  });

  it("replay after GC still returns the surviving unacked frames in id order", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    const acked = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: {} });
    enqueue(storage, sender, { agentId, type: "message", taskId: "t-1", data: { text: "b" } });
    enqueue(storage, sender, { agentId, type: "interrupt" });
    ackAt(acked.id, now - OUTBOX_RETENTION_MS - 1);

    expect(gcOutbox(storage, { now: () => now })).toBe(1);

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId)).toBe(2);
    expect(sender.sent.map(s => s.frame.type)).toEqual(["message", "interrupt"]);
  });
});

describe("M2 outbox — stale close TTL on replay (HOST-M3-13)", () => {
  /** Backdate a row's created_at (the close TTL is measured against it). */
  function createdAt(id: number, ts: number): void {
    db.update(outbox).set({ createdAt: ts }).where(eq(outbox.id, id)).run();
  }

  it("skips + retires a close frame past the TTL — a stale stop cannot kill a later start", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    const stale = enqueue(storage, sender, { agentId, type: "close", data: { reason: "stop" } });
    createdAt(stale.id, now - CLOSE_FRAME_TTL_MS - 1);

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId, { now: () => now })).toBe(0);
    expect(sender.sent).toHaveLength(0);
    // Retired (marked acked) so it never rescans on the next reconnect.
    expect(db.select().from(outbox).where(eq(outbox.id, stale.id)).get()?.ackedAt).not.toBeNull();
    expect(replayUnacked(storage, sender, agentId, { now: () => now })).toBe(0);
  });

  it("delivers a close frame still within the TTL", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    enqueue(storage, sender, { agentId, type: "close", data: { reason: "stop" } });

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId, { now: () => now })).toBe(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "close" });
  });

  it("non-close frames replay regardless of age (only close carries the NOW intent)", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    const old = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: {} });
    createdAt(old.id, now - CLOSE_FRAME_TTL_MS * 100);

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId, { now: () => now })).toBe(1);
    expect(sender.sent[0]!.frame).toMatchObject({ type: "task_upsert" });
  });

  it("honours a custom close TTL", () => {
    const sender = fakeSender(false);
    const now = Date.now();
    const row = enqueue(storage, sender, { agentId, type: "close", data: { reason: "restart" } });
    createdAt(row.id, now - 5000);

    sender.online = true;
    expect(replayUnacked(storage, sender, agentId, { closeTtlMs: 1000, now: () => now })).toBe(0);
  });
});

describe("M2 outbox — purge on remove (HOST-M3-12)", () => {
  it("deletes only the agent's UNACKED rows; acked rows stay for retention GC", () => {
    const sender = fakeSender(false);
    const unacked1 = enqueue(storage, sender, { agentId, type: "close", data: { reason: "remove" } });
    const unacked2 = enqueue(storage, sender, { agentId, type: "task_upsert", taskId: "t-1", data: {} });
    const acked = enqueue(storage, sender, { agentId, type: "interrupt" });
    ackOutbox(storage, agentId, acked.id);
    const other = enrollAgent(storage, { source: "manual", capabilities: { engines: ["claude"] } }).agent.id;
    const foreign = enqueue(storage, sender, { agentId: other, type: "close", data: { reason: "stop" } });

    expect(purgeAgentOutbox(storage, agentId)).toBe(2);

    const remaining = db.select({ id: outbox.id }).from(outbox).all().map(r => r.id);
    expect(remaining).not.toContain(unacked1.id);
    expect(remaining).not.toContain(unacked2.id);
    expect(remaining).toContain(acked.id);
    expect(remaining).toContain(foreign.id);
  });
});
