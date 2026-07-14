import type { DnsResolver, FetchLike } from "@/channels";
import type { AppDatabase } from "@/db";
import type { Channel } from "@/db/schema";
import type { ThreadMessageData } from "@/protocol/frames";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import {
  addWebhook,
  assertSafeWebhookUrl,
  bindConversation,
  DEFAULT_POLL_LIMIT,
  getChannel,
  MAX_POLL_LIMIT,
  pollChannel,
  pushChannelWebhooks,
  readCursor,
  registerChannel,
  tailTask,
} from "@/channels";
import { createDb } from "@/db";
import { messages } from "@/db/schema";
import { writeThreadMessage } from "@/msgstore";
import { createTask } from "@/tasks";

// M6 egress (OD10) — tail the M5 store by a per-(channel, task) cursor: deliver
// all kinds by default at-least-once (the cursor advances only on ack, never on
// read — HOST-M5M6-01), re-read idempotently from a stale cursor, honour an
// optional kind filter, and push to webhooks cursor-driven.

const OWNER = "owner-egress";
const THREAD = "thread-egress-1";

let db: AppDatabase;

type Kind = ThreadMessageData["kind"];

/** Seed N transcript events on a task's thread (seq 1..N) from a kind list. */
function seed(taskId: string, kinds: Kind[]): void {
  kinds.forEach((kind, i) => {
    writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq: i + 1, kind, content: `${kind}-${i + 1}` } });
  });
}

function makeTask(): string {
  return createTask(db, { ownerId: OWNER, engine: "claude", model: "sonnet" }).id;
}

function channel(filter?: Record<string, unknown>, type = "http"): Channel {
  return registerChannel(db, { name: `c-${type}`, type, ownerId: OWNER, ...(filter ? { filter } : {}) });
}

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

describe("M6 egress — cursor tail", () => {
  it("delivers all kinds in seq order, advances the cursor on ack, and stops re-delivering", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text", "tool_use", "tool_result"]);

    // First poll → every kind, in seq order.
    const first = pollChannel(db, ch, { taskId });
    expect(first).toHaveLength(1);
    expect(first[0]!.messages.map(m => m.kind)).toEqual(["user_input", "assistant_text", "tool_use", "tool_result"]);
    expect(first[0]!.messages.map(m => m.seq)).toEqual([1, 2, 3, 4]);

    // Acking the delivered cursor advances the stored cursor to it.
    expect(pollChannel(db, ch, { taskId, ack: first[0]!.cursor })).toHaveLength(0);
    const cursor = readCursor(db, ch.id, taskId);
    expect(cursor).toBe(first[0]!.cursor);
    expect(cursor).toBeGreaterThan(0);

    // A new message → only the new one is delivered.
    writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq: 5, kind: "assistant_text", content: "more" } });
    const third = pollChannel(db, ch, { taskId });
    expect(third[0]!.messages.map(m => m.seq)).toEqual([5]);

    // A stale (lower) ack is a no-op — the cursor never moves backwards.
    pollChannel(db, ch, { taskId, ack: 1 });
    expect(readCursor(db, ch.id, taskId)).toBe(cursor);
  });

  it("re-reads idempotently from a stale (explicit) cursor without confirming receipt", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text"]);

    // Drain: deliver, then ack the batch.
    const first = pollChannel(db, ch, { taskId });
    expect(first[0]!.messages).toHaveLength(2);
    expect(pollChannel(db, ch, { taskId, ack: first[0]!.cursor })).toHaveLength(0);

    // A re-read from position 0 re-delivers everything (restarts/failures re-read
    // without corrupting the record) …
    const replay = tailTask(db, ch, taskId, { after: 0 });
    expect(replay.messages.map(m => m.seq)).toEqual([1, 2]);

    // … and does not move the stored cursor (`after` reads, never confirms).
    const before = readCursor(db, ch.id, taskId);
    pollChannel(db, ch, { taskId, after: 0 });
    expect(readCursor(db, ch.id, taskId)).toBe(before);
  });

  it("honours an opt-in kind filter and acks past filtered rows (OD10)", () => {
    const taskId = makeTask();
    const ch = channel({ kinds: ["assistant_text"] });
    seed(taskId, ["user_input", "assistant_text", "tool_use", "assistant_text"]);

    const delivered = pollChannel(db, ch, { taskId });
    expect(delivered[0]!.messages.map(m => m.kind)).toEqual(["assistant_text", "assistant_text"]);
    // The delivered cursor covers the filtered-out rows too → after the ack they
    // are never re-scanned.
    expect(pollChannel(db, ch, { taskId, ack: delivered[0]!.cursor })).toHaveLength(0);
  });

  it("limit caps the batch and the acked cursor covers only included rows", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text", "thinking", "tool_use"]);

    const batch1 = pollChannel(db, ch, { taskId, limit: 2 });
    expect(batch1[0]!.messages.map(m => m.seq)).toEqual([1, 2]);
    const batch2 = pollChannel(db, ch, { taskId, limit: 2, ack: batch1[0]!.cursor });
    expect(batch2[0]!.messages.map(m => m.seq)).toEqual([3, 4]);
  });
});

describe("M6 egress — at-least-once poll (HOST-M5M6-01)", () => {
  it("a read never advances the cursor: an unacked batch is re-delivered (crash between read and receive loses nothing)", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text"]);

    // The client polls but the response is never received (disconnect/crash
    // mid-flush): the stored cursor must be unchanged.
    const lost = pollChannel(db, ch, { taskId });
    expect(lost[0]!.messages).toHaveLength(2);
    expect(readCursor(db, ch.id, taskId)).toBe(0);

    // The next poll re-delivers the exact same batch.
    const retry = pollChannel(db, ch, { taskId });
    expect(retry[0]!.messages.map(m => m.seq)).toEqual([1, 2]);
    expect(retry[0]!.cursor).toBe(lost[0]!.cursor);

    // Only the explicit ack confirms receipt and drains the channel.
    expect(pollChannel(db, ch, { taskId, ack: retry[0]!.cursor })).toHaveLength(0);
    expect(readCursor(db, ch.id, taskId)).toBe(retry[0]!.cursor);
  });
});

describe("M6 egress — cursor stability (HOST-M5M6-03)", () => {
  it("the cursor survives deletes + VACUUM (keys on messages.id, not a reusable rowid)", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text", "tool_use", "tool_result", "assistant_text"]);

    // Deliver + ack everything (cursor = high-water messages.id = 5).
    const first = pollChannel(db, ch, { taskId });
    pollChannel(db, ch, { taskId, ack: first[0]!.cursor });
    const cursor = readCursor(db, ch.id, taskId);

    // Simulate retention + compaction: delete early rows, then VACUUM. With an
    // implicit rowid this renumbers the survivors (4,5 → 1,2) and re-points the
    // stored cursor at the wrong rows; an INTEGER PRIMARY KEY AUTOINCREMENT is
    // stable and never reused.
    db.run(sql`DELETE FROM messages WHERE seq <= 3`);
    db.run(sql`VACUUM`);
    const survivors = db.select({ id: messages.id }).from(messages).all();
    expect(survivors.map(r => r.id)).toEqual([4, 5]); // ids preserved across VACUUM

    // A new message gets a NEW id past the cursor — it is delivered, exactly once
    // acked, with no replay of the survivors and no silent skip.
    writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq: 6, kind: "assistant_text", content: "post-vacuum" } });
    const next = pollChannel(db, ch, { taskId });
    expect(next).toHaveLength(1);
    expect(next[0]!.messages.map(m => m.seq)).toEqual([6]);
    expect(next[0]!.cursor).toBeGreaterThan(cursor);
  });
});

describe("M6 egress — seq-order delivery (HOST-M5M6-10)", () => {
  it("delivers a batch in (thread, seq) order even when frames arrived out of order", () => {
    const taskId = makeTask();
    const ch = channel();
    // Reconnect replay: seq 3 and 1 are stored AFTER seq 5 (higher arrival ids).
    for (const seq of [2, 5, 3, 1, 4]) {
      writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq, kind: "assistant_text", content: `m${seq}` } });
    }
    const delivered = pollChannel(db, ch, { taskId });
    expect(delivered[0]!.messages.map(m => m.seq)).toEqual([1, 2, 3, 4, 5]);
  });

  it("orders across threads by (thread, seq), not arrival", () => {
    const taskId = makeTask();
    const ch = channel();
    writeThreadMessage(db, { taskId, threadId: "thr-b", data: { seq: 2, kind: "assistant_text", content: "b2" } });
    writeThreadMessage(db, { taskId, threadId: "thr-a", data: { seq: 1, kind: "assistant_text", content: "a1" } });
    writeThreadMessage(db, { taskId, threadId: "thr-b", data: { seq: 1, kind: "assistant_text", content: "b1" } });
    const delivered = pollChannel(db, ch, { taskId });
    expect(delivered[0]!.messages.map(m => `${m.threadId}:${m.seq}`)).toEqual(["thr-a:1", "thr-b:1", "thr-b:2"]);
  });

  it("a capped window is contiguous by store position, so an ack never skips undelivered rows", () => {
    const taskId = makeTask();
    const ch = channel();
    // seq 5 arrives first (store id 1), then seq 1..3 (store ids 2..4).
    for (const seq of [5, 1, 2, 3]) {
      writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq, kind: "assistant_text", content: `m${seq}` } });
    }
    // limit 2 → the window is the two OLDEST store positions (seq 5, then 1),
    // delivered sorted by seq; acking it must not skip seq 2..3.
    const batch1 = pollChannel(db, ch, { taskId, limit: 2 });
    expect(batch1[0]!.messages.map(m => m.seq)).toEqual([1, 5]);
    const batch2 = pollChannel(db, ch, { taskId, limit: 2, ack: batch1[0]!.cursor });
    expect(batch2[0]!.messages.map(m => m.seq)).toEqual([2, 3]);
  });
});

describe("M6 egress — bounded reads (HOST-M5M6-08)", () => {
  it("applies DEFAULT_POLL_LIMIT when the caller omits limit and MAX_POLL_LIMIT as the hard cap", () => {
    const taskId = makeTask();
    const ch = channel();
    const total = MAX_POLL_LIMIT + 5;
    for (let seq = 1; seq <= total; seq++) {
      writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq, kind: "assistant_text", content: `m${seq}` } });
    }

    // No limit → a first-time poll (cursor 0) is still bounded, not the whole backlog.
    const first = pollChannel(db, ch, { taskId });
    expect(first[0]!.messages).toHaveLength(DEFAULT_POLL_LIMIT);

    // An oversized limit is clamped to the hard cap.
    const capped = tailTask(db, ch, taskId, { limit: MAX_POLL_LIMIT * 10 });
    expect(capped.scanned).toBe(MAX_POLL_LIMIT);

    // The remainder is deliverable after acking the first window (bounded ≠ lost).
    const rest = pollChannel(db, ch, { taskId, ack: first[0]!.cursor });
    expect(rest[0]!.messages).toHaveLength(DEFAULT_POLL_LIMIT);
  });
});

describe("M6 egress — webhook push (cursor-driven)", () => {
  it("advances the cursor after a 2xx and re-reads after a failure", async () => {
    const taskId = makeTask();
    const base = channel(undefined, "webhook");
    bindConversation(db, base.id, "conv-wh", taskId); // egress fans out over bound tasks
    addWebhook(db, base.id, { url: "https://hooks.example.com/in" });
    const ch = getChannel(db, base.id)!; // re-read so it carries the webhook config
    seed(taskId, ["user_input", "assistant_text"]);

    // Inject a resolver so delivery's SSRF DNS re-check never touches the network
    // (hooks.example.com resolves to a public IP here).
    const publicResolver: DnsResolver = async () => [{ address: "93.184.216.34", family: 4 }];

    // A failing receiver (500) → cursor NOT advanced (nothing lost).
    const fail: FetchLike = async () => ({ status: 500 });
    const failed = await pushChannelWebhooks(db, ch, { fetchImpl: fail, resolver: publicResolver });
    expect(failed[0]!.ok).toBe(false);
    expect(readCursor(db, ch.id, taskId)).toBe(0);

    // A succeeding receiver (200) → re-reads the same batch, then advances.
    const okFetch: FetchLike = async () => ({ status: 200 });
    const sent = await pushChannelWebhooks(db, ch, { fetchImpl: okFetch, resolver: publicResolver });
    expect(sent[0]!.ok).toBe(true);
    expect(sent[0]!.delivered).toBe(2);
    expect(readCursor(db, ch.id, taskId)).toBeGreaterThan(0);

    // Drained — a second push delivers nothing.
    const again = await pushChannelWebhooks(db, ch, { fetchImpl: okFetch, resolver: publicResolver });
    expect(again).toHaveLength(0);
  });

  it("SSRF-guards outbound webhook URLs (private/loopback/scheme)", () => {
    expect(() => assertSafeWebhookUrl("http://127.0.0.1/x")).toThrow();
    expect(() => assertSafeWebhookUrl("http://localhost/x")).toThrow();
    expect(() => assertSafeWebhookUrl("http://10.0.0.5/x")).toThrow();
    expect(() => assertSafeWebhookUrl("http://192.168.1.10/x")).toThrow();
    expect(() => assertSafeWebhookUrl("http://169.254.169.254/latest")).toThrow();
    expect(() => assertSafeWebhookUrl("ftp://example.com/x")).toThrow();
    // A public host passes.
    expect(assertSafeWebhookUrl("https://hooks.example.com/in").hostname).toBe("hooks.example.com");
  });

  it("a channel with no webhooks is a no-op push", async () => {
    const taskId = makeTask();
    const ch = channel(undefined, "webhook");
    bindConversation(db, ch.id, "conv-wh", taskId);
    seed(taskId, ["user_input"]);
    const results = await pushChannelWebhooks(db, ch, { fetchImpl: async () => ({ status: 200 }) });
    // No outbound endpoints → a true no-op: nothing pushed, the cursor untouched.
    expect(results).toHaveLength(0);
    expect(readCursor(db, ch.id, taskId)).toBe(0);
  });
});

describe("M6 egress — storage ⟂ distribution", () => {
  it("never writes the transcript when polling", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text"]);
    const before = db.select().from(messages).all().length;
    pollChannel(db, ch, { taskId });
    pollChannel(db, ch, { taskId, after: 0, ack: 1 });
    const after = db.select().from(messages).all().length;
    expect(after).toBe(before); // egress only reads messages + writes channel_cursors
  });
});
