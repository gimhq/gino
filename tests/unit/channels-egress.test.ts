import type { DnsResolver, FetchLike } from "@/channels";
import type { AppDatabase } from "@/db";
import type { Channel } from "@/db/schema";
import type { ThreadMessageData } from "@/protocol/frames";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  addWebhook,
  assertSafeWebhookUrl,
  bindConversation,
  getChannel,
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
// all kinds by default, advance the cursor, re-read idempotently from a stale
// cursor, honour an optional kind filter, and push to webhooks cursor-driven.

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
  it("delivers all kinds in seq order, advances the cursor, and stops re-delivering", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text", "tool_use", "tool_result"]);

    // First poll → every kind, in seq order.
    const first = pollChannel(db, ch, { taskId });
    expect(first).toHaveLength(1);
    expect(first[0]!.messages.map(m => m.kind)).toEqual(["user_input", "assistant_text", "tool_use", "tool_result"]);
    expect(first[0]!.messages.map(m => m.seq)).toEqual([1, 2, 3, 4]);

    // Cursor advanced to the high-water position.
    const cursor = readCursor(db, ch.id, taskId);
    expect(cursor).toBe(first[0]!.cursor);
    expect(cursor).toBeGreaterThan(0);

    // Second poll → nothing new.
    expect(pollChannel(db, ch, { taskId })).toHaveLength(0);

    // A new message → only the new one is delivered.
    writeThreadMessage(db, { taskId, threadId: THREAD, data: { seq: 5, kind: "assistant_text", content: "more" } });
    const third = pollChannel(db, ch, { taskId });
    expect(third[0]!.messages.map(m => m.seq)).toEqual([5]);
  });

  it("re-reads idempotently from a stale (explicit) cursor", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text"]);

    // Drain (advances the stored cursor).
    expect(pollChannel(db, ch, { taskId })[0]!.messages).toHaveLength(2);
    expect(pollChannel(db, ch, { taskId })).toHaveLength(0);

    // A re-read from position 0 re-delivers everything (restarts/failures re-read
    // without corrupting the record).
    const replay = tailTask(db, ch, taskId, { after: 0 });
    expect(replay.messages.map(m => m.seq)).toEqual([1, 2]);

    // peek does not advance the stored cursor.
    const before = readCursor(db, ch.id, taskId);
    pollChannel(db, ch, { taskId, after: 0, peek: true });
    expect(readCursor(db, ch.id, taskId)).toBe(before);
  });

  it("honours an opt-in kind filter and still advances past filtered rows (OD10)", () => {
    const taskId = makeTask();
    const ch = channel({ kinds: ["assistant_text"] });
    seed(taskId, ["user_input", "assistant_text", "tool_use", "assistant_text"]);

    const delivered = pollChannel(db, ch, { taskId });
    expect(delivered[0]!.messages.map(m => m.kind)).toEqual(["assistant_text", "assistant_text"]);
    // The cursor advanced past the filtered-out rows too → no re-scan next poll.
    expect(pollChannel(db, ch, { taskId })).toHaveLength(0);
  });

  it("limit caps the batch and advances only to the last included row", () => {
    const taskId = makeTask();
    const ch = channel();
    seed(taskId, ["user_input", "assistant_text", "thinking", "tool_use"]);

    const batch1 = pollChannel(db, ch, { taskId, limit: 2 });
    expect(batch1[0]!.messages.map(m => m.seq)).toEqual([1, 2]);
    const batch2 = pollChannel(db, ch, { taskId, limit: 2 });
    expect(batch2[0]!.messages.map(m => m.seq)).toEqual([3, 4]);
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
    pollChannel(db, ch, { taskId, after: 0, peek: true });
    const after = db.select().from(messages).all().length;
    expect(after).toBe(before); // egress only reads messages + writes channel_cursors
  });
});
