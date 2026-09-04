import type { FetchLike } from "@/channels";
import type { AppDatabase } from "@/db";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent } from "@/agents/registry";
import { addWebhook, bindConversation, readCursor, registerChannel, startWebhookPump } from "@/channels";
import { createDb } from "@/db";
import { createHostEvents } from "@/host";
import { ingestHttpFrame } from "@/protocol/inbound";
import { createTask, setPlacement } from "@/tasks";

// HOST-M5M6-04 regression — outbound webhook egress is WIRED. Pre-fix,
// `pushChannelWebhooks` had no production caller: webhook channels stored
// endpoints that were never pushed to. Now a `thread_message` landing through
// the M2 inbound path triggers the webhook pump, which pushes the new
// transcript rows to the channel's registered endpoints and advances the
// `(channel, task)` cursor only on a confirmed 2xx (ack-based semantics).

const PUBLIC_IP = "93.184.216.34";

let db: AppDatabase;

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

interface Setup {
  agentId: string;
  taskId: string;
  channelId: string;
}

/** Enroll an agent, place a task on it, and bind the task to a webhook channel with one endpoint. */
function setup(): Setup {
  const agentId = enrollAgent(db, { capabilities: { engines: ["claude"] } }).agent.id;
  const taskId = createTask(db, { ownerId: "owner-A", engine: "claude", model: "sonnet", status: "active" }).id;
  setPlacement(db, taskId, agentId);
  const channel = registerChannel(db, { name: "wh", type: "webhook", ownerId: "owner-A" });
  bindConversation(db, channel.id, "conv-1", taskId);
  addWebhook(db, channel.id, { url: "https://hooks.example.com/in", secret: "shh" });
  return { agentId, taskId, channelId: channel.id };
}

function threadMessageFrame(taskId: string, seq: number, content: string): string {
  return JSON.stringify({
    type: "thread_message",
    outboxId: seq,
    taskId,
    threadId: "thr-1",
    data: { seq, kind: "assistant_text", content },
  });
}

describe("M6 webhook egress wiring (HOST-M5M6-04)", () => {
  it("pushes a stored thread_message to the channel webhook and acks the cursor on 2xx", async () => {
    const { agentId, taskId, channelId } = setup();
    const received: Array<{ url: string; headers: Record<string, string> | undefined; body: string | undefined }> = [];
    const fetchImpl: FetchLike = async (url, init) => {
      received.push({ url, headers: init?.headers, body: init?.body });
      return { status: 200 };
    };
    const events = createHostEvents();
    const pump = startWebhookPump({
      db,
      events,
      fetchImpl,
      resolver: async () => [{ address: PUBLIC_IP, family: 4 }],
      intervalMs: 60_000, // the event path is under test — keep the tick out of the way
    });

    // The production inbound path: store the frame, emit `transcript.message` → the pump.
    const ack = ingestHttpFrame({ db, agentId, events }, threadMessageFrame(taskId, 1, "hello"));
    expect(ack).toEqual({ type: "ack", id: 1 });
    await pump.flush();

    expect(received).toHaveLength(1);
    const body = JSON.parse(received[0]!.body!) as { channel: string; taskId: string; conversationRef?: string; messages: Array<{ content: string }> };
    expect(body.channel).toBe(channelId);
    expect(body.taskId).toBe(taskId);
    expect(body.conversationRef).toBe("conv-1");
    expect(body.messages.map(m => m.content)).toEqual(["hello"]);
    expect(received[0]!.headers?.["x-webhook-secret"]).toBe("shh");
    expect(readCursor(db, channelId, taskId)).toBeGreaterThan(0); // 2xx confirmed → cursor advanced

    // Drained: another notify pushes nothing (no duplicate fan-out).
    pump.notifyTask(taskId);
    await pump.flush();
    expect(received).toHaveLength(1);

    pump.stop();
  });

  it("honors ack-based cursors end-to-end: a failed push re-delivers the SAME batch on the next trigger", async () => {
    const { agentId, taskId, channelId } = setup();
    let status = 500;
    const bodies: string[] = [];
    const fetchImpl: FetchLike = async (_url, init) => {
      bodies.push(init?.body ?? "");
      return { status };
    };
    const events = createHostEvents();
    const pump = startWebhookPump({
      db,
      events,
      fetchImpl,
      resolver: async () => [{ address: PUBLIC_IP, family: 4 }],
      intervalMs: 60_000,
    });

    // A failing receiver → the cursor must NOT move (nothing lost).
    ingestHttpFrame({ db, agentId, events }, threadMessageFrame(taskId, 1, "first"));
    await pump.flush();
    expect(bodies).toHaveLength(1);
    expect(readCursor(db, channelId, taskId)).toBe(0);

    // Recovery: the next trigger re-reads and re-delivers the SAME batch.
    status = 200;
    pump.notifyTask(taskId);
    await pump.flush();
    expect(bodies).toHaveLength(2);
    expect(JSON.parse(bodies[1]!).messages.map((m: { content: string }) => m.content)).toEqual(["first"]);
    expect(readCursor(db, channelId, taskId)).toBeGreaterThan(0);

    pump.stop();
  });

  it("a task with no webhook channel bound never triggers a push (quiet no-op)", async () => {
    const agentId = enrollAgent(db, { capabilities: { engines: ["claude"] } }).agent.id;
    const taskId = createTask(db, { ownerId: "owner-B", engine: "claude", model: "sonnet", status: "active" }).id;
    setPlacement(db, taskId, agentId);
    let called = 0;
    const fetchImpl: FetchLike = async () => {
      called++;
      return { status: 200 };
    };
    const events = createHostEvents();
    const pump = startWebhookPump({ db, events, fetchImpl, intervalMs: 60_000 });

    ingestHttpFrame({ db, agentId, events }, threadMessageFrame(taskId, 1, "unbound"));
    await pump.flush();
    expect(called).toBe(0);

    pump.stop();
  });
});
