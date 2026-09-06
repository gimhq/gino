import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { writeThreadMessage } from "@gino/host-core/domain";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { messages } from "@gino/host-store-sqlite/schema";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { websocket } from "hono/bun";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

// D-B4 — channels over a LIVE server: register a channel (owner) → ingress routes
// a new conversation to a new task (owner-stamped, dispatched) and continues it on
// repeat → seed the M5 store → egress tails it by cursor and re-reads idempotently.
// All endpoints sit behind the channel token.

const REGISTER_TOKEN = "reg-secret-ch";
const CHANNEL_TOKEN = "chan-secret-ch";
const OWNER = "owner-ch";

const dbPath = resolve(tmpdir(), `gino-ch-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
  // HOST-XC-02: explicit same-secret reuse so one bearer drives both surfaces in this test.
  GINO_HTTP_ADMIN_TOKEN: CHANNEL_TOKEN,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: ReturnType<typeof createDb>;
let storage: ReturnType<typeof sqliteStorage>;
let server: ReturnType<typeof Bun.serve>;
let httpBase: string;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  storage = sqliteStorage(db);
  const app = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
  server = Bun.serve({
    port: 0,
    fetch: (req, srv) => app.fetch(req, { IP: srv.requestIP(req), server: srv }),
    websocket,
  });
  httpBase = `http://localhost:${server.port}/api`;
});

afterAll(() => {
  server.stop(true);
  db.close();
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      rmSync(`${dbPath}${suffix}`, { force: true });
    }
    catch {}
  }
});

function authed(path: string, init?: RequestInit) {
  return fetch(`${httpBase}${path}`, {
    ...init,
    headers: { "authorization": `Bearer ${CHANNEL_TOKEN}`, "content-type": "application/json", ...(init?.headers ?? {}) },
  });
}

async function registerAgent(): Promise<string> {
  const res = await fetch(`${httpBase}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
  expect(res.status).toBe(200);
  const { agentId } = await res.json() as { agentId: string };
  expect((await authed(`/admin/agents/${agentId}/grant`, { method: "POST", body: JSON.stringify({ owner_id: OWNER }) })).status).toBe(200);
  return agentId;
}

describe("D-B4 channels — admin registration + auth", () => {
  it("registers a channel with owner_id + optional filter; 401 without the bearer", async () => {
    // Unauthenticated → 401 on admin AND messages surfaces.
    expect((await fetch(`${httpBase}/admin/channels`)).status).toBe(401);
    expect((await fetch(`${httpBase}/messages/whatever/send`, { method: "POST", body: "{}" })).status).toBe(401);
    expect((await fetch(`${httpBase}/messages/whatever/poll`)).status).toBe(401);

    const res = await authed("/admin/channels", {
      method: "POST",
      body: JSON.stringify({ name: "ops", type: "http", owner_id: OWNER, config: { engine: "claude", model: "sonnet" }, filter: { kinds: ["assistant_text"] } }),
    });
    expect(res.status).toBe(201);
    const { data } = await res.json() as { data: { id: string; ownerId: string; filter: { kinds: string[] } } };
    expect(data.ownerId).toBe(OWNER);
    expect(data.filter.kinds).toEqual(["assistant_text"]);

    // It shows up in the list.
    const list = await (await authed("/admin/channels")).json() as { data: Array<{ id: string }> };
    expect(list.data.some(c => c.id === data.id)).toBe(true);
  });
});

describe("D-B4 channels — ingress routing", () => {
  it("routes a NEW conversation to a new owner-stamped task → dispatched, and continues it on repeat", async () => {
    const agentId = await registerAgent();
    const reg = await authed("/admin/channels", {
      method: "POST",
      body: JSON.stringify({ name: "chat", type: "http", owner_id: OWNER, config: { engine: "claude", model: "sonnet" } }),
    });
    const channelId = (await reg.json() as { data: { id: string } }).data.id;

    // NEW conversation → new task, owner-stamped, placed on the permitted agent.
    const sent = await authed(`/messages/${channelId}/send`, { method: "POST", body: JSON.stringify({ conversation_ref: "c1", text: "hello there" }) });
    expect(sent.status).toBe(200);
    const r1 = (await sent.json() as { data: { taskId: string; created: boolean; delivered: boolean; dispatch?: { placed: boolean; agentId: string | null } } }).data;
    expect(r1.created).toBe(true);
    expect(r1.delivered).toBe(true);
    expect(r1.dispatch?.placed).toBe(true);
    expect(r1.dispatch?.agentId).toBe(agentId);

    // The task carries the channel owner (OD16).
    const task = await (await authed(`/admin/tasks/${r1.taskId}`)).json() as { data: { ownerId: string; agentId: string } };
    expect(task.data.ownerId).toBe(OWNER);
    expect(task.data.agentId).toBe(agentId);

    // SAME conversation_ref → continues the SAME task (no new task).
    const again = await authed(`/messages/${channelId}/send`, { method: "POST", body: JSON.stringify({ conversation_ref: "c1", text: "follow up" }) });
    const r2 = (await again.json() as { data: { taskId: string; created: boolean } }).data;
    expect(r2.created).toBe(false);
    expect(r2.taskId).toBe(r1.taskId);

    // A different conversation_ref → a different task.
    const fresh = await authed(`/messages/${channelId}/send`, { method: "POST", body: JSON.stringify({ conversation_ref: "c2", text: "new convo" }) });
    const r3 = (await fresh.json() as { data: { taskId: string; created: boolean } }).data;
    expect(r3.created).toBe(true);
    expect(r3.taskId).not.toBe(r1.taskId);

    // INGRESS never wrote the transcript — c1's task has no messages yet.
    expect(db.select().from(messages).all().filter(m => m.taskId === r1.taskId)).toHaveLength(0);

    // ── EGRESS: seed the M5 store for c1's task, then tail it by cursor ────────
    const thread = "thread-ch-1";
    writeThreadMessage(storage, { taskId: r1.taskId, threadId: thread, data: { seq: 1, kind: "user_input", content: "hello there" } });
    writeThreadMessage(storage, { taskId: r1.taskId, threadId: thread, data: { seq: 2, kind: "assistant_text", content: "hi! how can I help?" } });
    writeThreadMessage(storage, { taskId: r1.taskId, threadId: thread, data: { seq: 3, kind: "assistant_text", content: "anything else?" } });

    const poll1 = await authed(`/messages/${channelId}/poll`, { method: "POST", body: JSON.stringify({ conversation_ref: "c1" }) });
    expect(poll1.status).toBe(200);
    const d1 = (await poll1.json() as { data: { deliveries: Array<{ taskId: string; cursor: number; messages: Array<{ seq: number; kind: string }> }> } }).data;
    expect(d1.deliveries).toHaveLength(1);
    expect(d1.deliveries[0]!.taskId).toBe(r1.taskId);
    expect(d1.deliveries[0]!.messages.map(m => m.seq)).toEqual([1, 2, 3]); // all kinds, seq order

    // Unacked → at-least-once: a second poll re-delivers the same batch (HOST-M5M6-01).
    const poll2 = await authed(`/messages/${channelId}/poll`, { method: "POST", body: JSON.stringify({ conversation_ref: "c1" }) });
    const d2 = (await poll2.json() as { data: { deliveries: Array<{ messages: unknown[] }> } }).data;
    expect(d2.deliveries[0]!.messages).toHaveLength(3);

    // Acking the delivered cursor advances it — nothing new afterwards.
    const acked = await authed(`/messages/${channelId}/poll`, { method: "POST", body: JSON.stringify({ conversation_ref: "c1", ack: d1.deliveries[0]!.cursor }) });
    const dAcked = (await acked.json() as { data: { deliveries: unknown[] } }).data;
    expect(dAcked.deliveries).toHaveLength(0);

    // Re-poll from a stale cursor (after:0) → re-reads idempotently, cursor untouched.
    const replay = await authed(`/messages/${channelId}/poll`, { method: "POST", body: JSON.stringify({ conversation_ref: "c1", after: 0 }) });
    const d3 = (await replay.json() as { data: { deliveries: Array<{ messages: Array<{ seq: number }> }> } }).data;
    expect(d3.deliveries[0]!.messages.map(m => m.seq)).toEqual([1, 2, 3]);

    // Egress did NOT write the transcript — still exactly 3 rows for this task.
    expect(db.select().from(messages).all().filter(m => m.taskId === r1.taskId)).toHaveLength(3);
  });
});

describe("D-B4 channels — webhook CRUD + GET poll by task", () => {
  it("adds/removes an outbound webhook (SSRF-guarded) and polls by task via GET", async () => {
    await registerAgent();
    const reg = await authed("/admin/channels", {
      method: "POST",
      body: JSON.stringify({ name: "wh", type: "webhook", owner_id: OWNER, config: { engine: "claude", model: "sonnet" } }),
    });
    const channelId = (await reg.json() as { data: { id: string } }).data.id;

    // SSRF: a private/loopback webhook URL is rejected (422).
    expect((await authed(`/admin/channels/${channelId}/webhooks`, { method: "POST", body: JSON.stringify({ url: "http://127.0.0.1/x" }) })).status).toBe(422);

    // A public URL is accepted; the secret is never echoed back.
    const add = await authed(`/admin/channels/${channelId}/webhooks`, { method: "POST", body: JSON.stringify({ url: "https://hooks.example.com/in", secret: "shh" }) });
    expect(add.status).toBe(201);
    const hook = (await add.json() as { data: { id: string; url: string; secret?: string } }).data;
    expect(hook.url).toBe("https://hooks.example.com/in");
    expect(hook.secret).toBeUndefined();

    // The channel view lists the webhook (id + url only).
    const view = await (await authed(`/admin/channels/${channelId}`)).json() as { data: { webhooks: Array<{ id: string; url: string; secret?: string }> } };
    expect(view.data.webhooks).toHaveLength(1);
    expect(view.data.webhooks[0]!.secret).toBeUndefined();

    // Remove it.
    expect((await authed(`/admin/channels/${channelId}/webhooks/${hook.id}`, { method: "DELETE" })).status).toBe(200);
    expect((await authed(`/admin/channels/${channelId}/webhooks/${hook.id}`, { method: "DELETE" })).status).toBe(404);

    // GET poll scoped to an explicit task (no binding required) — empty for a fresh task.
    const create = await authed("/admin/tasks", { method: "POST", body: JSON.stringify({ owner_id: OWNER, engine: "claude", model: "sonnet" }) });
    const taskId = (await create.json() as { data: { task: { id: string } } }).data.task.id;
    const poll = await authed(`/messages/${channelId}/poll?task=${taskId}`);
    expect(poll.status).toBe(200);
    const data = (await poll.json() as { data: { deliveries: unknown[] } }).data;
    expect(data.deliveries).toHaveLength(0);
  });
});

describe("D-B4s security — explicit-taskId IDOR is 404 at the API layer", () => {
  it("rejects (404) a poll for a task the channel neither owns nor is bound to", async () => {
    const reg = await authed("/admin/channels", {
      method: "POST",
      body: JSON.stringify({ name: "idor", type: "http", owner_id: OWNER, config: { engine: "claude", model: "sonnet" } }),
    });
    const channelId = (await reg.json() as { data: { id: string } }).data.id;

    // A task owned by a DIFFERENT tenant, not bound to this channel.
    const create = await authed("/admin/tasks", { method: "POST", body: JSON.stringify({ owner_id: "other-owner", engine: "claude", model: "sonnet" }) });
    const victimTaskId = (await create.json() as { data: { task: { id: string } } }).data.task.id;
    writeThreadMessage(storage, { taskId: victimTaskId, threadId: "thread-idor", data: { seq: 1, kind: "assistant_text", content: "private" } });

    // GET and POST poll by explicit task → 404 (no transcript leaked).
    expect((await authed(`/messages/${channelId}/poll?task=${victimTaskId}`)).status).toBe(404);
    expect((await authed(`/messages/${channelId}/poll`, { method: "POST", body: JSON.stringify({ task: victimTaskId }) })).status).toBe(404);

    // A task this channel DOES own polls fine (200).
    const mine = await authed("/admin/tasks", { method: "POST", body: JSON.stringify({ owner_id: OWNER, engine: "claude", model: "sonnet" }) });
    const myTaskId = (await mine.json() as { data: { task: { id: string } } }).data.task.id;
    expect((await authed(`/messages/${channelId}/poll?task=${myTaskId}`)).status).toBe(200);
  });
});
