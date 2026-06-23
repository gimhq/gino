import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { and, eq, isNotNull } from "drizzle-orm";
import { websocket } from "hono/bun";
import { createApp } from "@/app";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { outbox, tasks } from "@/db/schema";
import { createLogger } from "@/shared/lib/logger";

// D-B3 — the END-TO-END MILESTONE, proven against a LIVE Bun server + real WS:
// create task → owner-pinned dispatch → orchestrator sends task_upsert + message
// over the M2 outbox → the connected agent SOCKET receives them → the agent
// streams thread_message + thread_result → M5 stores → /admin/tasks mirrors it.

const REGISTER_TOKEN = "reg-secret-e2e";
const CHANNEL_TOKEN = "chan-secret-e2e";
const OWNER = "owner-e2e";

const dbPath = resolve(tmpdir(), `gino-e2e-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: ReturnType<typeof createDb>;
let server: ReturnType<typeof Bun.serve>;
let httpBase: string;
let wsBase: string;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  const app = createApp({ config, db, logger: createLogger(config) });
  server = Bun.serve({
    port: 0,
    fetch: (req, srv) => app.fetch(req, { IP: srv.requestIP(req), server: srv }),
    websocket,
  });
  httpBase = `http://localhost:${server.port}/api`;
  wsBase = `ws://localhost:${server.port}/api`;
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

// ── helpers (mirror the B1b spine pattern) ────────────────────────────────────

interface OpenWs {
  ws: WebSocket;
  received: Array<Record<string, unknown>>;
}

type WsCtor = new (url: string, options?: { headers?: Record<string, string> }) => WebSocket;
const WS = WebSocket as unknown as WsCtor;

function openWs(agentId: string, token: string): Promise<OpenWs> {
  const ws = new WS(`${wsBase}/agent/ws/${agentId}`, { headers: { "x-internal-token": token } });
  const received: Array<Record<string, unknown>> = [];
  ws.addEventListener("message", (e: MessageEvent) => {
    received.push(JSON.parse(String(e.data)));
  });
  return new Promise((resolvePromise, reject) => {
    const timer = setTimeout(() => reject(new Error("ws open timeout")), 3000);
    ws.addEventListener("open", () => {
      clearTimeout(timer);
      resolvePromise({ ws, received });
    });
    ws.addEventListener("error", () => {
      clearTimeout(timer);
      reject(new Error("ws failed to open"));
    });
  });
}

async function waitFor<T extends Record<string, unknown>>(
  received: T[],
  pred: (f: T) => boolean,
  label: string,
  timeoutMs = 2500,
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const found = received.find(pred);
    if (found) {
      return found;
    }
    await new Promise(r => setTimeout(r, 20));
  }
  throw new Error(`timed out waiting for ${label}`);
}

function closeWs(ws: WebSocket): Promise<void> {
  return new Promise((resolvePromise) => {
    if (ws.readyState === WebSocket.CLOSED) {
      resolvePromise();
      return;
    }
    ws.addEventListener("close", () => resolvePromise());
    ws.close();
    setTimeout(resolvePromise, 1000);
  });
}

function admin(path: string, init?: RequestInit) {
  return fetch(`${httpBase}/admin${path}`, {
    ...init,
    headers: { "authorization": `Bearer ${CHANNEL_TOKEN}`, "content-type": "application/json", ...(init?.headers ?? {}) },
  });
}

async function register(): Promise<{ agentId: string; connectToken: string }> {
  const res = await fetch(`${httpBase}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
  expect(res.status).toBe(200);
  return await res.json() as { agentId: string; connectToken: string };
}

// ── the milestone ─────────────────────────────────────────────────────────────

describe("D-B3 end-to-end — create → pin → send → run → mirror", () => {
  it("drives a task from creation through the agent and back into the store", async () => {
    // 1. REGISTER an agent (engine claude) + grant the owner (dedicated default).
    const { agentId, connectToken } = await register();
    expect((await admin(`/agents/${agentId}/grant`, { method: "POST", body: JSON.stringify({ owner_id: OWNER }) })).status).toBe(200);

    // 2. CONNECT the simulated agent over WS (now online for live delivery).
    const conn = await openWs(agentId, connectToken);

    // 3. CREATE a task → dispatch pins it to the (only permitted) agent.
    const createRes = await admin("/tasks", {
      method: "POST",
      body: JSON.stringify({ owner_id: OWNER, engine: "claude", model: "sonnet", name: "e2e task" }),
    });
    expect(createRes.status).toBe(201);
    const created = await createRes.json() as { data: { task: { id: string; agentId: string; status: string }; dispatch: { placed: boolean; agentId: string } } };
    const taskId = created.data.task.id;
    expect(created.data.dispatch.placed).toBe(true);
    expect(created.data.dispatch.agentId).toBe(agentId);
    expect(created.data.task.agentId).toBe(agentId); // PINNED
    expect(created.data.task.status).toBe("active");

    // 4. SEND work → orchestrator enqueues task_upsert (first) + message via outbox.
    const msgRes = await admin(`/tasks/${taskId}/message`, { method: "POST", body: JSON.stringify({ text: "do the thing" }) });
    expect(msgRes.status).toBe(200);
    const sent = await msgRes.json() as { data: { threadId: string; msgId: number; seeded: boolean } };
    const threadId = sent.data.threadId;
    expect(sent.data.seeded).toBe(true); // task_upsert seeded on the first message

    // 5. The connected agent SOCKET receives both commands (the outbox delivered).
    const taskUpsert = await waitFor(conn.received, f => f.type === "task_upsert" && f.taskId === taskId, "task_upsert");
    expect((taskUpsert.data as { engine?: string }).engine).toBe("claude");
    const message = await waitFor(conn.received, f => f.type === "message" && f.taskId === taskId, "message");
    expect(message.msgId).toBe(sent.data.msgId);
    expect(message.threadId).toBe(threadId);
    expect((message.data as { text?: string }).text).toBe("do the thing");

    // Agent acks both (so the outbox stops replaying).
    conn.ws.send(JSON.stringify({ type: "ack", id: taskUpsert.outboxId }));
    conn.ws.send(JSON.stringify({ type: "msg_ack", msgId: message.msgId }));

    // 6. Agent RUNS the thread → streams a transcript event + the terminal result.
    conn.ws.send(JSON.stringify({
      type: "thread_message",
      outboxId: 1001,
      taskId,
      threadId,
      data: { seq: 1, kind: "assistant_text", content: "done the thing" },
    }));
    conn.ws.send(JSON.stringify({
      type: "thread_result",
      outboxId: 1002,
      taskId,
      threadId,
      data: { status: "succeeded", engine_session_id: "sess-1", duration_ms: 42, final_seq: 1 },
    }));
    await waitFor(conn.received, f => f.type === "ack" && f.id === 1001, "ack thread_message");
    await waitFor(conn.received, f => f.type === "ack" && f.id === 1002, "ack thread_result");

    // 7. M5 stored it; /admin/tasks/:id MIRRORS the thread + transcript.
    const getRes = await admin(`/tasks/${taskId}`);
    expect(getRes.status).toBe(200);
    const view = await getRes.json() as {
      data: {
        agentId: string;
        currentThreadId: string;
        threads: Array<{ threadId: string; run: { status: string; finalSeq: number; engineSessionId: string } | null; messages: Array<{ seq: number; content: string }> }>;
      };
    };
    expect(view.data.agentId).toBe(agentId);
    expect(view.data.currentThreadId).toBe(threadId);
    const thread = view.data.threads.find(t => t.threadId === threadId);
    expect(thread).toBeDefined();
    expect(thread!.run?.status).toBe("succeeded");
    expect(thread!.run?.finalSeq).toBe(1);
    expect(thread!.run?.engineSessionId).toBe("sess-1");
    expect(thread!.messages.map(m => m.content)).toEqual(["done the thing"]);

    // 8. The outbox DELIVERED the commands and they are now ACKED (durable path).
    const ackedTaskUpsert = db.select().from(outbox).where(and(eq(outbox.taskId, taskId), eq(outbox.type, "task_upsert"), isNotNull(outbox.ackedAt))).all();
    const ackedMessage = db.select().from(outbox).where(and(eq(outbox.taskId, taskId), eq(outbox.type, "message"), isNotNull(outbox.ackedAt))).all();
    expect(ackedTaskUpsert).toHaveLength(1);
    expect(ackedMessage).toHaveLength(1);

    // 9. PIN ONCE: assigning a DIFFERENT agent after placement is rejected (409).
    const { agentId: other } = await register();
    expect((await admin(`/agents/${other}/grant`, { method: "POST", body: JSON.stringify({ owner_id: OWNER }) })).status).toBe(200);
    const reassign = await admin(`/tasks/${taskId}/assign`, { method: "POST", body: JSON.stringify({ agent_id: other }) });
    expect(reassign.status).toBe(409);
    // The original placement is unchanged.
    expect(db.select().from(tasks).where(eq(tasks.id, taskId)).get()?.agentId).toBe(agentId);

    await closeWs(conn.ws);
  });

  it("401s the /admin/tasks surface without the channel bearer", async () => {
    expect((await fetch(`${httpBase}/admin/tasks`)).status).toBe(401);
  });

  it("emits cron_upsert + cron_delete commands to the agent", async () => {
    const { agentId, connectToken } = await register();
    expect((await admin(`/agents/${agentId}/grant`, { method: "POST", body: JSON.stringify({ owner_id: OWNER }) })).status).toBe(200);
    const conn = await openWs(agentId, connectToken);

    const created = await (await admin("/tasks", {
      method: "POST",
      body: JSON.stringify({ owner_id: OWNER, engine: "claude", model: "sonnet" }),
    })).json() as { data: { task: { id: string } } };
    const taskId = created.data.task.id;

    const cronRes = await admin(`/tasks/${taskId}/crons`, {
      method: "POST",
      body: JSON.stringify({ kind: "schedule", continuity: "isolated", schedule_type: "cron", schedule_value: "0 9 * * *", prompt: "daily standup" }),
    });
    expect(cronRes.status).toBe(201);
    const { data: { cronId } } = await cronRes.json() as { data: { cronId: string } };

    const cronCmd = await waitFor(conn.received, f => f.type === "cron_upsert" && f.cronId === cronId, "cron_upsert");
    expect((cronCmd.data as { continuity?: string }).continuity).toBe("isolated");
    conn.ws.send(JSON.stringify({ type: "ack", id: cronCmd.outboxId }));

    expect((await admin(`/tasks/${taskId}/crons/${cronId}`, { method: "DELETE" })).status).toBe(200);
    const del = await waitFor(conn.received, f => f.type === "cron_delete" && f.cronId === cronId, "cron_delete");
    expect(del.taskId).toBe(taskId);

    await closeWs(conn.ws);
  });
});
