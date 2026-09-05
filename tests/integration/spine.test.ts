import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { websocket } from "hono/bun";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { tasks } from "@/db/schema";
import { getThreadRun, getTranscript } from "@/msgstore";
import { enqueue } from "@/protocol/outbox";
import { createLogger } from "@/shared/lib/logger";
import { sqliteStorage } from "@/store/sqlite";

// D-B1b — the PROTOCOL SPINE, proven end-to-end against a LIVE Bun server with a
// real WebSocket: register → connect (connect-token auth) → frames persist + ack
// → host→agent outbox delivers → drop + reconnect → unacked outbox REPLAYS.

const REGISTER_TOKEN = "reg-secret-spine";
const dbPath = resolve(tmpdir(), `gino-spine-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: "chan-secret-spine",
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: ReturnType<typeof createDb>;
let storage: ReturnType<typeof sqliteStorage>;
let host: ReturnType<typeof createAppHost>;
let server: ReturnType<typeof Bun.serve>;
let httpBase: string;
let wsBase: string;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  storage = sqliteStorage(db);
  host = createAppHost({ config, db, logger: createLogger(config) });
  const app = createApp({ config, host });
  server = Bun.serve({
    port: 0,
    // Thread `server` into env so the hono/bun adapter can upgrade; wire `websocket`.
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

// ── helpers ──────────────────────────────────────────────────────────────────

interface OpenWs {
  ws: WebSocket;
  received: Array<Record<string, unknown>>;
}

// Bun's WebSocket client accepts a `{ headers }` option (a Bun extension); the
// DOM lib's constructor type only knows `protocols`, so type the constructor
// through `unknown` to reach the header form without `any`.
type WsCtor = new (url: string, options?: { headers?: Record<string, string> }) => WebSocket;
const WS = WebSocket as unknown as WsCtor;

function openWs(agentId: string, token: string): Promise<OpenWs> {
  const ws = new WS(`${wsBase}/agent/ws/${agentId}`, { headers: { "x-internal-token": token } });
  const received: Array<Record<string, unknown>> = [];
  ws.addEventListener("message", (e: MessageEvent) => {
    received.push(JSON.parse(String(e.data)));
  });
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("ws open timeout")), 3000);
    ws.addEventListener("open", () => {
      clearTimeout(timer);
      resolve({ ws, received });
    });
    ws.addEventListener("error", () => {
      clearTimeout(timer);
      reject(new Error("ws failed to open"));
    });
  });
}

/** True if the WS is rejected (never opens) — the 401 path for a bad/missing token. */
function wsRejected(agentId: string, headers?: Record<string, string>): Promise<boolean> {
  const ws = new WS(`${wsBase}/agent/ws/${agentId}`, headers ? { headers } : undefined);
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      try {
        ws.close();
      }
      catch {}
      resolve(false); // opened (or hung) → NOT rejected
    }, 1500);
    ws.addEventListener("open", () => {
      clearTimeout(timer);
      try {
        ws.close();
      }
      catch {}
      resolve(false);
    });
    ws.addEventListener("error", () => {
      clearTimeout(timer);
      resolve(true);
    });
    ws.addEventListener("close", () => {
      clearTimeout(timer);
      resolve(true);
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
  return new Promise((resolve) => {
    if (ws.readyState === WebSocket.CLOSED) {
      resolve();
      return;
    }
    ws.addEventListener("close", () => resolve());
    ws.close();
    setTimeout(resolve, 1000);
  });
}

async function register(body: Record<string, unknown>): Promise<Response> {
  return fetch(`${httpBase}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

// ── the spine ────────────────────────────────────────────────────────────────

describe("D-B1b protocol spine — register → connect → frames → store → ack → replay", () => {
  it("runs the full at-least-once round trip both ways", async () => {
    // 1. REGISTER (shared register token) → agentId + connect token + config.
    const regRes = await register({
      registerToken: REGISTER_TOKEN,
      protocolVersion: 2,
      name: "spine-agent",
      source: "manual",
      capabilities: ["claude"],
    });
    expect(regRes.status).toBe(200);
    const reg = await regRes.json() as { agentId: string; connectToken: string; config: Record<string, unknown> };
    expect(reg.agentId).toBeTruthy();
    expect(reg.connectToken).toBeTruthy();
    expect(reg.config).toMatchObject({
      agentId: reg.agentId,
      wsUrl: `/agent/ws/${reg.agentId}`,
      heartbeatIntervalMs: 30000,
      protocolVersion: 2,
    });

    const agentId = reg.agentId;
    const taskId = `t-${agentId}`;
    const threadId = `c-${agentId}@1`;

    // M4 owns task identity (B3); the spine seeds the task so the M5 thread_runs
    // FK (task_id → tasks) is satisfied when the terminal result is stored. The
    // task is PLACED on this agent (`agentId`) — inbound write frames are
    // authorized against `tasks.agent_id` (D-SEC: an agent may only write its own
    // placed task), so the spine's own frames must carry that placement.
    db.insert(tasks).values({ id: taskId, ownerId: "owner-1", engine: "claude", model: "sonnet", status: "active", agentId }).run();

    // 2. CONNECT over WS with the per-agent connect token.
    const conn = await openWs(agentId, reg.connectToken);

    // 3. INBOUND: a transcript event + the terminal result (each with its own outboxId).
    conn.ws.send(JSON.stringify({
      type: "thread_message",
      outboxId: 1,
      taskId,
      threadId,
      data: { seq: 1, kind: "assistant_text", content: "hello from agent" },
    }));
    conn.ws.send(JSON.stringify({
      type: "thread_result",
      outboxId: 2,
      taskId,
      threadId,
      data: { status: "succeeded", duration_ms: 12, final_seq: 1 },
    }));

    // Host replies ack{id} for each inbound frame (at-least-once, agent→host).
    await waitFor(conn.received, f => f.type === "ack" && f.id === 1, "ack id=1");
    await waitFor(conn.received, f => f.type === "ack" && f.id === 2, "ack id=2");

    // Rows landed in M5 (read via the M5 read helpers).
    const transcript = getTranscript(storage, threadId);
    expect(transcript).toHaveLength(1);
    expect(transcript[0]!.content).toBe("hello from agent");
    const run = getThreadRun(storage, threadId);
    expect(run?.status).toBe("succeeded");
    expect(run?.finalSeq).toBe(1);
    expect(run?.source).toBe("chat"); // B1b dispatch default

    // 4. OUTBOX (host→agent): enqueue a `message`; the live session delivers it.
    const msgRow = enqueue(db, host.sessions, { agentId, type: "message", taskId, threadId, data: { text: "hi from host" } });
    const delivered = await waitFor(
      conn.received,
      f => f.type === "message" && (f.data as { text?: string } | undefined)?.text === "hi from host",
      "host→agent message",
    );
    expect(delivered.msgId).toBe(msgRow.id); // message carries msgId, not outboxId
    expect(delivered.outboxId).toBeUndefined();

    // 5. DROP the socket WITHOUT acking the message → it stays unacked.
    await closeWs(conn.ws);
    // Let onClose run (detach + status offline).
    await new Promise(r => setTimeout(r, 100));

    // 6. RECONNECT → the unacked outbox message is REPLAYED on attach.
    const conn2 = await openWs(agentId, reg.connectToken);
    const replayed = await waitFor(
      conn2.received,
      f => f.type === "message" && (f.data as { text?: string } | undefined)?.text === "hi from host",
      "replayed host→agent message",
    );
    expect(replayed.msgId).toBe(msgRow.id);

    // Agent now durably stores it → msg_ack → host stops replaying.
    conn2.ws.send(JSON.stringify({ type: "msg_ack", msgId: msgRow.id }));
    await new Promise(r => setTimeout(r, 150));
    await closeWs(conn2.ws);
    await new Promise(r => setTimeout(r, 100));

    // A 3rd connect sees NO replay of the now-acked message.
    const conn3 = await openWs(agentId, reg.connectToken);
    await new Promise(r => setTimeout(r, 250));
    expect(conn3.received.some(f => f.type === "message")).toBe(false);
    await closeWs(conn3.ws);
  });
});

describe("D-B1b protocol spine — gates", () => {
  it("version gate rejects protocolVersion != 2 with register_error{unsupported_protocol}", async () => {
    const res = await register({ registerToken: REGISTER_TOKEN, protocolVersion: 1, capabilities: ["claude"] });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ register_error: { reason: "unsupported_protocol" } });
  });

  it("register rejects a bad register token (401)", async () => {
    const res = await register({ registerToken: "wrong", protocolVersion: 2, capabilities: ["claude"] });
    expect(res.status).toBe(401);
  });

  it("WS connect rejects a bad connect token and a missing token", async () => {
    const reg = await (await register({ registerToken: REGISTER_TOKEN, protocolVersion: 2, capabilities: ["claude"] })).json() as { agentId: string };
    expect(await wsRejected(reg.agentId, { "x-internal-token": "bogus-token" })).toBe(true);
    expect(await wsRejected(reg.agentId)).toBe(true);
  });
});
