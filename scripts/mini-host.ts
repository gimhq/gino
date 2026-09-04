#!/usr/bin/env bun
/* eslint-disable no-console */
/**
 * mini-host — a single-file, zero-dependency gino host speaking protocol v2
 * (docs/protocol.md). Enough to enroll a `gino-agent`, dispatch work to it and
 * read back the execution transcript. Not the real control plane: no launchers,
 * no attachments, no provisioning, no auth on the operator API (bind to loopback).
 *
 *   bun scripts/mini-host.ts
 *
 * Environment:
 *   MINI_HOST_PORT            listen port                       (default 3100)
 *   MINI_HOST_BIND            listen address                    (default 127.0.0.1)
 *   MINI_HOST_REGISTER_TOKEN  enrollment secret the agent sends (default "dev")
 *   MINI_HOST_DB              sqlite file                       (default ./mini-host.db)
 *   ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN / ANTHROPIC_BASE_URL / MINI_HOST_MODEL
 *                             forwarded to the agent in `config.engine`
 *
 * Agent side (protocol v2):
 *   POST /agent/register          GET /agent/config/:id
 *   WS   /agent/ws/:id            POST /agent/output/:id
 *
 * Operator side (JSON):
 *   GET    /api/agents
 *   GET    /api/tasks                       POST /api/tasks {agentId?, taskId?, name?, engine?, model?, workdir?}
 *   DELETE /api/tasks/:id
 *   POST   /api/tasks/:id/messages          {text, newThread?}   → {threadId, msgId}
 *   POST   /api/tasks/:id/interrupt
 *   POST   /api/tasks/:id/crons             {cronId?, prompt, scheduleType, scheduleValue, continuity?}
 *   DELETE /api/tasks/:id/crons/:cronId
 *   GET    /api/tasks/:id/threads
 *   GET    /api/threads/:threadId           run + ordered transcript
 *   GET    /api/threads/:threadId/wait?timeoutMs=60000   long-poll for the terminal result
 *   GET    /api/crons
 */

import type { ServerWebSocket } from "bun";
import process from "node:process";
import { Database } from "bun:sqlite";

const PROTOCOL_VERSION = 2;
const HEARTBEAT_INTERVAL_MS = 30_000;
const PORT = Number(process.env.MINI_HOST_PORT ?? 3100);
const BIND = process.env.MINI_HOST_BIND ?? "127.0.0.1";
const REGISTER_TOKEN = process.env.MINI_HOST_REGISTER_TOKEN ?? "dev";
const DB_PATH = process.env.MINI_HOST_DB ?? "./mini-host.db";

// ─── storage ─────────────────────────────────────────────────────────────────

const db = new Database(DB_PATH, { create: true });
db.exec("PRAGMA journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY, name TEXT, connect_token TEXT NOT NULL,
    capabilities TEXT, registered_at TEXT NOT NULL, last_seen TEXT
  );
  CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL,
    frame TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, name TEXT,
    current_thread_id TEXT, created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS threads (
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS thread_runs (
    thread_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, status TEXT NOT NULL,
    result TEXT, error TEXT, failure_reason TEXT, engine_session_id TEXT,
    duration_ms INTEGER, final_seq INTEGER, received_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS messages (
    task_id TEXT NOT NULL, thread_id TEXT NOT NULL, seq INTEGER NOT NULL,
    kind TEXT NOT NULL, data TEXT NOT NULL, received_at TEXT NOT NULL,
    PRIMARY KEY (thread_id, seq)
  );
  CREATE TABLE IF NOT EXISTS cron_states (
    cron_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, data TEXT NOT NULL, received_at TEXT NOT NULL
  );
`);

type Row = Record<string, unknown>;
const q = {
  agentById: db.query<Row, [string]>("SELECT * FROM agents WHERE id = ?"),
  agents: db.query<Row, []>("SELECT id, name, capabilities, registered_at, last_seen FROM agents ORDER BY registered_at"),
  insertAgent: db.query("INSERT INTO agents (id, name, connect_token, capabilities, registered_at) VALUES (?, ?, ?, ?, ?)"),
  touchAgent: db.query("UPDATE agents SET last_seen = ? WHERE id = ?"),
  insertOutbox: db.query<{ id: number }, [string, string, string]>("INSERT INTO outbox (agent_id, frame, created_at) VALUES (?, ?, ?) RETURNING id"),
  unacked: db.query<{ id: number; frame: string }, [string]>("SELECT id, frame FROM outbox WHERE agent_id = ? AND acked = 0 ORDER BY id"),
  ackOutbox: db.query("UPDATE outbox SET acked = 1 WHERE id = ? AND agent_id = ?"),
  tasks: db.query<Row, []>("SELECT * FROM tasks ORDER BY created_at"),
  taskById: db.query<Row, [string]>("SELECT * FROM tasks WHERE id = ?"),
  insertTask: db.query("INSERT INTO tasks (id, agent_id, name, created_at) VALUES (?, ?, ?, ?)"),
  deleteTask: db.query("DELETE FROM tasks WHERE id = ?"),
  setCurrentThread: db.query("UPDATE tasks SET current_thread_id = ? WHERE id = ?"),
  insertThread: db.query("INSERT OR IGNORE INTO threads (id, task_id, created_at) VALUES (?, ?, ?)"),
  threadsByTask: db.query<Row, [string]>(
    `SELECT t.id AS thread_id, t.created_at, r.status, r.result, r.error, r.failure_reason, r.duration_ms, r.final_seq,
            (SELECT COUNT(*) FROM messages m WHERE m.thread_id = t.id) AS message_count
       FROM threads t LEFT JOIN thread_runs r ON r.thread_id = t.id
      WHERE t.task_id = ? ORDER BY t.created_at`,
  ),
  runByThread: db.query<Row, [string]>("SELECT * FROM thread_runs WHERE thread_id = ?"),
  upsertRun: db.query(
    `INSERT INTO thread_runs (thread_id, task_id, status, result, error, failure_reason, engine_session_id, duration_ms, final_seq, received_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(thread_id) DO UPDATE SET status = excluded.status, result = excluded.result, error = excluded.error,
       failure_reason = excluded.failure_reason, engine_session_id = excluded.engine_session_id,
       duration_ms = excluded.duration_ms, final_seq = excluded.final_seq, received_at = excluded.received_at`,
  ),
  insertMessage: db.query("INSERT OR IGNORE INTO messages (task_id, thread_id, seq, kind, data, received_at) VALUES (?, ?, ?, ?, ?, ?)"),
  messagesByThread: db.query<Row, [string]>("SELECT seq, kind, data, received_at FROM messages WHERE thread_id = ? ORDER BY seq"),
  upsertCronState: db.query(
    `INSERT INTO cron_states (cron_id, task_id, data, received_at) VALUES (?, ?, ?, ?)
     ON CONFLICT(cron_id) DO UPDATE SET task_id = excluded.task_id, data = excluded.data, received_at = excluded.received_at`,
  ),
  cronStates: db.query<Row, []>("SELECT * FROM cron_states ORDER BY cron_id"),
};

const now = () => new Date().toISOString();
const newId = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
const str = (v: unknown) => (typeof v === "string" ? v : undefined);
const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : undefined);
const asObject = (v: unknown): Row => (v !== null && typeof v === "object" && !Array.isArray(v) ? (v as Row) : {});
const parseJson = (v: unknown) => (typeof v === "string" ? JSON.parse(v) : v);

// ─── agent-facing config + outbox ────────────────────────────────────────────

function agentConfig(agentId: string) {
  const engine: Row = {};
  if (process.env.ANTHROPIC_API_KEY)
    engine.anthropicApiKey = process.env.ANTHROPIC_API_KEY;
  if (process.env.CLAUDE_CODE_OAUTH_TOKEN)
    engine.claudeCodeOauthToken = process.env.CLAUDE_CODE_OAUTH_TOKEN;
  if (process.env.ANTHROPIC_BASE_URL)
    engine.anthropicBaseUrl = process.env.ANTHROPIC_BASE_URL;
  if (process.env.MINI_HOST_MODEL)
    engine.model = process.env.MINI_HOST_MODEL;
  return {
    agentId,
    wsUrl: `/agent/ws/${agentId}`,
    configUrl: `/agent/config/${agentId}`,
    outputUrl: `/agent/output/${agentId}`,
    heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
    protocolVersion: PROTOCOL_VERSION,
    entries: [],
    version: 0,
    engine,
  };
}

type Socket = ServerWebSocket<{ agentId: string }>;
const sockets = new Map<string, Socket>();

/** Persist a host→agent frame (its row id is the `msgId`) and push it if the agent is online. */
function enqueue(agentId: string, frame: Row): number {
  const { id } = q.insertOutbox.get(agentId, JSON.stringify(frame), now())!;
  const ws = sockets.get(agentId);
  if (ws)
    ws.send(JSON.stringify({ ...frame, msgId: id, outboxId: id }));
  return id;
}

/** `interrupt` is live-only (protocol §5): never persisted, never replayed. */
function sendLive(agentId: string, frame: Row): boolean {
  const ws = sockets.get(agentId);
  if (!ws)
    return false;
  ws.send(JSON.stringify(frame));
  return true;
}

function replayUnacked(ws: Socket) {
  const rows = q.unacked.all(ws.data.agentId);
  for (const row of rows) {
    ws.send(JSON.stringify({ ...JSON.parse(row.frame), msgId: row.id, outboxId: row.id }));
  }
  if (rows.length > 0)
    console.log(`[ws] ${ws.data.agentId} replayed ${rows.length} unacked frame(s)`);
}

// ─── agent → host frames ─────────────────────────────────────────────────────

/** Mirror one inbound frame; returns the `outboxId` to ack (or undefined when nothing needs acking). */
function handleAgentFrame(agentId: string, frame: Row): number | undefined {
  const type = str(frame.type);
  const outboxId = num(frame.outboxId);
  const taskId = str(frame.taskId) ?? "";
  const threadId = str(frame.threadId) ?? "";
  const data = asObject(frame.data);
  const ts = now();
  switch (type) {
    case "msg_ack": {
      const msgId = num(frame.msgId);
      if (msgId !== undefined)
        q.ackOutbox.run(msgId, agentId);
      return undefined;
    }
    case "ack":
      return undefined;
    case "heartbeat":
      q.touchAgent.run(ts, agentId);
      return outboxId;
    case "thread_message": {
      const seq = num(data.seq);
      if (threadId && seq !== undefined) {
        q.insertThread.run(threadId, taskId, ts);
        q.insertMessage.run(taskId, threadId, seq, str(data.kind) ?? "unknown", JSON.stringify(data), ts);
      }
      return outboxId;
    }
    case "thread_result": {
      if (threadId) {
        q.insertThread.run(threadId, taskId, ts);
        q.upsertRun.run(
          threadId,
          taskId,
          str(data.status) ?? "unknown",
          str(data.result) ?? null,
          str(data.error) ?? null,
          str(data.failure_reason) ?? null,
          str(data.engine_session_id) ?? null,
          num(data.duration_ms) ?? null,
          num(data.final_seq) ?? null,
          ts,
        );
      }
      return outboxId;
    }
    case "cron_state": {
      const cronId = str(frame.cronId);
      if (cronId)
        q.upsertCronState.run(cronId, taskId, JSON.stringify(data), ts);
      return outboxId;
    }
    default:
      // Unknown type: ignore the payload but still ack so the agent's outbox drains (§1 additive rule).
      return outboxId;
  }
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const error = (status: number, message: string) => json({ error: message }, status);

async function readJson(req: Request): Promise<Row> {
  try {
    return asObject(await req.json());
  }
  catch {
    return {};
  }
}

function authAgent(req: Request, agentId: string): Row | undefined {
  const agent = q.agentById.get(agentId);
  if (!agent)
    return undefined;
  const token = req.headers.get("x-internal-token");
  return token && token === agent.connect_token ? agent : undefined;
}

/** Resolve the agent a new task should target: explicit, or the only registered one. */
function pickAgent(explicit: string | undefined): string | Response {
  if (explicit)
    return q.agentById.get(explicit) ? explicit : error(404, `unknown agent ${explicit}`);
  const all = q.agents.all();
  if (all.length === 1)
    return all[0]!.id as string;
  return error(400, all.length === 0 ? "no agent registered yet" : "several agents registered; pass agentId");
}

function threadView(threadId: string) {
  const run = q.runByThread.get(threadId) ?? null;
  const messages = q.messagesByThread.all(threadId).map(m => ({
    seq: m.seq,
    kind: m.kind,
    received_at: m.received_at,
    ...asObject(parseJson(m.data)),
  }));
  return { threadId, run, messages };
}

// ─── routes ──────────────────────────────────────────────────────────────────

async function agentRoutes(req: Request, url: URL, server: ReturnType<typeof Bun.serve>): Promise<Response | undefined> {
  const parts = url.pathname.split("/").filter(Boolean); // ["agent", ...]
  const [, kind, agentId] = parts;

  if (kind === "register" && req.method === "POST") {
    const body = await readJson(req);
    if (body.registerToken !== REGISTER_TOKEN)
      return json({ register_error: { reason: "invalid_register_token" } }, 401);
    if (body.protocolVersion !== PROTOCOL_VERSION)
      return json({ register_error: { reason: "unsupported_protocol" } }, 400);
    const id = newId("ag");
    const connectToken = crypto.randomUUID().replaceAll("-", "") + crypto.randomUUID().replaceAll("-", "");
    q.insertAgent.run(id, str(body.name) ?? null, connectToken, JSON.stringify(body.capabilities ?? []), now());
    console.log(`[register] ${id} (${str(body.name) ?? "unnamed"}) capabilities=${JSON.stringify(body.capabilities ?? [])}`);
    return json({ agentId: id, connectToken, config: agentConfig(id) });
  }

  if (!agentId)
    return error(404, "not found");
  if (!authAgent(req, agentId))
    return error(401, "invalid connect token");

  if (kind === "config" && req.method === "GET")
    return json(agentConfig(agentId));

  if (kind === "output" && req.method === "POST") {
    const frame = await readJson(req);
    const outboxId = handleAgentFrame(agentId, frame);
    return json(outboxId === undefined ? {} : { type: "ack", id: outboxId });
  }

  if (kind === "ws" && req.method === "GET") {
    if (server.upgrade(req, { data: { agentId } }))
      return undefined;
    return error(426, "upgrade required");
  }
  return error(404, "not found");
}

async function apiRoutes(req: Request, url: URL): Promise<Response> {
  const parts = url.pathname.split("/").filter(Boolean); // ["api", ...]
  const [, resource, id, sub, subId] = parts;

  if (resource === "agents" && req.method === "GET") {
    return json(q.agents.all().map(a => ({ ...a, capabilities: parseJson(a.capabilities), online: sockets.has(a.id as string) })));
  }

  if (resource === "crons" && !id && req.method === "GET") {
    return json(q.cronStates.all().map(c => ({ cronId: c.cron_id, taskId: c.task_id, received_at: c.received_at, ...asObject(parseJson(c.data)) })));
  }

  if (resource === "threads" && id) {
    if (!sub && req.method === "GET")
      return json(threadView(id));
    if (sub === "wait" && req.method === "GET") {
      const timeoutMs = Number(url.searchParams.get("timeoutMs") ?? 60_000);
      const deadline = Date.now() + timeoutMs;
      while (!q.runByThread.get(id)) {
        if (Date.now() >= deadline)
          return json({ ...threadView(id), timedOut: true }, 408);
        await Bun.sleep(200);
      }
      return json(threadView(id));
    }
  }

  if (resource !== "tasks")
    return error(404, "not found");

  if (!id) {
    if (req.method === "GET") {
      return json(q.tasks.all().map(t => ({ ...t, agentOnline: sockets.has(t.agent_id as string) })));
    }
    if (req.method === "POST") {
      const body = await readJson(req);
      const agentId = pickAgent(str(body.agentId));
      if (agentId instanceof Response)
        return agentId;
      const taskId = str(body.taskId) ?? newId("task");
      if (q.taskById.get(taskId))
        return error(409, `task ${taskId} exists`);
      q.insertTask.run(taskId, agentId, str(body.name) ?? null, now());
      const data: Row = { status: "active" };
      for (const key of ["name", "kind", "engine", "model", "workdir", "meta"]) {
        if (body[key] !== undefined)
          data[key] = body[key];
      }
      const msgId = enqueue(agentId, { type: "task_upsert", taskId, data });
      return json({ taskId, agentId, msgId }, 201);
    }
    return error(405, "method not allowed");
  }

  const task = q.taskById.get(id);
  if (!task)
    return error(404, `unknown task ${id}`);
  const agentId = task.agent_id as string;

  if (!sub) {
    if (req.method === "GET")
      return json({ ...task, threads: q.threadsByTask.all(id) });
    if (req.method === "DELETE") {
      q.deleteTask.run(id);
      return json({ taskId: id, msgId: enqueue(agentId, { type: "task_delete", taskId: id }) });
    }
  }

  if (sub === "threads" && req.method === "GET")
    return json(q.threadsByTask.all(id));

  if (sub === "messages" && req.method === "POST") {
    const body = await readJson(req);
    const text = str(body.text);
    if (!text)
      return error(400, "text is required");
    // The host mints the thread id and records it BEFORE emitting the frame (protocol §5).
    let threadId = str(task.current_thread_id);
    if (!threadId || body.newThread === true) {
      threadId = newId("th");
      q.insertThread.run(threadId, id, now());
      q.setCurrentThread.run(threadId, id);
    }
    const msgId = enqueue(agentId, { type: "message", taskId: id, threadId, data: { text } });
    return json({ taskId: id, threadId, msgId, agentOnline: sockets.has(agentId) }, 202);
  }

  if (sub === "interrupt" && req.method === "POST") {
    return sendLive(agentId, { type: "interrupt", taskId: id }) ? json({ sent: true }) : error(409, "agent offline; interrupt is live-only");
  }

  if (sub === "crons") {
    if (req.method === "POST") {
      const body = await readJson(req);
      const prompt = str(body.prompt);
      const scheduleType = str(body.scheduleType);
      const scheduleValue = str(body.scheduleValue);
      if (!prompt || !scheduleType || !scheduleValue)
        return error(400, "prompt, scheduleType (cron|interval|once) and scheduleValue are required");
      const cronId = str(body.cronId) ?? newId("cron");
      const data = {
        kind: "schedule",
        prompt,
        schedule_type: scheduleType,
        schedule_value: scheduleValue,
        continuity: str(body.continuity) ?? "isolated",
        status: "active",
      };
      return json({ taskId: id, cronId, msgId: enqueue(agentId, { type: "cron_upsert", taskId: id, cronId, data }) }, 201);
    }
    if (req.method === "DELETE" && subId) {
      return json({ taskId: id, cronId: subId, msgId: enqueue(agentId, { type: "cron_delete", taskId: id, cronId: subId }) });
    }
  }
  return error(404, "not found");
}

// ─── server ──────────────────────────────────────────────────────────────────

Bun.serve<{ agentId: string }>({
  port: PORT,
  hostname: BIND,
  async fetch(req, srv) {
    const url = new URL(req.url);
    try {
      if (url.pathname.startsWith("/agent/"))
        return await agentRoutes(req, url, srv);
      if (url.pathname.startsWith("/api/"))
        return await apiRoutes(req, url);
      if (url.pathname === "/")
        return json({ ok: true, agents: q.agents.all().length, online: sockets.size });
      return error(404, "not found");
    }
    catch (err) {
      console.error(`[http] ${req.method} ${url.pathname} failed:`, err);
      return error(500, String(err));
    }
  },
  websocket: {
    open(ws) {
      const { agentId } = ws.data;
      sockets.get(agentId)?.close(1000, "superseded");
      sockets.set(agentId, ws);
      q.touchAgent.run(now(), agentId);
      console.log(`[ws] ${agentId} connected`);
      replayUnacked(ws);
    },
    message(ws, raw) {
      let frame: Row;
      try {
        frame = asObject(JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw)));
      }
      catch {
        return;
      }
      const outboxId = handleAgentFrame(ws.data.agentId, frame);
      if (outboxId !== undefined)
        ws.send(JSON.stringify({ type: "ack", id: outboxId }));
      const t = frame.type;
      if (t === "thread_result" || t === "thread_message" || t === "cron_state") {
        const d = asObject(frame.data);
        console.log(`[ws] ${t} task=${frame.taskId ?? "-"} thread=${frame.threadId ?? frame.cronId ?? "-"} ${str(d.status) ?? str(d.kind) ?? ""}`);
      }
    },
    close(ws) {
      if (sockets.get(ws.data.agentId) === ws)
        sockets.delete(ws.data.agentId);
      console.log(`[ws] ${ws.data.agentId} disconnected`);
    },
  },
});

console.log(`mini-host listening on http://${BIND}:${PORT} (db ${DB_PATH}, register token ${REGISTER_TOKEN === "dev" ? "\"dev\" — set MINI_HOST_REGISTER_TOKEN" : "set"})`);
console.log(`agent: GINO_HOST_URL=http://${BIND}:${PORT} GINO_REGISTER_TOKEN=... gino-agent`);
