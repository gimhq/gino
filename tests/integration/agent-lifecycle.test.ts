import type { Storage } from "@/host/ports/storage";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { websocket } from "hono/bun";
import { sweepLiveness } from "@/agents/liveness";
import { getAgent } from "@/agents/registry";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { createLogger } from "@/shared/lib/logger";
import { sqliteStorage } from "@/store/sqlite";

// M3 agent status lifecycle against a LIVE server (HOST-M1M2-05 / HOST-M3-02 /
// HOST-M3-08): a dirty reconnect's superseded-socket close must not clobber the
// live session's `online` status, and `onOpen` must stamp `last_seen` so a
// fresh connection is never insta-swept before its first heartbeat.

const REGISTER_TOKEN = "reg-secret-lifecycle";
const dbPath = resolve(tmpdir(), `gino-lifecycle-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: "chan-secret-lifecycle",
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: ReturnType<typeof createDb>;
let storage: Storage;
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

// ── helpers (spine.test.ts idiom) ────────────────────────────────────────────

type WsCtor = new (url: string, options?: { headers?: Record<string, string> }) => WebSocket;
const WS = WebSocket as unknown as WsCtor;

function openWs(agentId: string, token: string): Promise<WebSocket> {
  const ws = new WS(`${wsBase}/agent/ws/${agentId}`, { headers: { "x-internal-token": token } });
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("ws open timeout")), 3000);
    ws.addEventListener("open", () => {
      clearTimeout(timer);
      resolve(ws);
    });
    ws.addEventListener("error", () => {
      clearTimeout(timer);
      reject(new Error("ws failed to open"));
    });
  });
}

function closedPromise(ws: WebSocket): Promise<void> {
  return new Promise((resolve) => {
    if (ws.readyState === WebSocket.CLOSED) {
      resolve();
      return;
    }
    ws.addEventListener("close", () => resolve());
  });
}

const settle = (ms = 150) => new Promise(r => setTimeout(r, ms));

async function registerAgent(): Promise<{ agentId: string; connectToken: string }> {
  const res = await fetch(`${httpBase}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      registerToken: REGISTER_TOKEN,
      protocolVersion: 2,
      source: "manual",
      capabilities: ["claude"],
    }),
  });
  expect(res.status).toBe(200);
  return await res.json() as { agentId: string; connectToken: string };
}

// ── the lifecycle ────────────────────────────────────────────────────────────

describe("M3 agent status lifecycle over a live WS", () => {
  it("onOpen stamps last_seen so a fresh connection is never insta-swept (HOST-M3-08)", async () => {
    const { agentId, connectToken } = await registerAgent();
    expect(getAgent(storage, agentId)?.lastSeen).toBeNull(); // enrolled, never seen

    const ws = await openWs(agentId, connectToken);
    await settle();

    // No frame has been sent yet — the attach alone must stamp liveness, so
    // the sweep's `last_seen IS NOT NULL` window already covers this agent.
    const agent = getAgent(storage, agentId);
    expect(agent?.status).toBe("online");
    expect(agent?.lastSeen).not.toBeNull();
    expect(sweepLiveness({ storage, sessions: host.sessions })).toBe(0);
    expect(getAgent(storage, agentId)?.status).toBe("online");

    ws.close();
    await closedPromise(ws);
  });

  it("a dirty reconnect's superseded close leaves the agent online (HOST-M1M2-05 / HOST-M3-02)", async () => {
    const { agentId, connectToken } = await registerAgent();

    // Old socket attaches; the host still holds it when the agent redials.
    const staleWs = await openWs(agentId, connectToken);
    await settle();
    expect(getAgent(storage, agentId)?.status).toBe("online");

    // Dirty reconnect: the new attach supersedes (closes) the stale socket,
    // whose late `onClose` lands AFTER the new session went online.
    const staleClosed = closedPromise(staleWs);
    const freshWs = await openWs(agentId, connectToken);
    await staleClosed;
    await settle();

    // The regression: the superseded close used to flip the row `offline`
    // while frames flowed on the fresh socket — it must stay `online`.
    expect(getAgent(storage, agentId)?.status).toBe("online");
    expect(host.sessions.isOnline(agentId)).toBe(true);

    // The CURRENT socket's close still marks the agent offline.
    freshWs.close();
    await closedPromise(freshWs);
    await settle();
    expect(getAgent(storage, agentId)?.status).toBe("offline");
    expect(host.sessions.isOnline(agentId)).toBe(false);
  });
});
