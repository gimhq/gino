import type { BridgeSocket, WsFactory } from "@/proxy";
import type { AppEnv } from "@/shared/lib/types";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { createHost } from "@gino/host-core";
import { hashToken } from "@gino/host-core/domain";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { agents } from "@gino/host-store-sqlite/schema";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { websocket } from "hono/bun";
import { loadConfigStrict } from "@/config";
import { llmRoutes } from "@/edge/llm";
import { createLogger } from "@/shared/lib/logger";
import { errorHandler } from "@/shared/middleware/error-handler";

// D-B5w — the `/llm/v1/responses` Codex WS passthrough, proven end-to-end against
// a LIVE Bun server with a real client WebSocket. The HTTP→WS upgrade reuses the
// host's shared `hono/bun` `upgradeWebSocket`/`websocket` (B1b's createBunWebSocket
// instance); the UPSTREAM socket is a MOCKED factory injected into llmRoutes, so
// the bidirectional relay + buffer-until-open + credential injection are exercised
// with NO real network.

const CONNECT_TOKEN = "connect-secret-ws";
const UPSTREAM_URL = "wss://fake-codex.test/responses";
const dbPath = resolve(tmpdir(), `gino-proxy-ws-${process.pid}-${Date.now()}.db`);

const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: "reg",
  GINO_HTTP_CHANNEL_TOKEN: "chan",
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
  GINO_PROXY_OPENAI_API_KEY: "sk-openai-host-real",
});

// ── mocked upstream WS factory ────────────────────────────────────────────────

interface FakeUpstream {
  url: string;
  headers: Record<string, string>;
  sent: string[];
  socket: BridgeSocket;
  closed: boolean;
}

const upstreams: FakeUpstream[] = [];

// Records every upstream dial and auto-fires `onopen` on the next microtask (the
// relay attaches its handlers synchronously right after this returns), which
// flushes any client frames buffered before the upstream was ready.
const fakeFactory: WsFactory = (url, opts) => {
  const rec: FakeUpstream = {
    url,
    headers: opts.headers,
    sent: [],
    closed: false,
    socket: {
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      send: d => rec.sent.push(d),
      close: () => { rec.closed = true; },
    },
  };
  upstreams.push(rec);
  queueMicrotask(() => rec.socket.onopen?.(null));
  return rec.socket;
};

let db: ReturnType<typeof createDb>;
let server: ReturnType<typeof Bun.serve>;
let wsBase: string;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  const logger = createLogger(config);
  const host = createHost({ storage: sqliteStorage(db), logger });

  // Minimal app: per-request context + the /llm router with the FAKE upstream
  // factory injected. Production wires `llmRoutes()` (real WebSocket) — only the
  // test overrides the upstream so the relay touches no network.
  const api = new Hono<AppEnv>();
  api.use("*", (c, next) => {
    c.set("host", host);
    c.set("config", config);
    c.set("logger", logger);
    return next();
  });
  api.route("/api/llm", llmRoutes({ wsFactory: fakeFactory, upstreamUrl: UPSTREAM_URL }));
  api.onError(errorHandler);

  server = Bun.serve({
    port: 0,
    // Thread `server` into env so the shared hono/bun adapter can upgrade.
    fetch: (req, srv) => api.fetch(req, { IP: srv.requestIP(req), server: srv }),
    websocket,
  });
  wsBase = `ws://localhost:${server.port}/api/llm`;

  // Seed an enrolled agent whose connect token authorises the proxy upgrade.
  db.insert(agents).values({
    id: "agent-ws-1",
    source: "manual",
    access: "public",
    connectTokenHash: hashToken(CONNECT_TOKEN),
    capabilities: { engines: ["claude"] },
    status: "online",
  }).run();
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

// ── helpers ───────────────────────────────────────────────────────────────────

// Bun's WebSocket client accepts a `{ headers }` option (a Bun extension); the
// DOM lib's constructor type only knows `protocols`, so reach the header form
// through a narrow cast (no `any`).
type WsCtor = new (url: string, options?: { headers?: Record<string, string> }) => WebSocket;
const WS = WebSocket as unknown as WsCtor;

interface OpenWs {
  ws: WebSocket;
  received: string[];
}

function openWs(headers?: Record<string, string>): Promise<OpenWs> {
  const ws = new WS(`${wsBase}/v1/responses`, headers ? { headers } : undefined);
  const received: string[] = [];
  ws.addEventListener("message", (e: MessageEvent) => received.push(String(e.data)));
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

/** True if the handshake is rejected (the upgrade never opens) — the 401 path. */
function wsRejected(headers?: Record<string, string>): Promise<boolean> {
  const ws = new WS(`${wsBase}/v1/responses`, headers ? { headers } : undefined);
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

async function waitFor<T>(get: () => T | undefined, label: string, timeoutMs = 2500): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const v = get();
    if (v !== undefined)
      return v;
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

// ── the relay ──────────────────────────────────────────────────────────────────

describe("D-B5w /llm/v1/responses — live WS upgrade → createCodexRelay", () => {
  it("upgrades, injects the host upstream credential, and relays both directions", async () => {
    upstreams.length = 0;
    const conn = await openWs({ "x-internal-token": CONNECT_TOKEN });

    // On open, the relay dials the (mocked) upstream with the host's OpenAI
    // credential injected — the engine never sees it.
    const up = await waitFor(() => upstreams[0], "upstream dial");
    expect(up.url).toBe(UPSTREAM_URL);
    expect(up.headers.authorization).toBe("Bearer sk-openai-host-real");

    // client → upstream (buffered until the upstream open microtask flushes).
    conn.ws.send("client-frame-1");
    await waitFor(() => (up.sent.includes("client-frame-1") ? true : undefined), "client→upstream relay");

    // upstream → client.
    up.socket.onmessage?.({ data: "upstream-frame-1" });
    await waitFor(() => (conn.received.includes("upstream-frame-1") ? true : undefined), "upstream→client relay");

    // client close tears down the upstream.
    await closeWs(conn.ws);
    await waitFor(() => (up.closed ? true : undefined), "upstream teardown on client close");
  });

  it("rejects an unauthenticated upgrade before any socket opens (cred gate)", async () => {
    upstreams.length = 0;
    expect(await wsRejected()).toBe(true); // no credential
    expect(await wsRejected({ "x-internal-token": "wrong" })).toBe(true); // bad credential
    // The gate runs before the upgrade → the relay never dialled the upstream.
    expect(upstreams).toHaveLength(0);
  });
});
