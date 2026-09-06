import type { HostEnv } from "../../src/env";
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { agentRouter } from "../../src/agent";
import { errorHandler } from "../../src/errors";
import { bunWebSocket } from "../../src/ws/bun";

// The `/agent/*` router over the Bun upgrade against a LIVE server with the
// memory storage: register → connect token → WS upgrade → attach (online) →
// heartbeat → close → detach (offline). The lifecycle ordering is the wire's
// (host-core `host/wire/socket`); this proves the adapter drives it 1:1 with
// no app config in the loop.

const REGISTER_TOKEN = "reg-secret-router";

const storage = memoryStorage();
const host = createHost({ storage, logger: noopLogger, agents: { registerToken: REGISTER_TOKEN } });

let server: ReturnType<typeof Bun.serve>;
let httpBase: string;
let wsBase: string;

beforeAll(() => {
  const { upgradeWebSocket, websocket } = bunWebSocket();
  const app = new Hono<HostEnv>();
  app.route("/agent", agentRouter(host, { upgradeWebSocket }));
  app.onError(errorHandler);
  server = Bun.serve({
    port: 0,
    fetch: (req, srv) => app.fetch(req, { IP: srv.requestIP(req), server: srv }),
    websocket,
  });
  httpBase = `http://localhost:${server.port}`;
  wsBase = `ws://localhost:${server.port}`;
});

afterAll(() => {
  server.stop(true);
  host.stop();
});

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
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
  expect(res.status).toBe(200);
  return await res.json() as { agentId: string; connectToken: string };
}

describe("agentRouter over the Bun upgrade (memory storage)", () => {
  it("rejects a bad register token with the protocol §3 envelope", async () => {
    const res = await fetch(`${httpBase}/agent/register`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ registerToken: "wrong", protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
    });
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ register_error: { reason: "invalid_register_token" } });
  });

  it("401 on the WS endpoint without a connect token; 403 for another agent's token", async () => {
    const { agentId, connectToken } = await registerAgent();
    expect((await fetch(`${httpBase}/agent/ws/${agentId}`)).status).toBe(401);
    expect((await fetch(`${httpBase}/agent/config/some-other-agent`, { headers: { "x-internal-token": connectToken } })).status).toBe(403);
    const cfg = await fetch(`${httpBase}/agent/config/${agentId}`, { headers: { "x-internal-token": connectToken } });
    expect(cfg.status).toBe(200);
  });

  it("register → WS attach (online) → heartbeat → close (offline)", async () => {
    const { agentId, connectToken } = await registerAgent();
    expect(host.agents.get(agentId)?.status).toBe("enrolled");

    const ws = await openWs(agentId, connectToken);
    await settle();
    expect(host.agents.get(agentId)?.status).toBe("online");
    expect(host.agents.get(agentId)?.lastSeen).not.toBeNull();
    expect(host.sessions.isOnline(agentId)).toBe(true);

    ws.send(JSON.stringify({ type: "heartbeat", data: { activeThreads: 2 } }));
    await settle();
    expect(host.agents.get(agentId)?.activeThreads).toBe(2);
    expect(host.agents.get(agentId)?.status).toBe("online");

    ws.close();
    await closedPromise(ws);
    await settle();
    expect(host.agents.get(agentId)?.status).toBe("offline");
    expect(host.sessions.isOnline(agentId)).toBe(false);
  });

  it("a dirty reconnect's superseded close leaves the agent online (current-socket identity via raw)", async () => {
    const { agentId, connectToken } = await registerAgent();
    const staleWs = await openWs(agentId, connectToken);
    await settle();
    const staleClosed = closedPromise(staleWs);
    const freshWs = await openWs(agentId, connectToken);
    await staleClosed;
    await settle();
    expect(host.agents.get(agentId)?.status).toBe("online");
    expect(host.sessions.isOnline(agentId)).toBe(true);

    freshWs.close();
    await closedPromise(freshWs);
    await settle();
    expect(host.agents.get(agentId)?.status).toBe("offline");
  });
});
