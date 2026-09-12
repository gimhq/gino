import type { Host } from "@gino/host-core";
import type { ServedHost } from "../../src/index";
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { afterEach, describe, expect, it } from "bun:test";
import { serveHostBun } from "../../src/bun";
import { serveHostNode } from "../../src/node";

// The two listeners over one scenario: register (HTTP) → WS with the connect
// token → online → heartbeat → close → offline, plus the auth and routing
// edges the framework-free handler owns (401/403/404, fallback, basePath).

const REGISTER_TOKEN = "reg-secret";
const settle = (ms = 100) => new Promise(r => setTimeout(r, ms));

function newHost(): Host {
  return createHost({ storage: memoryStorage(), logger: noopLogger, agents: { registerToken: REGISTER_TOKEN } });
}

async function register(base: string, token = REGISTER_TOKEN): Promise<Response> {
  return fetch(`${base}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: token, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
}

function openSocket(url: string, token: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers: { "x-internal-token": token } } as unknown as string[]);
    ws.onopen = () => resolve(ws);
    ws.onerror = () => reject(new Error("socket refused"));
  });
}

type Adapter = (host: Host, fallback?: (req: Request) => Response) => Promise<ServedHost>;

const adapters: [string, Adapter][] = [
  ["bun", async (host, fallback) => serveHostBun(host, fallback ? { fallback } : {})],
  ["node", (host, fallback) => serveHostNode(host, fallback ? { fallback } : {})],
];

for (const [name, serve] of adapters) {
  describe(`serveHost (${name})`, () => {
    let host: Host;
    let served: ServedHost;

    afterEach(async () => {
      await served.stop();
      host.stop();
    });

    it("registers, attaches the socket, tracks heartbeat and detaches on close", async () => {
      host = newHost();
      served = await serve(host);

      const res = await register(served.url);
      expect(res.status).toBe(200);
      const { agentId, connectToken } = await res.json() as { agentId: string; connectToken: string };
      expect(host.agents.get(agentId)?.status).toBe("enrolled");

      const config = await fetch(`${served.url}/agent/config/${agentId}`, { headers: { "x-internal-token": connectToken } });
      expect(config.status).toBe(200);
      expect((await config.json() as { agentId: string }).agentId).toBe(agentId);

      const ws = await openSocket(`${served.url.replace(/^http/, "ws")}/agent/ws/${agentId}`, connectToken);
      await settle();
      expect(host.sessions.isOnline(agentId)).toBe(true);
      expect(host.agents.get(agentId)?.status).toBe("online");

      ws.send(JSON.stringify({ type: "heartbeat", data: { activeThreads: 2 } }));
      await settle();
      expect(host.agents.get(agentId)?.activeThreads).toBe(2);

      const closed = new Promise(resolve => (ws.onclose = resolve));
      ws.close();
      await closed;
      await settle();
      expect(host.sessions.isOnline(agentId)).toBe(false);
      expect(host.agents.get(agentId)?.status).toBe("offline");
    });

    it("rejects a bad register token, a missing connect token and a cross-agent path", async () => {
      host = newHost();
      served = await serve(host);

      expect((await register(served.url, "wrong")).status).toBe(401);

      const { agentId, connectToken } = await (await register(served.url)).json() as { agentId: string; connectToken: string };
      const other = (await (await register(served.url)).json() as { agentId: string }).agentId;

      expect((await fetch(`${served.url}/agent/config/${agentId}`)).status).toBe(401);
      expect((await fetch(`${served.url}/agent/config/${other}`, { headers: { "x-internal-token": connectToken } })).status).toBe(403);
      expect((await fetch(`${served.url}/agent/ws/${agentId}`)).status).toBe(401);
      expect((await fetch(`${served.url}/agent/ws/${other}`, { headers: { "x-internal-token": connectToken } })).status).toBe(403);
      expect((await fetch(`${served.url}/agent/ws/${agentId}`, { headers: { "x-internal-token": connectToken } })).status).toBe(426);
      await expect(openSocket(`${served.url.replace(/^http/, "ws")}/agent/ws/${agentId}`, "nope")).rejects.toThrow();
      expect(host.sessions.isOnline(agentId)).toBe(false);
    });

    it("404s an unknown agent path and hands everything else to the fallback", async () => {
      host = newHost();
      served = await serve(host, req => Response.json({ echo: new URL(req.url).pathname }));

      const unknown = await fetch(`${served.url}/agent/nope`);
      expect(unknown.status).toBe(404);
      expect((await unknown.json() as { error: { code: string } }).error.code).toBe("NOT_FOUND");

      const own = await fetch(`${served.url}/api/tasks`);
      expect(own.status).toBe(200);
      expect(await own.json()).toEqual({ echo: "/api/tasks" });
    });

    it("defaults the fallback to the 404 envelope", async () => {
      host = newHost();
      served = await serve(host);
      expect((await fetch(`${served.url}/anything`)).status).toBe(404);
    });
  });
}

describe("createAgentHandler basePath", () => {
  it("mounts the data plane under a custom prefix", async () => {
    const host = newHost();
    const served = serveHostBun(host, { basePath: "/api/agent" });
    try {
      expect((await fetch(`${served.url}/agent/register`, { method: "POST" })).status).toBe(404);
      const res = await fetch(`${served.url}/api/agent/register`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
      });
      expect(res.status).toBe(200);
    }
    finally {
      await served.stop();
      host.stop();
    }
  });
});
