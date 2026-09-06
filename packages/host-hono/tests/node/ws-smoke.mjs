// Node WS smoke — `@hono/node-server` + `nodeWebSocket` + `agentRouter` over
// the memory storage, consumed the way a Node embedder does (through dist/).
// register (HTTP) → WS with the connect token → online → close → offline.
import process from "node:process";
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { agentRouter, errorHandler } from "@gino/host-hono";
import { nodeWebSocket } from "@gino/host-hono/node";
import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { WebSocket } from "ws";

const REGISTER_TOKEN = "reg-secret-node";
const settle = (ms = 150) => new Promise(r => setTimeout(r, ms));

function assert(cond, msg) {
  if (!cond) {
    console.error(`ws-smoke: FAIL — ${msg}`);
    process.exit(1);
  }
}

async function main() {
  const storage = memoryStorage();
  const host = createHost({ storage, logger: noopLogger, agents: { registerToken: REGISTER_TOKEN } });

  const app = new Hono();
  const { upgradeWebSocket, injectWebSocket } = nodeWebSocket(app);
  app.route("/agent", agentRouter(host, { upgradeWebSocket }));
  app.onError(errorHandler);

  const server = serve({ fetch: app.fetch, port: 0, hostname: "127.0.0.1" });
  injectWebSocket(server);
  await new Promise(resolve => server.once("listening", resolve));
  const { port } = server.address();
  const httpBase = `http://127.0.0.1:${port}`;

  const timeout = setTimeout(() => {
    console.error("ws-smoke: FAIL — timed out");
    process.exit(1);
  }, 10_000);

  const res = await fetch(`${httpBase}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
  assert(res.status === 200, `register → ${res.status}`);
  const { agentId, connectToken } = await res.json();
  assert(host.agents.get(agentId)?.status === "enrolled", "enrolled agent is `enrolled`");

  const denied = await fetch(`${httpBase}/agent/ws/${agentId}`);
  assert(denied.status === 401, `WS endpoint without a token → ${denied.status}`);

  const ws = new WebSocket(`ws://127.0.0.1:${port}/agent/ws/${agentId}`, { headers: { "x-internal-token": connectToken } });
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  await settle();
  assert(host.agents.get(agentId)?.status === "online", "agent is online after the WS attach");
  assert(host.sessions.isOnline(agentId), "session registry reports the agent online");

  ws.send(JSON.stringify({ type: "heartbeat", data: { activeThreads: 1 } }));
  await settle();
  assert(host.agents.get(agentId)?.activeThreads === 1, "heartbeat updated activeThreads");

  const closed = new Promise(resolve => ws.once("close", resolve));
  ws.close();
  await closed;
  await settle();
  assert(host.agents.get(agentId)?.status === "offline", "agent is offline after the WS close");
  assert(!host.sessions.isOnline(agentId), "session registry reports the agent offline");

  clearTimeout(timeout);
  host.stop();
  server.close();
  console.log("ws-smoke: OK (node register → ws online → heartbeat → close → offline)");
  process.exit(0);
}

main().catch((err) => {
  console.error("ws-smoke: FAIL —", err);
  process.exit(1);
});
