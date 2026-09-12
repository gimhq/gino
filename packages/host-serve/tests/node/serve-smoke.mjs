// Node smoke — `serveHostNode` over the memory storage, consumed the way a Node
// embedder does (through dist/): register (HTTP) → WS with the connect token →
// online → heartbeat → close → offline, plus a fallback route on the same port.
import process from "node:process";
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { serveHostNode } from "@gino/host-serve/node";
import { WebSocket } from "ws";

const REGISTER_TOKEN = "reg-secret-node";
const settle = (ms = 150) => new Promise(r => setTimeout(r, ms));

function assert(cond, msg) {
  if (!cond) {
    console.error(`serve-smoke: FAIL — ${msg}`);
    process.exit(1);
  }
}

async function main() {
  const host = createHost({ storage: memoryStorage(), logger: noopLogger, agents: { registerToken: REGISTER_TOKEN } });
  const served = await serveHostNode(host, { fallback: () => Response.json({ ok: true }) });

  const timeout = setTimeout(() => {
    console.error("serve-smoke: FAIL — timed out");
    process.exit(1);
  }, 10_000);

  const res = await fetch(`${served.url}/agent/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
  assert(res.status === 200, `register → ${res.status}`);
  const { agentId, connectToken } = await res.json();

  assert((await fetch(`${served.url}/agent/ws/${agentId}`)).status === 401, "WS endpoint without a token → 401");
  assert((await (await fetch(`${served.url}/health`)).json()).ok === true, "fallback route served on the same port");

  const ws = new WebSocket(`${served.url.replace(/^http/, "ws")}/agent/ws/${agentId}`, { headers: { "x-internal-token": connectToken } });
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  await settle();
  assert(host.sessions.isOnline(agentId), "agent online after the WS attach");

  ws.send(JSON.stringify({ type: "heartbeat", data: { activeThreads: 1 } }));
  await settle();
  assert(host.agents.get(agentId)?.activeThreads === 1, "heartbeat updated activeThreads");

  const closed = new Promise(resolve => ws.once("close", resolve));
  ws.close();
  await closed;
  await settle();
  assert(!host.sessions.isOnline(agentId), "agent offline after the WS close");

  clearTimeout(timeout);
  host.stop();
  await served.stop();
  console.log("serve-smoke: OK (node register → ws online → heartbeat → close → offline)");
  process.exit(0);
}

main().catch((err) => {
  console.error("serve-smoke: FAIL —", err);
  process.exit(1);
});
