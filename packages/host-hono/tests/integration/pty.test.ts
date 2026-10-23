import type { HostEnv } from "../../src/env";
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { afterAll, beforeAll, expect, it } from "bun:test";
import { Hono } from "hono";
import { WebSocket } from "ws";
import { adminRouter } from "../../src/admin";
import { errorHandler } from "../../src/errors";
import { bunWebSocket } from "../../src/ws/bun";

const host = createHost({ storage: memoryStorage(), logger: noopLogger });
const { agent } = host.agents.enroll({ capabilities: { engines: [] } });
const sent: { type: string; ptyId: string; data: Record<string, unknown> }[] = [];
let server: ReturnType<typeof Bun.serve>;
let base: string;
beforeAll(() => {
  host.wire.attachSocket(agent.id, {
    send(text) {
      const frame = JSON.parse(text);
      sent.push(frame);
      if (frame.type === "pty_open") {
        queueMicrotask(() => host.wire.onSocketMessage(agent.id, JSON.stringify({ type: "pty_ready", ptyId: frame.ptyId, data: {} })));
      }
      if (frame.type === "pty_input") {
        host.wire.onSocketMessage(agent.id, JSON.stringify({ type: "pty_output", ptyId: frame.ptyId, data: frame.data }));
      }
    },
    close() {},
  });
  const app = new Hono<HostEnv>();
  const { upgradeWebSocket, websocket } = bunWebSocket();
  app.route("/admin", adminRouter(host, { authorize: c => c.req.header("authorization") === "Bearer test-admin" ? { kind: "admin" } : null, upgradeWebSocket }));
  app.onError(errorHandler);
  server = Bun.serve({ port: 0, fetch: (req, srv) => app.fetch(req, { server: srv }), websocket });
  base = `http://localhost:${server.port}`;
});
afterAll(() => {
  server.stop(true);
  host.stop();
});

async function ticket(origin?: string) {
  const res = await fetch(`${base}/admin/agents/${agent.id}/pty-ticket`, { method: "POST", headers: { authorization: "Bearer test-admin", ...(origin ? { origin } : {}) } });
  expect(res.status).toBe(200);
  return (await res.json() as { data: { ticket: string } }).data.ticket;
}

async function eventually<T>(read: () => T, expected: T) {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    if (read() === expected)
      return;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  expect(read()).toBe(expected);
}

function connect(token: string, origin?: string) {
  const ws = new WebSocket(`${base.replace("http:", "ws:")}/admin/agents/${agent.id}/pty?ticket=${token}`, { headers: origin ? { origin } : {} });
  const messages: { type: string; ptyId: string; data: Record<string, unknown> }[] = [];
  ws.addEventListener("message", e => messages.push(JSON.parse(String(e.data))));
  return { ws, messages, ready: new Promise<void>((resolve, reject) => {
    ws.addEventListener("open", () => resolve(), { once: true });
    ws.addEventListener("error", () => reject(new Error("Upgrade rejected")), { once: true });
  }) };
}

it("requires admin authorization to issue a ticket and never accepts it on other admin routes", async () => {
  const res = await fetch(`${base}/admin/agents/${agent.id}/pty-ticket`, { method: "POST" });
  expect(res.status).toBe(401);
  const token = await ticket();
  expect((await fetch(`${base}/admin/agents?ticket=${token}`)).status).toBe(401);
  expect((await fetch(`${base}/admin/agents/other/pty?ticket=${token}`, { headers: { upgrade: "websocket" } })).status).toBe(401);
});

it("opens with a one-use browser ticket, forwards terminal events, and closes on disconnect", async () => {
  const token = await ticket();
  const { ws, ready, messages } = connect(token);
  await ready;
  ws.send(JSON.stringify({ type: "open", data: { cols: 80, rows: 24 } }));
  await eventually(() => messages[0]?.type, "ready");
  ws.send(JSON.stringify({ type: "input", data: { data: "aGk=" } }));
  await eventually(() => messages.find(m => m.type === "output")?.data.data, "aGk=");
  ws.send(JSON.stringify({ type: "resize", data: { cols: 100, rows: 30 } }));
  await eventually(() => sent.at(-1)?.type, "pty_resize");
  ws.close();
  await eventually(() => sent.at(-1)?.type, "pty_close");
  await expect(connect(token).ready).rejects.toThrow("Upgrade rejected");
});

it("rejects a cross-origin upgrade and closes malformed client messages", async () => {
  const token = await ticket();
  const res = await fetch(`${base}/admin/agents/${agent.id}/pty?ticket=${token}`, { headers: { upgrade: "websocket", origin: "https://unrelated.example" } });
  expect(res.status).toBe(403);
  const { ws, ready } = connect(await ticket());
  await ready;
  const closed = new Promise<void>(resolve => ws.addEventListener("close", () => resolve(), { once: true }));
  ws.send("not-json");
  await closed;
});

it("binds browser tickets to the authenticated origin across TLS termination", async () => {
  const origin = "https://console.example";
  const { ws, ready } = connect(await ticket(origin), origin);
  try {
    await ready;
  }
  finally {
    ws.close();
  }
  await expect(connect(await ticket(origin), "https://unrelated.example").ready).rejects.toThrow("Upgrade rejected");
});
