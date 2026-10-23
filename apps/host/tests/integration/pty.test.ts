import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { afterAll, beforeAll, expect, it } from "bun:test";
import { websocket } from "hono/bun";
import { createApp } from "@/app";
import { loadConfigStrict } from "@/config";

const host = createHost({ storage: memoryStorage(), logger: noopLogger });
const { agent } = host.agents.enroll({ capabilities: { engines: [] } });
const config = loadConfigStrict({ NODE_ENV: "test", GINO_HTTP_ADMIN_TOKEN: "terminal-admin", GINO_LOG_LEVEL: "error" });
const app = createApp({ config, host });
let server: ReturnType<typeof Bun.serve>;
let base: string;

beforeAll(() => {
  host.wire.attachSocket(agent.id, {
    send(text) {
      const frame = JSON.parse(text);
      if (frame.type === "pty_open") {
        queueMicrotask(() => {
          host.wire.onSocketMessage(agent.id, JSON.stringify({ type: "pty_ready", ptyId: frame.ptyId, data: {} }));
          host.wire.onSocketMessage(agent.id, JSON.stringify({ type: "pty_exit", ptyId: frame.ptyId, data: { exit_code: 0 } }));
        });
      }
    },
    close() {},
  });
  server = Bun.serve({ port: 0, fetch: (req, srv) => app.fetch(req, { server: srv }), websocket });
  base = `http://localhost:${server.port}/api/admin/agents/${agent.id}`;
});
afterAll(() => {
  server.stop(true);
  host.stop();
});

it("upgrades through the standalone app middleware and forwards terminal exit", async () => {
  const response = await fetch(`${base}/pty-ticket`, { method: "POST", headers: { authorization: "Bearer terminal-admin" } });
  expect(response.status).toBe(200);
  const { data } = await response.json() as { data: { ticket: string } };
  const frames: { type: string; data: unknown }[] = [];
  const ws = new WebSocket(`${base.replace("http:", "ws:")}/pty?ticket=${data.ticket}`);
  const finished = new Promise<void>((resolve, reject) => {
    ws.addEventListener("open", () => ws.send(JSON.stringify({ type: "open", data: {} })));
    ws.addEventListener("message", e => frames.push(JSON.parse(String(e.data))));
    ws.addEventListener("close", () => resolve());
    ws.addEventListener("error", () => reject(new Error("Terminal upgrade failed")));
  });
  await finished;
  expect(frames.map(f => f.type)).toEqual(["ready", "exit"]);
  expect(frames[1]!.data).toEqual({ exit_code: 0 });
});

it("rate limits terminal tickets before authorization", async () => {
  let status = 0;
  for (let i = 0; i < 31; i++)
    status = (await fetch(`${base}/pty-ticket`, { method: "POST" })).status;
  expect(status).toBe(429);
});
