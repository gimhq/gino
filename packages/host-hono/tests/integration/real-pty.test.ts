import type { HostEnv } from "../../src/env";
import { Buffer } from "node:buffer";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { createHost, memoryStorage, noopLogger } from "@gino/host-core";
import { Database } from "bun:sqlite";
import { expect, it } from "bun:test";
import { Hono } from "hono";
import { adminRouter } from "../../src/admin";
import { agentRouter } from "../../src/agent";
import { errorHandler } from "../../src/errors";
import { bunWebSocket } from "../../src/ws/bun";

const binary = process.env.GINO_PTY_TEST_BIN;

it.skipIf(!binary)("real agent and host serve a browser terminal without an engine", async () => {
  const workspace = mkdtempSync(join(tmpdir(), "gino-pty-"));
  const host = createHost({ storage: memoryStorage(), logger: noopLogger, agents: { registerToken: "pty-test-register" } });
  const { upgradeWebSocket, websocket } = bunWebSocket();
  const app = new Hono<HostEnv>();
  app.route("/agent", agentRouter(host, { upgradeWebSocket }));
  app.route("/admin", adminRouter(host, {
    upgradeWebSocket,
    authorize: c => c.req.header("authorization") === "Bearer pty-test-admin" ? { kind: "admin" } : null,
  }));
  app.onError(errorHandler);
  const server = Bun.serve({ port: 0, fetch: (req, srv) => app.fetch(req, { server: srv }), websocket });
  const base = `http://127.0.0.1:${server.port}`;
  const child = Bun.spawn([binary!, "run"], {
    env: { PATH: "/usr/bin:/bin", GINO_HOST_URL: base, GINO_REGISTER_TOKEN: "pty-test-register", GINO_WORKSPACE_DIR: workspace, RUST_LOG: "off" },
    stdout: "ignore",
    stderr: "ignore",
  });
  let ws: WebSocket | undefined;
  try {
    const deadline = Date.now() + 8000;
    while (!host.agents.list().some(a => host.sessions.isOnline(a.id))) {
      if (Date.now() >= deadline)
        throw new Error(`Agent did not connect (exit ${child.exitCode})`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const id = host.agents.list()[0]!.id;
    await expect(host.agents.openPty(id)).rejects.toThrow("disabled");
    host.agents.setRemoteOps(id, { pty: true });
    for (;;) {
      try {
        const probe = await host.agents.openPty(id);
        probe.close();
        break;
      }
      catch (error) {
        if (!(error instanceof Error) || !error.message.includes("disabled") || Date.now() >= deadline)
          throw error;
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }
    const response = await fetch(`${base}/admin/agents/${id}/pty-ticket`, { method: "POST", headers: { authorization: "Bearer pty-test-admin" } });
    expect(response.status).toBe(200);
    const { data } = await response.json() as { data: { ticket: string } };
    ws = new WebSocket(`${base.replace("http:", "ws:")}/admin/agents/${id}/pty?ticket=${data.ticket}`);
    const socket = ws;
    const chunks: Uint8Array[] = [];
    let exit: { exit_code?: number } | undefined;
    await new Promise<void>((resolve, reject) => {
      socket.addEventListener("open", () => socket.send(JSON.stringify({ type: "open", data: {
        argv: ["/bin/sh", "-c", "test -t 0 && test -t 1 || exit 9; read line; stty size; printf 'received:%s' \"$line\"; exit 7"],
      } })));
      socket.addEventListener("message", (event) => {
        const frame = JSON.parse(String(event.data));
        if (frame.type === "ready") {
          socket.send(JSON.stringify({ type: "resize", data: { cols: 100, rows: 33 } }));
          socket.send(JSON.stringify({ type: "input", data: { data: Buffer.from("terminal-e2e\n").toString("base64") } }));
        }
        if (frame.type === "output")
          chunks.push(Buffer.from(frame.data.data, "base64"));
        if (frame.type === "exit")
          exit = frame.data;
      });
      socket.addEventListener("close", () => resolve());
      socket.addEventListener("error", () => reject(new Error("Terminal socket failed")));
    });
    const output = Buffer.concat(chunks).toString();
    expect(output).toContain("33 100");
    expect(output).toContain("received:terminal-e2e");
    expect(exit?.exit_code).toBe(7);
    const running = await host.agents.openPty(id, { argv: ["/bin/sh", "-c", "exec sleep 60"] });
    host.agents.setRemoteOps(id, { pty: false });
    expect((await running.exited).error?.code).toBe("disabled");
    const db = new Database(join(workspace, "agent.db"), { readonly: true });
    try {
      for (const table of ["inbox", "outbox"])
        expect(db.query(`SELECT count(*) AS count FROM ${table} WHERE type LIKE 'pty_%'`).get()).toEqual({ count: 0 });
      expect(db.query("SELECT count(*) AS count FROM threads").get()).toEqual({ count: 0 });
    }
    finally {
      db.close();
    }
  }
  finally {
    ws?.close();
    child.kill("SIGTERM");
    await child.exited;
    server.stop(true);
    host.stop();
    rmSync(workspace, { recursive: true, force: true });
  }
}, 15000);
