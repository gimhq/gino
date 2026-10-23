import type { WsLike } from "../../src/agents/sessions";
import { Buffer } from "node:buffer";
import { describe, expect, it } from "bun:test";
import { RemoteOps } from "../../src/agents/remote";
import { SessionRegistry } from "../../src/agents/sessions";
import { createHost, memoryStorage, noopLogger } from "../../src/index";

function setup(timeoutMs = 100) {
  const sessions = new SessionRegistry();
  const remote = new RemoteOps(sessions, memoryStorage().blobs, { timeoutMs });
  sessions.onDrop = id => remote.dropAgent(id);
  const sent: { type: string; ptyId: string; msgId?: number; data: Record<string, unknown> }[] = [];
  const ws: WsLike = { send: text => sent.push(JSON.parse(text)), close() {} };
  sessions.attach("a", ws);
  return { remote, sessions, sent, ws };
}

async function opened(s: ReturnType<typeof setup>) {
  const pending = s.remote.openPty("a", { cols: 80, rows: 24 });
  const id = s.sent.at(-1)!.ptyId;
  s.remote.receivePty("a", "pty_ready", id, {});
  return pending;
}

describe("live PTY sessions", () => {
  it("waits for ready, streams bytes in order and preserves output before exit", async () => {
    const s = setup();
    const pty = await opened(s);
    expect(s.sent[0]).toMatchObject({ type: "pty_open", data: { cols: 80, rows: 24 } });
    expect(s.sent[0]!.msgId).toBeUndefined();
    const bytes = new Uint8Array([0, 255, 27, 91, 109]);
    pty.write(bytes);
    pty.resize(120, 40);
    expect(s.sent[1]).toMatchObject({ type: "pty_input", ptyId: pty.id, data: { data: Buffer.from(bytes).toString("base64") } });
    expect(s.sent[2]).toMatchObject({ type: "pty_resize", data: { cols: 120, rows: 40 } });
    s.remote.receivePty("a", "pty_output", pty.id, { data: Buffer.from(bytes).toString("base64") });
    s.remote.receivePty("a", "pty_exit", pty.id, { exit_code: 7 });
    const reader = pty.output.getReader();
    expect((await reader.read()).value).toEqual(bytes);
    expect((await reader.read()).done).toBe(true);
    expect(await pty.exited).toEqual({ exit_code: 7 });
    expect(() => pty.write(bytes)).toThrow();
  });

  it("ignores other agents and unknown sessions; refuses offline agents and times out old peers", async () => {
    const s = setup(15);
    await expect(s.remote.openPty("offline", {})).rejects.toMatchObject({ code: "AGENT_OFFLINE" });
    const pending = s.remote.openPty("a", {});
    const id = s.sent[0]!.ptyId;
    s.remote.receivePty("other", "pty_ready", id, {});
    s.remote.receivePty("a", "pty_ready", "unknown", {});
    await expect(pending).rejects.toMatchObject({ code: "AGENT_TIMEOUT" });
    expect(s.sent.at(-1)).toMatchObject({ type: "pty_close", ptyId: id });
  });

  it("reports spawn refusal without exposing a ready session", async () => {
    const s = setup();
    const pending = s.remote.openPty("a", {});
    s.remote.receivePty("a", "pty_exit", s.sent[0]!.ptyId, { error: { code: "disabled", message: "disabled" } });
    await expect(pending).rejects.toThrow("disabled");
  });

  it("cancels a pending open and ignores its late ready response", async () => {
    const s = setup();
    const abort = new AbortController();
    const pending = s.remote.openPty("a", {}, abort.signal);
    const id = s.sent[0]!.ptyId;
    abort.abort();
    await expect(pending).rejects.toThrow("cancelled");
    expect(s.sent.at(-1)).toMatchObject({ type: "pty_close", ptyId: id });
    s.remote.receivePty("a", "pty_ready", id, {});
    expect(s.sent).toHaveLength(2);
  });

  it("bounds per-agent sessions and sends close even when the transport buffer is full", async () => {
    const s = setup();
    const terminals: Awaited<ReturnType<typeof opened>>[] = [];
    for (let i = 0; i < 8; i++)
      terminals.push(await opened(s));
    await expect(s.remote.openPty("a", {})).rejects.toMatchObject({ code: "PTY_LIMIT" });
    s.ws.raw = { bufferedAmount: 256 * 1024 };
    expect(() => terminals[0]!.write(new Uint8Array([1]))).toThrow("disconnected");
    expect(s.sent.at(-1)).toMatchObject({ type: "pty_close", ptyId: terminals[0]!.id });
    for (const terminal of terminals)
      terminal.close();
  });

  it("ends sessions on detach, replacement, removal and host shutdown", async () => {
    for (const drop of [
      (s: ReturnType<typeof setup>) => s.sessions.detach("a", s.ws),
      (s: ReturnType<typeof setup>) => s.sessions.attach("a", { send() {}, close() {} }),
      (s: ReturnType<typeof setup>) => s.sessions.closeAgent("a", 1001, "removed"),
      (s: ReturnType<typeof setup>) => s.sessions.closeAll(1001, "shutdown"),
    ]) {
      const s = setup();
      const pty = await opened(s);
      drop(s);
      expect((await pty.exited).error?.code).toBe("agent_disconnected");
      expect((await pty.output.getReader().read()).done).toBe(true);
    }
  });

  it("closes on consumer cancellation and rejects oversized input or invalid dimensions", async () => {
    const s = setup();
    const pty = await opened(s);
    expect(() => pty.resize(0, 24)).toThrow();
    expect(() => pty.write(new Uint8Array(16385))).toThrow();
    await pty.output.cancel();
    expect(s.sent.at(-1)).toMatchObject({ type: "pty_close", ptyId: pty.id });
    pty.close();
    expect(s.sent.filter(f => f.type === "pty_close")).toHaveLength(1);
    expect((await pty.exited).error?.code).toBe("closed");
  });

  it("terminates malformed output and bounded-buffer overflow", async () => {
    for (const malformed of [true, false]) {
      const s = setup();
      const pty = await opened(s);
      if (malformed) {
        s.remote.receivePty("a", "pty_output", pty.id, { data: "!invalid" });
      }
      else {
        for (let i = 0; i < 20; i++)
          s.remote.receivePty("a", "pty_output", pty.id, { data: Buffer.alloc(16384).toString("base64") });
      }
      expect((await pty.exited).error?.code).toBe(malformed ? "invalid" : "slow_consumer");
      expect(s.sent.at(-1)!.type).toBe("pty_close");
    }
  });
});

it("merges independently configured file and PTY flags", () => {
  const host = createHost({ storage: memoryStorage(), logger: noopLogger });
  try {
    const { agent } = host.agents.enroll({ capabilities: { engines: [] } });
    expect(host.agents.setRemoteOps(agent.id, { fs: true })).toEqual({ fs: true, pty: false });
    expect(host.agents.setRemoteOps(agent.id, { pty: true })).toEqual({ fs: true, pty: true });
    expect(host.agents.setRemoteOps(agent.id, { fs: false })).toEqual({ fs: false, pty: true });
  }
  finally {
    host.stop();
  }
});
