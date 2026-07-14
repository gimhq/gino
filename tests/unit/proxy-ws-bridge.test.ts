import type { BridgeSocket, ClientSocket, ProxyConfig, WsFactory } from "@/proxy";
import { describe, expect, it } from "bun:test";
import { createCodexRelay, CredentialStore } from "@/proxy";

function makeConfig(overrides: Partial<ProxyConfig> = {}): ProxyConfig {
  return {
    anthropic: { baseUrl: new URL("https://api.anthropic.com") },
    openai: { baseUrl: new URL("https://api.openai.com"), apiKey: "sk-openai" },
    codex: {},
    modelMap: {},
    ...overrides,
  };
}

function fakeClient() {
  const sent: Array<string | ArrayBuffer> = [];
  let closed: { code?: number | undefined; reason?: string | undefined } | null = null;
  const socket: ClientSocket = {
    send: d => sent.push(d),
    close: (code, reason) => { closed = { code, reason }; },
  };
  return { socket, sent, getClosed: () => closed };
}

function fakeUpstream() {
  const sent: string[] = [];
  let sock: BridgeSocket | null = null;
  const factory: WsFactory = () => {
    sock = { onopen: null, onmessage: null, onerror: null, onclose: null, send: d => sent.push(d), close: () => {} };
    return sock;
  };
  return { sent, factory, socket: () => sock! };
}

describe("M7 proxy/ws-bridge — Codex relay", () => {
  it("buffers client frames until the upstream opens, then relays both directions", async () => {
    const client = fakeClient();
    const up = fakeUpstream();
    const relay = createCodexRelay(client.socket, { store: new CredentialStore(makeConfig()), wsFactory: up.factory });

    await relay.start();

    // Frame sent before upstream open is buffered.
    relay.onClientMessage("frame-1");
    expect(up.sent).toEqual([]);

    // Upstream opens → buffer flushes.
    up.socket().onopen?.(null);
    expect(up.sent).toEqual(["frame-1"]);

    // After open, frames relay immediately.
    relay.onClientMessage("frame-2");
    expect(up.sent).toEqual(["frame-1", "frame-2"]);

    // Upstream → client relay.
    up.socket().onmessage?.({ data: "downstream-1" });
    expect(client.sent).toEqual(["downstream-1"]);

    // Upstream close propagates to the client.
    up.socket().onclose?.(null);
    expect(client.getClosed()).not.toBeNull();
  });

  it("closes the client with 4002 when no upstream credentials are available", async () => {
    const client = fakeClient();
    const up = fakeUpstream();
    const store = new CredentialStore(makeConfig({ openai: { baseUrl: new URL("https://api.openai.com") } }));
    const relay = createCodexRelay(client.socket, { store, wsFactory: up.factory });

    await relay.start();
    expect(client.getClosed()).toEqual({ code: 4002, reason: "No upstream credentials" });
  });

  it("closes the client with 1009 when the pre-connect buffer exceeds the cap (HOST-M7-04)", async () => {
    const client = fakeClient();
    let upstreamClosed = false;
    const factory: WsFactory = () => ({
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      send: () => {},
      close: () => {
        upstreamClosed = true;
      },
    });
    const relay = createCodexRelay(client.socket, { store: new CredentialStore(makeConfig()), wsFactory: factory });
    await relay.start();

    // Upstream never opens; flood > 1 MiB of client frames.
    const frame = "x".repeat(512 * 1024);
    relay.onClientMessage(frame);
    relay.onClientMessage(frame);
    expect(client.getClosed()).toBeNull();

    relay.onClientMessage(frame); // crosses the 1 MiB cap
    expect(client.getClosed()).toEqual({ code: 1009, reason: "Pre-connect buffer overflow" });
    expect(upstreamClosed).toBe(true);
  });

  it("tears down the upstream when the client closes", async () => {
    const client = fakeClient();
    let upstreamClosed = false;
    const factory: WsFactory = () => ({
      onopen: null,
      onmessage: null,
      onerror: null,
      onclose: null,
      send: () => {},
      close: () => {
        upstreamClosed = true;
      },
    });
    const relay = createCodexRelay(client.socket, { store: new CredentialStore(makeConfig()), wsFactory: factory });
    await relay.start();
    relay.onClientClose();
    expect(upstreamClosed).toBe(true);
  });
});
