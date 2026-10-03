import type { BridgeSocket, ProxyConfig, WsFactory } from "../../src/index";
import { describe, expect, it } from "bun:test";
import { bridgeClaudeToOpenAI } from "../../src/bridge";
import { CredentialStore } from "../../src/index";

function makeConfig(overrides: Partial<ProxyConfig> = {}): ProxyConfig {
  return {
    anthropic: { baseUrl: new URL("https://api.anthropic.com") },
    openai: { baseUrl: new URL("https://api.openai.com"), apiKey: "sk-openai" },
    codex: {},
    modelMap: {},
    ...overrides,
  };
}

function fakeWs(): { sent: string[]; closed: () => number; factory: WsFactory; socket: () => BridgeSocket } {
  const sent: string[] = [];
  let closes = 0;
  let sock: BridgeSocket | null = null;
  const factory: WsFactory = () => {
    sock = { onopen: null, onmessage: null, onerror: null, onclose: null, send: d => sent.push(d), close: () => closes++ };
    return sock;
  };
  return { sent, closed: () => closes, factory, socket: () => sock! };
}

describe("M7 proxy/bridge — Claude→OpenAI Messages→Responses", () => {
  it("sends a response.create and streams translated Claude SSE back", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const body = JSON.stringify({ model: "gpt-5-codex", system: "Be brief.", messages: [{ role: "user", content: "hi" }] });

    const res = await bridgeClaudeToOpenAI(body, "gpt-5-codex", { store, wsFactory: ws.factory });
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");

    // Upstream connects → response.create is sent with the target model + instructions.
    ws.socket().onopen?.(null);
    expect(ws.sent).toHaveLength(1);
    const createMsg = JSON.parse(ws.sent[0]!) as { type: string; model: string; instructions: string };
    expect(createMsg.type).toBe("response.create");
    expect(createMsg.model).toBe("gpt-5-codex");
    expect(createMsg.instructions).toBe("Be brief.");

    // Drive a minimal Responses stream → expect translated Claude SSE.
    const emit = (obj: unknown) => ws.socket().onmessage?.({ data: JSON.stringify(obj) });
    emit({ type: "response.created", response: { id: "resp_1", model: "gpt-5-codex" } });
    emit({ type: "response.content_part.added" });
    emit({ type: "response.output_text.delta", delta: "Hi there" });
    emit({ type: "response.content_part.done" });
    emit({ type: "response.completed", response: { usage: { input_tokens: 5, output_tokens: 2 } } });

    const text = await res.text();
    expect(text).toContain("event: message_start");
    expect(text).toContain("event: content_block_delta");
    expect(text).toContain("Hi there");
    expect(text).toContain("event: message_stop");
  });

  it("returns 401 when no OpenAI credentials are configured", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig({ openai: { baseUrl: new URL("https://api.openai.com") } }));
    const res = await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", { store, wsFactory: ws.factory });
    expect(res.status).toBe(401);
    expect(await res.json()).toMatchObject({ error: { type: "authentication_error" } });
  });

  it("closes the upstream WS when the client aborts mid-stream", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const ctl = new AbortController();
    const res = await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", { store, wsFactory: ws.factory, signal: ctl.signal });
    ws.socket().onopen?.(null);
    expect(ws.closed()).toBe(0);

    ctl.abort();
    expect(ws.closed()).toBe(1);
    // The SSE stream is finalized, not left hanging.
    await res.text();
  });

  it("closes the upstream WS immediately when the signal is already aborted", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const ctl = new AbortController();
    ctl.abort();
    await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", { store, wsFactory: ws.factory, signal: ctl.signal });
    expect(ws.closed()).toBe(1);
  });

  it("closes the upstream WS when the response stream is canceled", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const res = await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", { store, wsFactory: ws.factory });
    ws.socket().onopen?.(null);
    await res.body!.cancel();

    // The next upstream event hits a rejected write → the bridge tears down the WS.
    ws.socket().onmessage?.({ data: JSON.stringify({ type: "response.created", response: { id: "resp_1", model: "gpt-5" } }) });
    await Bun.sleep(0);
    expect(ws.closed()).toBe(1);
  });

  it("returns 400 on an invalid JSON body", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const res = await bridgeClaudeToOpenAI("not-json", "gpt-5", { store, wsFactory: ws.factory });
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ error: { type: "invalid_request_error" } });
  });

  // Connect/idle watchdogs and SSE backpressure.
  it("errors the stream and closes the WS when the upstream never opens (connect timeout)", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const res = await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", {
      store,
      wsFactory: ws.factory,
      connectTimeoutMs: 10,
    });
    // Never fire onopen — the watchdog must finalize the stream on its own.
    const text = await res.text();
    expect(text).toContain("Upstream connect timeout");
    expect(ws.closed()).toBeGreaterThanOrEqual(1);
  });

  it("errors the stream and closes the WS when the upstream goes silent (idle timeout)", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const res = await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", {
      store,
      wsFactory: ws.factory,
      idleTimeoutMs: 10,
    });
    ws.socket().onopen?.(null);
    // No frames after open — the idle watchdog must finalize the stream.
    const text = await res.text();
    expect(text).toContain("Upstream idle timeout");
    expect(ws.closed()).toBeGreaterThanOrEqual(1);
  });

  it("upstream frames reset the idle watchdog; a normal completion carries no error event", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    const res = await bridgeClaudeToOpenAI(JSON.stringify({ messages: [{ role: "user", content: "hi" }] }), "gpt-5", {
      store,
      wsFactory: ws.factory,
      idleTimeoutMs: 50,
    });
    ws.socket().onopen?.(null);
    const emit = (obj: unknown) => ws.socket().onmessage?.({ data: JSON.stringify(obj) });
    emit({ type: "response.created", response: { id: "resp_1", model: "gpt-5" } });
    await Bun.sleep(30);
    emit({ type: "response.output_text.delta", delta: "tick" });
    await Bun.sleep(30); // total elapsed > idleTimeoutMs, but each frame re-armed the timer
    emit({ type: "response.completed", response: { usage: { input_tokens: 1, output_tokens: 1 } } });
    const text = await res.text();
    expect(text).toContain("event: message_stop");
    expect(text).not.toContain("Upstream idle timeout");
  });

  it("closes the upstream WS when a slow client lets the SSE buffer exceed the cap", async () => {
    const ws = fakeWs();
    const store = new CredentialStore(makeConfig());
    await bridgeClaudeToOpenAI(JSON.stringify({ messages: [] }), "gpt-5", { store, wsFactory: ws.factory });
    ws.socket().onopen?.(null);
    const emit = (obj: unknown) => ws.socket().onmessage?.({ data: JSON.stringify(obj) });
    emit({ type: "response.created", response: { id: "resp_1", model: "gpt-5" } });
    emit({ type: "response.content_part.added" });
    // Nobody reads the response body; flood > 1 MiB of deltas — the bridge
    // must cut the upstream off instead of queueing without bound.
    const bigDelta = "x".repeat(64 * 1024);
    for (let i = 0; i < 20 && ws.closed() === 0; i++)
      emit({ type: "response.output_text.delta", delta: bigDelta });
    expect(ws.closed()).toBe(1);
  });
});
