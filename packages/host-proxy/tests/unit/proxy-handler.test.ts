import type { FetchLike, ProxyConfig, WsFactory } from "../../src/index";
import { describe, expect, it } from "bun:test";
import { createProxyHandler, CredentialStore } from "../../src/index";

function makeConfig(overrides: Partial<ProxyConfig> = {}): ProxyConfig {
  return {
    anthropic: { baseUrl: new URL("https://api.anthropic.com"), apiKey: "sk-ant-host" },
    openai: { baseUrl: new URL("https://api.openai.com"), apiKey: "sk-openai-host" },
    codex: {},
    modelMap: {},
    ...overrides,
  };
}

interface Recorded { url: string; init: RequestInit }
function recordingFetch(resp?: Response): { calls: Recorded[]; fetchImpl: FetchLike } {
  const calls: Recorded[] = [];
  const fetchImpl: FetchLike = async (input, init) => {
    calls.push({ url: String(input), init: init ?? {} });
    return resp ?? new Response("UPSTREAM-OK", { status: 200, headers: { "x-upstream": "yes", "content-length": "11" } });
  };
  return { calls, fetchImpl };
}

const noopWs: WsFactory = () => ({ onopen: null, onmessage: null, onerror: null, onclose: null, send: () => {}, close: () => {} });

function reqHeaders(init: RequestInit): Headers {
  return new Headers(init.headers as HeadersInit);
}

describe("M7 proxy/handler — anthropic forward", () => {
  it("forwards to the anthropic origin, injects the host key, strips internal headers, rewrites the alias", async () => {
    const config = makeConfig();
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });

    const req = new Request("http://host/api/llm/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": "sk-ant-proxy-conn",
        "x-internal-token": "conn",
        "x-gino-model": "",
        "content-type": "application/json",
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({ model: "claude-sonnet", max_tokens: 10 }),
    });
    const res = await handler.handle(req, "/v1/messages", "");

    expect(res.status).toBe(200);
    expect(await res.text()).toBe("UPSTREAM-OK");
    expect(res.headers.has("content-length")).toBe(false); // stripped from response

    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe("https://api.anthropic.com/v1/messages");
    const h = reqHeaders(calls[0]!.init);
    expect(h.get("x-api-key")).toBe("sk-ant-host"); // placeholder → real host key
    expect(h.has("x-internal-token")).toBe(false);
    expect(h.has("x-gino-model")).toBe(false);
    expect(h.get("anthropic-version")).toBe("2023-06-01");
    expect(JSON.parse(String(calls[0]!.init.body))).toMatchObject({ model: "claude-sonnet-4-6" }); // alias rewritten
  });

  it("injects the OAuth bearer + beta flag when anthropic is OAuth-mode", async () => {
    const config = makeConfig({ anthropic: { baseUrl: new URL("https://api.anthropic.com"), oauthToken: "oauth-xyz" } });
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    const req = new Request("http://host/api/llm/v1/messages", {
      method: "POST",
      headers: { "x-api-key": "sk-ant-proxy-conn" },
      body: JSON.stringify({ model: "claude-opus" }),
    });
    await handler.handle(req, "/v1/messages", "");
    const h = reqHeaders(calls[0]!.init);
    expect(h.get("authorization")).toBe("Bearer oauth-xyz");
    expect(h.get("anthropic-beta")).toContain("oauth-2025-04-20");
    expect(h.has("x-api-key")).toBe(false);
  });

  it("passes a real Anthropic temp key through unchanged", async () => {
    const config = makeConfig();
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    const req = new Request("http://host/api/llm/v1/messages", {
      method: "POST",
      headers: { "x-api-key": "sk-ant-api03-temp" },
      body: JSON.stringify({ model: "claude-opus" }),
    });
    await handler.handle(req, "/v1/messages", "");
    expect(reqHeaders(calls[0]!.init).get("x-api-key")).toBe("sk-ant-api03-temp");
  });

  it("injects the real OAuth token on the /api/oauth/* exchange path", async () => {
    const config = makeConfig({ anthropic: { baseUrl: new URL("https://api.anthropic.com"), oauthToken: "real-oauth" } });
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    const req = new Request("http://host/api/llm/api/oauth/token", {
      method: "POST",
      headers: { authorization: "Bearer placeholder" },
      body: "{}",
    });
    await handler.handle(req, "/api/oauth/token", "");
    expect(calls[0]!.url).toBe("https://api.anthropic.com/api/oauth/token");
    expect(reqHeaders(calls[0]!.init).get("authorization")).toBe("Bearer real-oauth");
  });
});

describe("M7 proxy/handler — openai forward", () => {
  it("forwards to the openai origin and injects the bearer; preserves the query string", async () => {
    const config = makeConfig();
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    const req = new Request("http://host/api/llm/v1/chat/completions?beta=1", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5" }),
    });
    await handler.handle(req, "/v1/chat/completions", "?beta=1");
    expect(calls[0]!.url).toBe("https://api.openai.com/v1/chat/completions?beta=1");
    expect(reqHeaders(calls[0]!.init).get("authorization")).toBe("Bearer sk-openai-host");
  });

  it("forwards GET /v1/models with no body", async () => {
    const config = makeConfig();
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    await handler.handle(new Request("http://host/api/llm/v1/models", { headers: { "x-internal-token": "c" } }), "/v1/models", "");
    expect(calls[0]!.url).toBe("https://api.openai.com/v1/models");
    expect(calls[0]!.init.body).toBeUndefined();
  });
});

describe("M7 proxy/handler — bridge + errors", () => {
  it("bridges /v1/messages to the OpenAI Responses bridge when the model maps to openai", async () => {
    const config = makeConfig({ modelMap: { "gpt-mini": { provider: "openai", model: "gpt-5-codex-mini" } } });
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl, wsFactory: noopWs });
    const req = new Request("http://host/api/llm/v1/messages", {
      method: "POST",
      headers: { "x-api-key": "sk-ant-proxy-conn" },
      body: JSON.stringify({ model: "gpt-mini", messages: [{ role: "user", content: "hi" }] }),
    });
    const res = await handler.handle(req, "/v1/messages", "");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    expect(res.headers.get("x-bridge-provider")).toBe("openai");
    expect(calls).toHaveLength(0); // bridged via WS, not the HTTP forward
  });

  it("never bridges for a temp-key caller — the key passes through to anthropic instead (HOST-M7-01)", async () => {
    const config = makeConfig({ modelMap: { "gpt-mini": { provider: "openai", model: "gpt-5-codex-mini" } } });
    const { calls, fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl, wsFactory: noopWs });
    const req = new Request("http://host/api/llm/v1/messages", {
      method: "POST",
      headers: { "x-api-key": "sk-ant-api03-temp" },
      body: JSON.stringify({ model: "gpt-mini", messages: [{ role: "user", content: "hi" }] }),
    });
    const res = await handler.handle(req, "/v1/messages", "");
    expect(res.headers.get("x-bridge-provider")).toBeNull();
    expect(calls).toHaveLength(1); // direct forward, not the bridge
    expect(calls[0]!.url).toBe("https://api.anthropic.com/v1/messages");
    const h = reqHeaders(calls[0]!.init);
    expect(h.get("x-api-key")).toBe("sk-ant-api03-temp"); // client key as-is — no host credential injected
    expect(h.get("authorization")).toBeNull();
  });

  it("returns 404 for a path outside the allow-list", async () => {
    const config = makeConfig();
    const { fetchImpl } = recordingFetch();
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    const res = await handler.handle(new Request("http://host/api/llm/v1/embeddings", { method: "POST", body: "{}" }), "/v1/embeddings", "");
    expect(res.status).toBe(404);
  });

  it("returns 502 when the upstream fetch throws", async () => {
    const config = makeConfig();
    const fetchImpl: FetchLike = async () => {
      throw new Error("network down");
    };
    const handler = createProxyHandler({ config, credentials: new CredentialStore(config), fetchImpl });
    const res = await handler.handle(new Request("http://host/api/llm/v1/models", { headers: { "x-internal-token": "c" } }), "/v1/models", "");
    expect(res.status).toBe(502);
  });
});
