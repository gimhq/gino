import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { hashToken } from "@gino/host-core/domain";
import { createDb } from "@gino/host-store-sqlite";
import { agents } from "@gino/host-store-sqlite/schema";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

const CONNECT_TOKEN = "connect-secret-proxy";
const dbPath = resolve(tmpdir(), `gino-proxy-${process.pid}-${Date.now()}.db`);

const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: "reg",
  GINO_HTTP_CHANNEL_TOKEN: "chan",
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
  GINO_PROXY_ANTHROPIC_API_KEY: "sk-ant-host-real",
  GINO_PROXY_OPENAI_API_KEY: "sk-openai-host-real",
});

let db: ReturnType<typeof createDb>;
let app: ReturnType<typeof createApp>;
const realFetch = globalThis.fetch;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  app = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
  // Seed an enrolled agent whose connect token authorizes the proxy.
  db.insert(agents).values({
    id: "agent-proxy-1",
    source: "manual",
    access: "public",
    connectTokenHash: hashToken(CONNECT_TOKEN),
    capabilities: { engines: ["claude"] },
    status: "online",
  }).run();
});

afterEach(() => {
  globalThis.fetch = realFetch;
});

afterAll(() => {
  globalThis.fetch = realFetch;
  db.close();
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      rmSync(`${dbPath}${suffix}`, { force: true });
    }
    catch {}
  }
});

// Replace global fetch with a recorder so the proxy never hits the network.
function mockUpstream(): { calls: Array<{ url: string; headers: Headers; body: string }> } {
  const calls: Array<{ url: string; headers: Headers; body: string }> = [];
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input);
    const headers = new Headers(init?.headers as HeadersInit);
    const body = init?.body ? String(init.body) : "";
    calls.push({ url, headers, body });
    return new Response(JSON.stringify({ ok: true, echoUrl: url }), { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  return { calls };
}

const proxyUrl = (path: string, init: RequestInit = {}) => new Request(`http://local/api/llm${path}`, init);

describe("M7 /llm/* — mount + auth", () => {
  it("401 without a proxy credential", async () => {
    const res = await app.fetch(proxyUrl("/v1/models"));
    expect(res.status).toBe(401);
  });

  it("401 with an unknown connect token", async () => {
    const res = await app.fetch(proxyUrl("/v1/models", { headers: { "x-internal-token": "wrong" } }));
    expect(res.status).toBe(401);
  });

  it("401: a forged sk-ant- key does not authenticate host-credential paths (HOST-M7-01)", async () => {
    const forged = { "x-api-key": "sk-ant-forged-anything", "content-type": "application/json" };
    for (const [path, init] of [
      ["/v1/chat/completions", { method: "POST", headers: forged, body: JSON.stringify({ model: "gpt-5" }) }],
      ["/v1/responses", { headers: forged }],
      ["/api/oauth/token", { method: "POST", headers: forged, body: "{}" }],
    ] as const) {
      const res = await app.fetch(proxyUrl(path, init));
      expect(res.status).toBe(401);
    }
  });

  it("a temp key on /v1/messages stays on the anthropic pass-through even when the model maps to openai", async () => {
    const up = mockUpstream();
    const res = await app.fetch(proxyUrl("/v1/messages", {
      method: "POST",
      headers: { "x-api-key": "sk-ant-forged-anything", "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5", messages: [] }),
    }));
    expect(res.status).toBe(200); // mocked upstream; real Anthropic would reject the key
    expect(up.calls).toHaveLength(1);
    expect(up.calls[0]!.url).toBe("https://api.anthropic.com/v1/messages"); // NOT bridged to OpenAI
    expect(up.calls[0]!.headers.get("x-api-key")).toBe("sk-ant-forged-anything"); // no host credential injected
    expect(up.calls[0]!.headers.get("authorization")).toBeNull();
  });

  it("unknown /llm subpath → 404 (no catch-all), even with a valid credential", async () => {
    const res = await app.fetch(proxyUrl("/v1/embeddings", { method: "POST", headers: { "x-internal-token": CONNECT_TOKEN }, body: "{}" }));
    expect(res.status).toBe(404);
  });
});

describe("M7 /llm/* — prefix strip + provider routing (mocked upstream)", () => {
  it("forwards anthropic /v1/messages: strips /llm, injects host key, rewrites the alias", async () => {
    const up = mockUpstream();
    const res = await app.fetch(proxyUrl("/v1/messages", {
      method: "POST",
      headers: { "x-api-key": `sk-ant-proxy-${CONNECT_TOKEN}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "claude-sonnet", max_tokens: 5 }),
    }));
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ok: true });

    expect(up.calls).toHaveLength(1);
    expect(up.calls[0]!.url).toBe("https://api.anthropic.com/v1/messages"); // /llm + /api prefix stripped
    expect(up.calls[0]!.headers.get("x-api-key")).toBe("sk-ant-host-real");
    expect(up.calls[0]!.headers.has("x-internal-token")).toBe(false);
    expect(JSON.parse(up.calls[0]!.body)).toMatchObject({ model: "claude-sonnet-4-6" });
  });

  it("forwards openai /v1/chat/completions with the host bearer", async () => {
    const up = mockUpstream();
    const res = await app.fetch(proxyUrl("/v1/chat/completions", {
      method: "POST",
      headers: { "x-internal-token": CONNECT_TOKEN, "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5" }),
    }));
    expect(res.status).toBe(200);
    expect(up.calls[0]!.url).toBe("https://api.openai.com/v1/chat/completions");
    expect(up.calls[0]!.headers.get("authorization")).toBe("Bearer sk-openai-host-real");
  });

  it("forwards GET /v1/models", async () => {
    const up = mockUpstream();
    const res = await app.fetch(proxyUrl("/v1/models", { headers: { "x-internal-token": CONNECT_TOKEN } }));
    expect(res.status).toBe(200);
    expect(up.calls[0]!.url).toBe("https://api.openai.com/v1/models");
  });
});

describe("M7 /llm/* — WS responses", () => {
  // The Codex WS passthrough is now a real upgrade (handed to createCodexRelay);
  // the live bidirectional relay is covered in tests/integration/proxy-ws.test.ts.
  // Here we only pin the HTTP-edge behaviour reachable via app.fetch.
  it("426 Upgrade Required for a plain (non-Upgrade) GET", async () => {
    const res = await app.fetch(proxyUrl("/v1/responses", { headers: { "x-internal-token": CONNECT_TOKEN } }));
    expect(res.status).toBe(426);
    expect(await res.json()).toMatchObject({ success: false, error: { code: "UPGRADE_REQUIRED" } });
  });

  it("401 without a proxy credential — the gate runs before the upgrade", async () => {
    const res = await app.fetch(proxyUrl("/v1/responses"));
    expect(res.status).toBe(401);
  });
});
