// HOST-M1M2-10 / HOST-M9-02 / HOST-M7-03 — the rate-limit middleware is wired
// onto the edge: POST /agent/register (register_error{rate_limited}, protocol
// §3), the /llm/* proxy surface (per-principal keying), and the /messages/*
// channel-token routes. Each `newApp` call builds a fresh config object, so
// its rate-limit buckets are isolated (state is keyed by config identity).
import type { Storage } from "@/host/ports/storage";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { registerChannel } from "@/channels";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { agents } from "@/db/schema";
import { hashToken } from "@/platform";
import { createLogger } from "@/shared/lib/logger";
import { sqliteStorage } from "@/store/sqlite";

const REGISTER_TOKEN = "reg-secret-rl";
const CHANNEL_TOKEN = "chan-secret-rl";
const dbPath = resolve(tmpdir(), `gino-rl-${process.pid}-${Date.now()}.db`);

let db: ReturnType<typeof createDb>;
let storage: Storage;
const realFetch = globalThis.fetch;

function newApp(overrides: Record<string, string> = {}) {
  const config = loadConfigStrict({
    NODE_ENV: "test",
    GINO_DB_PATH: dbPath,
    GINO_REGISTER_TOKEN: REGISTER_TOKEN,
    GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
    GINO_LOG_LEVEL: "error",
    GINO_LOG_TO_STDOUT: "true",
    GINO_PROXY_ANTHROPIC_API_KEY: "sk-ant-host-real",
    ...overrides,
  });
  return createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
}

beforeAll(() => {
  db = createDb(dbPath);
  storage = sqliteStorage(db);
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

/** Replace global fetch so the /llm proxy never hits the network. */
function mockUpstream(): void {
  globalThis.fetch = (async (_input: string | URL | Request, _init?: RequestInit) =>
    new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "content-type": "application/json" } })
  ) as typeof fetch;
}

function registerReq(token: string = REGISTER_TOKEN): Request {
  return new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: token, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  });
}

function seedAgent(id: string, connectToken: string): void {
  db.insert(agents).values({
    id,
    source: "manual",
    access: "public",
    connectTokenHash: hashToken(connectToken),
    capabilities: { engines: ["claude"] },
    status: "online",
  }).run();
}

describe("rate limit — POST /agent/register (HOST-M1M2-10 / HOST-M9-02)", () => {
  it("under the limit registrations succeed; over the limit → 429 register_error{rate_limited}", async () => {
    const app = newApp({ GINO_RATE_LIMIT_REGISTER_MAX: "3" });

    for (let i = 0; i < 3; i++) {
      const res = await app.fetch(registerReq());
      expect(res.status).toBe(200);
    }

    const over = await app.fetch(registerReq());
    expect(over.status).toBe(429);
    expect(over.headers.get("retry-after")).toBeTruthy();
    expect(await over.json()).toEqual({ register_error: { reason: "rate_limited" } });
  });

  it("throttles BEFORE auth: rotating bad register tokens cannot evade the IP budget", async () => {
    const app = newApp({ GINO_RATE_LIMIT_REGISTER_MAX: "2" });

    expect((await app.fetch(registerReq("guess-1"))).status).toBe(401);
    expect((await app.fetch(registerReq("guess-2"))).status).toBe(401);
    const over = await app.fetch(registerReq("guess-3"));
    expect(over.status).toBe(429);
    expect(await over.json()).toEqual({ register_error: { reason: "rate_limited" } });
  });
});

describe("rate limit — /llm/* (HOST-M7-03)", () => {
  const llmReq = (token: string) =>
    new Request("http://local/api/llm/v1/models", { headers: { "x-internal-token": token } });

  it("keys per verified principal: one agent's burst does not throttle another", async () => {
    mockUpstream();
    seedAgent("agent-rl-a", "connect-rl-a");
    seedAgent("agent-rl-b", "connect-rl-b");
    const app = newApp({ GINO_RATE_LIMIT_LLM_MAX: "2" });

    // Agent A: under the limit unaffected, over the limit → 429.
    expect((await app.fetch(llmReq("connect-rl-a"))).status).toBe(200);
    expect((await app.fetch(llmReq("connect-rl-a"))).status).toBe(200);
    const over = await app.fetch(llmReq("connect-rl-a"));
    expect(over.status).toBe(429);
    expect(over.headers.get("retry-after")).toBeTruthy();
    const body = await over.json() as { success: boolean; error: { code: string } };
    expect(body.success).toBe(false);
    expect(body.error.code).toBe("RATE_LIMITED");

    // Agent B's budget is untouched by A's burst.
    expect((await app.fetch(llmReq("connect-rl-b"))).status).toBe(200);
  });

  it("invalid credentials share the per-IP budget: rotating guesses cannot evade it", async () => {
    const app = newApp({ GINO_RATE_LIMIT_LLM_MAX: "2" });

    expect((await app.fetch(llmReq("wrong-1"))).status).toBe(401);
    expect((await app.fetch(llmReq("wrong-2"))).status).toBe(401);
    expect((await app.fetch(llmReq("wrong-3"))).status).toBe(429);
  });
});

describe("rate limit — /messages/* channel routes (HOST-M9-02)", () => {
  const pollReq = (channel: string, bearer: string) =>
    new Request(`http://local/api/messages/${channel}/poll`, {
      headers: { authorization: `Bearer ${bearer}` },
    });

  it("valid-bearer traffic is limited per principal; under the limit unaffected", async () => {
    const channel = registerChannel(storage, { name: "rl-chan", type: "test", ownerId: "owner-rl" });
    const app = newApp({ GINO_RATE_LIMIT_MESSAGES_MAX: "3" });

    for (let i = 0; i < 3; i++) {
      expect((await app.fetch(pollReq(channel.id, CHANNEL_TOKEN))).status).toBe(200);
    }
    const over = await app.fetch(pollReq(channel.id, CHANNEL_TOKEN));
    expect(over.status).toBe(429);
    const body = await over.json() as { success: boolean; error: { code: string } };
    expect(body.error.code).toBe("RATE_LIMITED");
  });

  it("bad-bearer floods drain the IP budget, not the valid principal's", async () => {
    const channel = registerChannel(storage, { name: "rl-chan-2", type: "test", ownerId: "owner-rl" });
    const app = newApp({ GINO_RATE_LIMIT_MESSAGES_MAX: "2" });

    // Rotating guessed bearers all land in the shared per-IP bucket.
    expect((await app.fetch(pollReq(channel.id, "guess-1"))).status).toBe(401);
    expect((await app.fetch(pollReq(channel.id, "guess-2"))).status).toBe(401);
    expect((await app.fetch(pollReq(channel.id, "guess-3"))).status).toBe(429);

    // The valid channel token is keyed separately and still passes.
    expect((await app.fetch(pollReq(channel.id, CHANNEL_TOKEN))).status).toBe(200);
  });
});
