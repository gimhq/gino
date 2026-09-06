import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { createDb } from "@gino/host-store-sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

const REGISTER_TOKEN = "reg-secret-e2e";
const CHANNEL_TOKEN = "chan-secret-e2e";

const dbPath = resolve(tmpdir(), `gino-edge-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
  // HOST-XC-02: explicit same-secret reuse so one bearer drives both surfaces in this test.
  GINO_HTTP_ADMIN_TOKEN: CHANNEL_TOKEN,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: ReturnType<typeof createDb>;
let app: ReturnType<typeof createApp>;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  app = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
});

afterAll(() => {
  db.close();
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      rmSync(`${dbPath}${suffix}`, { force: true });
    }
    catch {}
  }
});

const url = (path: string) => new Request(`http://local${path}`);

describe("M1 edge skeleton — topology", () => {
  it("config (token vars) loaded", () => {
    expect(config.REGISTER_TOKEN).toBe(REGISTER_TOKEN);
    expect(config.HTTP_CHANNEL_TOKEN).toBe(CHANNEL_TOKEN);
  });

  it("keeps the public system routes working", async () => {
    const res = await app.fetch(url("/api/health"));
    expect(res.status).toBe(200);
  });

  it("unknown path → clean 404 (no catch-all)", async () => {
    expect((await app.fetch(url("/api/nope"))).status).toBe(404);
    // Unknown sub-paths under a mounted prefix also 404 (with valid auth).
    const res = await app.fetch(new Request("http://local/api/admin/does-not-exist", {
      headers: { authorization: `Bearer ${CHANNEL_TOKEN}` },
    }));
    expect(res.status).toBe(404);
  });
});

describe("M1 edge skeleton — /admin/* admin token (HOST-XC-02; equals the channel token here by explicit config)", () => {
  it("401 without the admin bearer", async () => {
    const res = await app.fetch(url("/api/admin/agents"));
    expect(res.status).toBe(401);
  });

  // The /admin surface is reachable behind the admin bearer. `/admin/agents`
  // (M3/B2) + `/admin/tasks` (M4/B3) + `/admin/channels` (M6/B4) + `/admin/config`
  // (M8/B6) are all LIVE. The Config surface now validates its input (a missing
  // `name`/`kind` → 400 in the standard envelope) instead of the old 501 stub —
  // reachable + authorized, not a placeholder.
  it("config surface is live (400 on a bad request) with the admin bearer", async () => {
    const res = await app.fetch(new Request("http://local/api/admin/config/artifacts", {
      method: "POST",
      headers: { authorization: `Bearer ${CHANNEL_TOKEN}` },
    }));
    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ success: false, error: { code: "BAD_REQUEST" } });
  });

  it("/admin/agents + /admin/tasks + /admin/channels are live (200 list) with the admin bearer", async () => {
    for (const path of ["/api/admin/agents", "/api/admin/tasks", "/api/admin/channels"]) {
      const res = await app.fetch(new Request(`http://local${path}`, {
        headers: { authorization: `Bearer ${CHANNEL_TOKEN}` },
      }));
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ success: true });
    }
  });
});

describe("M1 edge skeleton — /agent/register register token", () => {
  it("401 without a register token", async () => {
    const res = await app.fetch(new Request("http://local/api/agent/register", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    }));
    expect(res.status).toBe(401);
  });

  // M3 enrollment is implemented (B1b): a valid register token + protocolVersion
  // enrolls the agent and returns its id + connect token + config.
  it("enrolls (200) with the register token", async () => {
    const res = await app.fetch(new Request("http://local/api/agent/register", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, capabilities: ["claude"] }),
    }));
    expect(res.status).toBe(200);
    const body = await res.json() as { agentId?: string; connectToken?: string };
    expect(body.agentId).toBeTruthy();
    expect(body.connectToken).toBeTruthy();
  });
});

describe("M1 edge skeleton — /agent data plane connect token", () => {
  it("401 on /agent/ws without x-internal-token", async () => {
    const res = await app.fetch(url("/api/agent/ws/agent-1"));
    expect(res.status).toBe(401);
  });

  it("401 on /agent/config without x-internal-token", async () => {
    const res = await app.fetch(url("/api/agent/config/agent-1"));
    expect(res.status).toBe(401);
  });

  // The connect token is now verified per-agent (B1b): an unknown agent / bad
  // token is 401, not a reachable stub. (A valid round trip lives in spine.test.ts.)
  it("401 on /agent/config with an unrecognized connect token", async () => {
    const res = await app.fetch(new Request("http://local/api/agent/config/agent-1", {
      headers: { "x-internal-token": "any-connect-token" },
    }));
    expect(res.status).toBe(401);
  });
});
