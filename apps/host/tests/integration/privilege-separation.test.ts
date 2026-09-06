// HOST-XC-02 — privilege separation between the channel token (user message
// I/O on /messages/*) and the admin token (control plane: /admin/*,
// /system/lode/*, GET /metrics). A channel credential must never reach the
// control plane, the admin credential does not grant message I/O (unless the
// operator explicitly configures the same secret for both), and an
// unconfigured admin token fails CLOSED — it never falls back to the channel
// token.
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

const CHANNEL_TOKEN = "chan-secret-sep";
const ADMIN_TOKEN = "admin-secret-sep";
const dbPath = resolve(tmpdir(), `gino-privsep-${process.pid}-${Date.now()}.db`);

const baseEnv = {
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
} as const;

let db: ReturnType<typeof createDb>;
let app: ReturnType<typeof createApp>;

beforeAll(() => {
  const config = loadConfigStrict({
    ...baseEnv,
    GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
    GINO_HTTP_ADMIN_TOKEN: ADMIN_TOKEN,
  });
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

function get(path: string, token?: string) {
  return app.fetch(new Request(`http://local/api${path}`, {
    ...(token != null ? { headers: { authorization: `Bearer ${token}` } } : {}),
  }));
}

function post(path: string, token?: string) {
  return app.fetch(new Request(`http://local/api${path}`, {
    method: "POST",
    ...(token != null ? { headers: { authorization: `Bearer ${token}` } } : {}),
  }));
}

// Control-plane surfaces, one representative per mount: the /admin router,
// the lode operator actions, and the metrics scrape.
const ADMIN_SURFACES: Array<[string, (token?: string) => Response | Promise<Response>]> = [
  ["/admin/agents", token => get("/admin/agents", token)],
  ["/system/lode/restart", token => post("/system/lode/restart", token)],
  ["/metrics", token => get("/metrics", token)],
];

describe("HOST-XC-02 — the channel token does NOT grant the control plane", () => {
  it.each(ADMIN_SURFACES)("%s → 401 with the channel bearer", async (_path, hit) => {
    expect((await hit(CHANNEL_TOKEN)).status).toBe(401);
  });

  it.each(ADMIN_SURFACES)("%s → 401 with no bearer", async (_path, hit) => {
    expect((await hit()).status).toBe(401);
  });
});

describe("HOST-XC-02 — the admin token grants the control plane", () => {
  it("GET /admin/agents → 200", async () => {
    expect((await get("/admin/agents", ADMIN_TOKEN)).status).toBe(200);
  });

  it("GET /metrics → 200", async () => {
    expect((await get("/metrics", ADMIN_TOKEN)).status).toBe(200);
  });

  it("POST /system/lode/restart → 409 (auth passed; not under the lode supervisor)", async () => {
    expect((await post("/system/lode/restart", ADMIN_TOKEN)).status).toBe(409);
  });
});

describe("HOST-XC-02 — message I/O stays on the channel token", () => {
  // 404 (unknown channel) proves auth PASSED and the handler ran; 401 proves
  // the bearer was rejected before any channel lookup.
  it("channel bearer reaches /messages/* (404 on an unknown channel)", async () => {
    expect((await get("/messages/no-such-channel/poll?after=0", CHANNEL_TOKEN)).status).toBe(404);
  });

  it("admin bearer is rejected on /messages/* when the tokens differ", async () => {
    expect((await get("/messages/no-such-channel/poll?after=0", ADMIN_TOKEN)).status).toBe(401);
  });
});

describe("HOST-XC-02 — unconfigured admin token fails CLOSED (no channel fallback)", () => {
  it("every control-plane surface is 401 for every bearer", async () => {
    const config = loadConfigStrict({
      ...baseEnv,
      GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
      // GINO_HTTP_ADMIN_TOKEN deliberately unset.
    });
    const bare = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
    for (const [method, path] of [
      ["GET", "/admin/agents"],
      ["POST", "/system/lode/restart"],
      ["GET", "/metrics"],
    ] as const) {
      for (const headers of [
        undefined,
        { authorization: `Bearer ${CHANNEL_TOKEN}` },
        { authorization: "Bearer " },
      ]) {
        const res = await bare.fetch(new Request(`http://local/api${path}`, {
          method,
          ...(headers ? { headers } : {}),
        }));
        expect(res.status).toBe(401);
      }
    }
  });
});

describe("HOST-XC-02 — explicit same-secret reuse still works (operator opt-in)", () => {
  it("one shared secret set for BOTH env vars grants both surfaces", async () => {
    const config = loadConfigStrict({
      ...baseEnv,
      GINO_HTTP_CHANNEL_TOKEN: "shared-secret",
      GINO_HTTP_ADMIN_TOKEN: "shared-secret",
    });
    const shared = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
    const admin = await shared.fetch(new Request("http://local/api/admin/agents", {
      headers: { authorization: "Bearer shared-secret" },
    }));
    expect(admin.status).toBe(200);
    const messages = await shared.fetch(new Request("http://local/api/messages/no-such-channel/poll?after=0", {
      headers: { authorization: "Bearer shared-secret" },
    }));
    expect(messages.status).toBe(404);
  });
});
