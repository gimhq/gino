// HOST-M9-09 — /metrics serves the in-process registry, gated by the admin
// bearer (HOST-XC-02: NOT the channel token). HOST-M9-08 — createApp warns at
// composition time when the proxy config is spoofable (TRUST_PROXY without
// TRUSTED_PROXY_IPS).
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

const CHANNEL_TOKEN = "test-channel-token";
const ADMIN_TOKEN = "test-admin-token";
const dbPath = resolve(tmpdir(), `gino-metrics-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
  GINO_HTTP_ADMIN_TOKEN: ADMIN_TOKEN,
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

describe("GET /api/metrics (HOST-M9-09)", () => {
  it("rejects an unauthenticated scrape with 401", async () => {
    const res = await app.fetch(new Request("http://local/api/metrics"));
    expect(res.status).toBe(401);
  });

  it("rejects a wrong bearer with 401", async () => {
    const res = await app.fetch(new Request("http://local/api/metrics", {
      headers: { authorization: "Bearer wrong-token" },
    }));
    expect(res.status).toBe(401);
  });

  it("rejects the CHANNEL bearer with 401 (HOST-XC-02 — /metrics is admin-only)", async () => {
    const res = await app.fetch(new Request("http://local/api/metrics", {
      headers: { authorization: `Bearer ${CHANNEL_TOKEN}` },
    }));
    expect(res.status).toBe(401);
  });

  it("serves prometheus exposition to the admin bearer", async () => {
    // Prime the registry: any metered request creates http_requests_total.
    await app.fetch(new Request("http://local/api/system/version"));
    const res = await app.fetch(new Request("http://local/api/metrics", {
      headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    }));
    expect(res.status).toBe(200);
    const body = await res.text();
    expect(body).toContain("# TYPE http_requests_total counter");
    // Scrapes themselves are excluded from the request metrics.
    expect(body).not.toContain("route=\"/api/metrics\"");
  });

  it("fails closed when no admin token is configured", async () => {
    const bare = loadConfigStrict({
      NODE_ENV: "test",
      GINO_DB_PATH: dbPath,
      GINO_LOG_LEVEL: "error",
      GINO_LOG_TO_STDOUT: "true",
    });
    const bareApp = createApp({ config: bare, host: createAppHost({ config: bare, db, logger: createLogger(bare) }) });
    const res = await bareApp.fetch(new Request("http://local/api/metrics", {
      headers: { authorization: "Bearer " },
    }));
    expect(res.status).toBe(401);
  });
});

describe("spoofable proxy config warning (HOST-M9-08)", () => {
  function warnCapturingLogger() {
    const warnings: unknown[] = [];
    const logger = {
      debug: () => {},
      info: () => {},
      warn: (msg: unknown) => warnings.push(msg),
      error: () => {},
      fatal: () => {},
    };
    return { logger: logger as unknown as ReturnType<typeof createLogger>, warnings };
  }

  it("warns when TRUST_PROXY is enabled without an allow-list", () => {
    const spoofable = loadConfigStrict({
      NODE_ENV: "test",
      GINO_DB_PATH: dbPath,
      GINO_LOG_TO_STDOUT: "true",
      GINO_TRUST_PROXY: "true",
    });
    const { logger, warnings } = warnCapturingLogger();
    createApp({ config: spoofable, host: createAppHost({ config: spoofable, db, logger }) });
    expect(warnings).toHaveLength(1);
    expect(String(warnings[0])).toContain("TRUSTED_PROXY_IPS");
  });

  it("stays quiet when an allow-list narrows the trusted peers", () => {
    const narrowed = loadConfigStrict({
      NODE_ENV: "test",
      GINO_DB_PATH: dbPath,
      GINO_LOG_TO_STDOUT: "true",
      GINO_TRUST_PROXY: "true",
      GINO_TRUSTED_PROXY_IPS: "10.0.0.5",
    });
    const { logger, warnings } = warnCapturingLogger();
    createApp({ config: narrowed, host: createAppHost({ config: narrowed, db, logger }) });
    expect(warnings).toHaveLength(0);
  });
});
