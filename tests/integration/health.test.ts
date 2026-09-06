import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

const dbPath = resolve(tmpdir(), `gino-health-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
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

describe("system routes", () => {
  it("GET /api/health → 200 { status: ok }", async () => {
    const res = await app.fetch(new Request("http://local/api/health"));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok" });
  });

  it("GET /api/health/ready → 200 { status: ready } (db reachable)", async () => {
    const res = await app.fetch(new Request("http://local/api/health/ready"));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ready" });
  });

  it("GET /api/system/version → build info envelope", async () => {
    const res = await app.fetch(new Request("http://local/api/system/version"));
    expect(res.status).toBe(200);
    const body = await res.json() as { success: boolean; data: { version: string } };
    expect(body.success).toBe(true);
    expect(typeof body.data.version).toBe("string");
  });

  it("GET /api/openapi.json → spec served", async () => {
    const res = await app.fetch(new Request("http://local/api/openapi.json"));
    expect(res.status).toBe(200);
    const spec = await res.json() as { openapi: string; paths: Record<string, unknown> };
    expect(spec.openapi).toBe("3.1.0");
    expect(spec.paths["/health"]).toBeDefined();
  });

  it("unknown path → clean 404 (no catch-all)", async () => {
    const res = await app.fetch(new Request("http://local/api/nope"));
    expect(res.status).toBe(404);
  });
});
