// HOST-M1M2-09 — `POST /agent/output/:agentId` HTTP retry semantics: only an
// UNDECODABLE frame maps to 400 BAD_FRAME (permanently bad — the agent drops
// it); a TRANSIENT storage failure surfaces as a 5xx so the agent retries; a
// decodable-but-poison frame is acked-and-dropped with a 2xx (HOST-M1M2-04).
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { getTranscript } from "@/msgstore";
import { createLogger } from "@/shared/lib/logger";
import { sqliteStorage } from "@/store/sqlite";
import { createTask, setPlacement } from "@/tasks";

const REGISTER_TOKEN = "reg-secret-output";
const dbPath = resolve(tmpdir(), `gino-output-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: "chan-secret-output",
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: ReturnType<typeof createDb>;
let storage: ReturnType<typeof sqliteStorage>;
let app: ReturnType<typeof createApp>;
let agentId: string;
let connectToken: string;
let taskId: string;

beforeAll(async () => {
  db = createDb(config.DB_PATH);
  storage = sqliteStorage(db);
  app = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });

  const res = await app.fetch(new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, source: "manual", capabilities: ["claude"] }),
  }));
  expect(res.status).toBe(200);
  ({ agentId, connectToken } = await res.json() as { agentId: string; connectToken: string });

  taskId = createTask(storage, { ownerId: "owner", engine: "claude", model: "sonnet", status: "active" }).id;
  setPlacement(storage, taskId, agentId);
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

async function postOutput(body: string): Promise<Response> {
  return await app.fetch(new Request(`http://local/api/agent/output/${agentId}`, {
    method: "POST",
    headers: { "x-internal-token": connectToken },
    body,
  }));
}

describe("POST /agent/output — 400 vs 5xx vs poison-ack (HOST-M1M2-09 / HOST-M1M2-04)", () => {
  it("stores a valid frame and returns its ack (200)", async () => {
    const res = await postOutput(JSON.stringify({ type: "thread_message", outboxId: 1, taskId, threadId: "thr-http", data: { seq: 1, kind: "assistant_text", content: "hi" } }));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ type: "ack", id: 1 });
    expect(getTranscript(storage, "thr-http")).toHaveLength(1);
  });

  it("maps an UNDECODABLE body to 400 BAD_FRAME (do not retry)", async () => {
    for (const body of ["not json", JSON.stringify({ noType: 1 })]) {
      const res = await postOutput(body);
      expect(res.status).toBe(400);
      const json = await res.json() as { error: { code: string } };
      expect(json.error.code).toBe("BAD_FRAME");
    }
  });

  it("acks-and-drops a decodable frame with permanently invalid data (200, poison policy)", async () => {
    const res = await postOutput(JSON.stringify({ type: "thread_result", outboxId: 2, taskId, threadId: "thr-http", data: { status: "exploded", duration_ms: 1, final_seq: 1 } }));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ type: "ack", id: 2 });
  });

  it("surfaces a storage failure as a 5xx so the agent retries (NOT 400)", async () => {
    db.run(sql`DROP TABLE messages`); // deterministic stand-in for a transient write failure — last test in the file
    const res = await postOutput(JSON.stringify({ type: "thread_message", outboxId: 3, taskId, threadId: "thr-http", data: { seq: 2, kind: "assistant_text", content: "retry me" } }));
    expect(res.status).toBe(500);
  });
});
