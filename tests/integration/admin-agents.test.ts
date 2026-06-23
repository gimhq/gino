import type { AppDatabase } from "@/db";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { verifyConnectToken } from "@/agents/registry";
import { createApp } from "@/app";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { tasks } from "@/db/schema";
import { createLogger } from "@/shared/lib/logger";

// M3 — `/admin/agents` OOB control API (behind the channel token).

const REGISTER_TOKEN = "reg-secret-admin";
const CHANNEL_TOKEN = "chan-secret-admin";

const dbPath = resolve(tmpdir(), `gino-admin-agents-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: AppDatabase;
let app: ReturnType<typeof createApp>;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  app = createApp({ config, db, logger: createLogger(config) });
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

function admin(path: string, init?: RequestInit) {
  return app.fetch(new Request(`http://local/api/admin${path}`, {
    ...init,
    headers: { authorization: `Bearer ${CHANNEL_TOKEN}`, ...(init?.headers ?? {}) },
  }));
}

/** Enroll a real agent through the register endpoint; returns id + connect token. */
async function enroll(): Promise<{ agentId: string; connectToken: string }> {
  const res = await app.fetch(new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, capabilities: ["claude"], maxThreads: 3 }),
  }));
  expect(res.status).toBe(200);
  const body = await res.json() as { agentId: string; connectToken: string };
  return body;
}

describe("M3 /admin/agents — auth", () => {
  it("401 without the channel bearer", async () => {
    expect((await app.fetch(new Request("http://local/api/admin/agents"))).status).toBe(401);
  });
});

describe("M3 /admin/agents — list + get", () => {
  it("lists agents in the success envelope (+ online flag + capacity)", async () => {
    const { agentId } = await enroll();
    const res = await admin("/agents");
    expect(res.status).toBe(200);
    const body = await res.json() as { success: boolean; data: Array<{ id: string; online: boolean; capacity: unknown }> };
    expect(body.success).toBe(true);
    const row = body.data.find(a => a.id === agentId);
    expect(row).toBeDefined();
    expect(row?.online).toBe(false); // enrolled, no live WS
    expect(row?.capacity).toMatchObject({ maxThreads: 3, hasRoom: true });
  });

  it("gets one agent with manual runtime inspect 'unknown' + empty grants", async () => {
    const { agentId } = await enroll();
    const res = await admin(`/agents/${agentId}`);
    expect(res.status).toBe(200);
    const body = await res.json() as { data: { id: string; runtime: string; grants: string[]; status: string } };
    expect(body.data.id).toBe(agentId);
    expect(body.data.runtime).toBe("unknown"); // manual launcher
    expect(body.data.grants).toEqual([]);
    expect(body.data.status).toBe("enrolled");
  });

  it("404 for an unknown agent", async () => {
    expect((await admin("/agents/no-such-agent")).status).toBe(404);
  });
});

describe("M3 /admin/agents — launch (B2b, wired)", () => {
  it("401 without the channel bearer", async () => {
    const res = await app.fetch(new Request("http://local/api/admin/agents/launch", { method: "POST" }));
    expect(res.status).toBe(401);
  });

  it("422 when the body is missing hostUrl (route is live + validated, not a 501 stub)", async () => {
    const res = await admin("/agents/launch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ kind: "docker" }),
    });
    expect(res.status).toBe(422);
    expect(await res.json()).toMatchObject({ success: false, error: { code: "VALIDATION_ERROR" } });
  });
});

describe("M3 /admin/agents — grant/revoke (OD13)", () => {
  it("grant then revoke reflect in the agent's grants", async () => {
    const { agentId } = await enroll();

    const granted = await admin(`/agents/${agentId}/grant`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ owner_id: "owner-42" }),
    });
    expect(granted.status).toBe(200);
    let get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { grants: string[] } }).data.grants).toContain("owner-42");

    const revoked = await admin(`/agents/${agentId}/revoke`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ owner_id: "owner-42" }),
    });
    expect(revoked.status).toBe(200);
    get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { grants: string[] } }).data.grants).toEqual([]);
  });

  it("422 when owner_id is missing", async () => {
    const { agentId } = await enroll();
    const res = await admin(`/agents/${agentId}/grant`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(422);
  });
});

describe("M3 /admin/agents — stop + remove", () => {
  it("stop on a manual agent falls back to a WS close", async () => {
    const { agentId } = await enroll();
    const res = await admin(`/agents/${agentId}/stop`, { method: "POST" });
    expect(res.status).toBe(200);
    expect(((await res.json()) as { data: { via: string } }).data.via).toBe("ws-close");
  });

  it("remove revokes the connect token + orphans tasks (mirror kept)", async () => {
    const { agentId, connectToken } = await enroll();
    db.insert(tasks).values({
      id: `task-${agentId}`,
      ownerId: "owner-1",
      engine: "claude",
      model: "sonnet",
      status: "running",
      agentId,
    }).run();

    const res = await admin(`/agents/${agentId}/remove`, { method: "POST" });
    expect(res.status).toBe(200);
    const body = await res.json() as { data: { orphanedTasks: number } };
    expect(body.data.orphanedTasks).toBe(1);

    // Token revoked, status removed.
    expect(verifyConnectToken(db, agentId, connectToken)).toBeNull();
    const get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { status: string } }).data.status).toBe("removed");

    // Mirror kept: the task row still exists, placement cleared.
    const task = db.select().from(tasks).where(eq(tasks.id, `task-${agentId}`)).get();
    expect(task).toBeDefined();
    expect(task?.agentId).toBeNull();
  });
});
