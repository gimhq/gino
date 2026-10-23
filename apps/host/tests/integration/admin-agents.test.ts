import type { Storage } from "@gino/host-core";
import type { AppDatabase } from "@gino/host-store-sqlite";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { verifyConnectToken } from "@gino/host-core/domain";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { tasks } from "@gino/host-store-sqlite/schema";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { eq } from "drizzle-orm";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
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
  // Explicit same-secret reuse so one bearer drives both surfaces in this test.
  GINO_HTTP_ADMIN_TOKEN: CHANNEL_TOKEN,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: AppDatabase;
let storage: Storage;
let app: ReturnType<typeof createApp>;
let host: ReturnType<typeof createAppHost>;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  storage = sqliteStorage(db);
  host = createAppHost({ config, db, logger: createLogger(config) });
  app = createApp({ config, host });
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

describe("HOST-M3-09 /admin/agents/:id/access — admin-only access mutation", () => {
  async function setAccess(agentId: string, access: string) {
    return admin(`/agents/${agentId}/access`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ access }),
    });
  }

  it("flips a dedicated agent public and back", async () => {
    const { agentId } = await enroll(); // always lands dedicated
    let get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { access: string } }).data.access).toBe("dedicated");

    expect((await setAccess(agentId, "public")).status).toBe(200);
    get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { access: string } }).data.access).toBe("public");

    expect((await setAccess(agentId, "dedicated")).status).toBe(200);
    get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { access: string } }).data.access).toBe("dedicated");
  });

  it("rejects an invalid access value (422), an unknown agent (404), a removed agent (410)", async () => {
    const { agentId } = await enroll();
    expect((await setAccess(agentId, "everyone")).status).toBe(422);
    expect((await setAccess("no-such-agent", "public")).status).toBe(404);

    await admin(`/agents/${agentId}/remove`, { method: "POST" });
    expect((await setAccess(agentId, "public")).status).toBe(410);
  });

  it("requires the channel bearer (401 without)", async () => {
    const { agentId } = await enroll();
    const res = await app.fetch(new Request(`http://local/api/admin/agents/${agentId}/access`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ access: "public" }),
    }));
    expect(res.status).toBe(401);
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
    expect(verifyConnectToken(storage, agentId, connectToken)).toBeNull();
    const get = await admin(`/agents/${agentId}`);
    expect(((await get.json()) as { data: { status: string } }).data.status).toBe("removed");

    // Mirror kept: the task row still exists, placement cleared.
    const task = db.select().from(tasks).where(eq(tasks.id, `task-${agentId}`)).get();
    expect(task).toBeDefined();
    expect(task?.agentId).toBeNull();
  });
});

describe("M3 /admin/agents — engine-free file operations", () => {
  const json = (body: unknown): RequestInit => ({ method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

  async function agentConfig(agentId: string, connectToken: string) {
    const res = await app.fetch(new Request(`http://local/api/agent/config/${agentId}`, { headers: { "x-internal-token": connectToken } }));
    expect(res.status).toBe(200);
    return await res.json() as { remoteOps: { fs: boolean } };
  }

  it("401 without the admin bearer", async () => {
    const { agentId } = await enroll();
    for (const path of ["fs", "remote-ops"]) {
      const res = await app.fetch(new Request(`http://local/api/admin/agents/${agentId}/${path}`, json({ op: "list", path: ".", fs: true })));
      expect(res.status).toBe(401);
    }
  });

  it("the switch is off by default, cannot be self-declared at register, and is served once an admin sets it", async () => {
    const res = await app.fetch(new Request("http://local/api/agent/register", json({
      registerToken: REGISTER_TOKEN,
      protocolVersion: 2,
      capabilities: { engines: ["claude"], remote_ops: { fs: true } },
    })));
    const { agentId, connectToken, config: registered } = await res.json() as { agentId: string; connectToken: string; config: { remoteOps: { fs: boolean } } };
    expect(registered.remoteOps).toMatchObject({ fs: false });
    expect((await agentConfig(agentId, connectToken)).remoteOps).toMatchObject({ fs: false });

    const set = await admin(`/agents/${agentId}/remote-ops`, json({ fs: true }));
    expect(set.status).toBe(200);
    expect((await set.json() as { data: unknown }).data).toMatchObject({ agentId, remoteOps: { fs: true } });
    expect((await agentConfig(agentId, connectToken)).remoteOps).toMatchObject({ fs: true });
    // Queued for the agent as a config_update (it does not re-fetch on reconnect).
    const queued = storage.outbox.listUnacked(agentId).filter(row => row.type === "config_update");
    expect(queued.map(row => row.dataJson)).toMatchObject([{ remoteOps: { fs: true } }]);
  });

  it("404 for an unknown agent, 409 for an offline one, 422 for a bad request", async () => {
    expect((await admin("/agents/ghost/fs", json({ op: "list", path: "." }))).status).toBe(404);
    expect((await admin("/agents/ghost/remote-ops", json({ fs: true }))).status).toBe(404);
    const { agentId } = await enroll();
    const offline = await admin(`/agents/${agentId}/fs`, json({ op: "list", path: "." }));
    expect(offline.status).toBe(409);
    expect((await admin(`/agents/${agentId}/fs`, json({ op: "chmod", path: "." }))).status).toBe(422);
  });

  /** Attach a fake agent socket that answers each fs_request through `answer`. */
  function attachAgent(agentId: string, answer: (data: { op: string; path: string; ref: string; upload: string; size: number }) => Promise<unknown> | unknown) {
    const ws = {
      send: (raw: string) => {
        const frame = JSON.parse(raw) as { type: string; reqId: string; data: Parameters<typeof answer>[0] };
        expect(frame.type).toBe("fs_request");
        void Promise.resolve(answer(frame.data)).then(data =>
          host.wire.onSocketMessage(agentId, JSON.stringify({ type: "fs_response", reqId: frame.reqId, data })));
      },
      close: () => {},
    };
    host.wire.attachSocket(agentId, ws);
    return () => host.wire.detachSocket(agentId, ws);
  }

  const agentFetch = (path: string, connectToken: string, init?: RequestInit) =>
    app.fetch(new Request(`http://local/api${path}`, { ...init, headers: { "x-internal-token": connectToken, ...(init?.headers ?? {}) } }));

  it("round-trips a metadata request to the connected agent and nothing is queued", async () => {
    const { agentId } = await enroll();
    const detach = attachAgent(agentId, (data) => {
      expect({ op: data.op, path: data.path }).toEqual({ op: "stat", path: "a.txt" });
      return { ok: true, stat: { type: "file", size: 2 } };
    });
    try {
      const res = await admin(`/agents/${agentId}/fs`, json({ op: "stat", path: "a.txt" }));
      expect(res.status).toBe(200);
      expect((await res.json() as { data: unknown }).data).toEqual({ ok: true, stat: { type: "file", size: 2 } });
      expect(storage.outbox.listUnacked(agentId)).toEqual([]);
    }
    finally {
      detach();
    }
  });

  it("the transfer operations cannot be requested through /fs", async () => {
    const { agentId } = await enroll();
    for (const op of ["put", "get", "read", "write"]) {
      expect((await admin(`/agents/${agentId}/fs`, json({ op, path: "a", ref: "http://elsewhere/x" }))).status).toBe(422);
    }
  });

  it("PUT /files stages the bytes for that agent only and removes them afterwards", async () => {
    const { agentId, connectToken } = await enroll();
    const other = await enroll();
    const content = new TextEncoder().encode("hello from the host\n");
    let stagedRef = "";
    const detach = attachAgent(agentId, async (data) => {
      expect(data).toMatchObject({ op: "put", path: "dir/note.txt", size: content.byteLength });
      stagedRef = data.ref;
      // Another agent's connect token cannot fetch the staged bytes.
      expect((await agentFetch(data.ref, other.connectToken)).status).toBe(403);
      const fetched = await agentFetch(data.ref, connectToken);
      expect(fetched.status).toBe(200);
      expect(new Uint8Array(await fetched.arrayBuffer())).toEqual(content);
      return { ok: true };
    });
    try {
      const res = await admin(`/agents/${agentId}/files?path=${encodeURIComponent("dir/note.txt")}`, { method: "PUT", body: content });
      expect(res.status).toBe(200);
      expect((await res.json() as { data: unknown }).data).toEqual({ ok: true });
      // Single-use: the grant and the staged bytes are gone.
      expect((await agentFetch(stagedRef, connectToken)).status).toBe(404);
      expect(storage.outbox.listUnacked(agentId)).toEqual([]);
    }
    finally {
      detach();
    }
  });

  it("GET /files returns the bytes the agent uploaded and maps a refusal to a status", async () => {
    const { agentId, connectToken } = await enroll();
    const content = new Uint8Array([0xFF, 0x00, 0xFE, 0x41]);
    const detach = attachAgent(agentId, async (data) => {
      if (data.path === "missing") {
        return { ok: false, error: { code: "not_found", message: "No such file" } };
      }
      const uploaded = await agentFetch("/agent/attachments", connectToken, { method: "POST", headers: { "x-attachment-name": data.upload }, body: content });
      expect(uploaded.status).toBe(201);
      const { sha256, size } = await uploaded.json() as { sha256: string; size: number };
      return { ok: true, sha256, size };
    });
    try {
      const res = await admin(`/agents/${agentId}/files?path=${encodeURIComponent("out/b.bin")}`);
      expect(res.status).toBe(200);
      expect(res.headers.get("content-disposition")).toContain("b.bin");
      expect(new Uint8Array(await res.arrayBuffer())).toEqual(content);

      const missing = await admin(`/agents/${agentId}/files?path=missing`);
      expect(missing.status).toBe(404);
      expect((await missing.json() as { error: { code: string } }).error.code).toBe("FS_NOT_FOUND");
    }
    finally {
      detach();
    }
  });

  it("the file routes need the admin bearer, a path, and a connected agent", async () => {
    const { agentId } = await enroll();
    expect((await app.fetch(new Request(`http://local/api/admin/agents/${agentId}/files?path=a`))).status).toBe(401);
    expect((await admin(`/agents/${agentId}/files`)).status).toBe(422);
    expect((await admin(`/agents/${agentId}/files?path=a`)).status).toBe(409);
    expect((await admin(`/agents/${agentId}/files?path=a`, { method: "PUT", body: "x" })).status).toBe(409);
    expect((await admin("/agents/ghost/files?path=a")).status).toBe(404);
  });
});
