import type { Storage } from "@gino/host-core";
import type { AppDatabase } from "@gino/host-store-sqlite";
import { rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { createTask, setPlacement } from "@gino/host-core/domain";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

// M8/B6 — the `/agent/attachments` byte channel: ingest → store + descriptor;
// download verified bytes (by id | sha256); a sha256 mismatch is rejected.
//
// AUTHORIZATION (D-B6s / IDOR fix): the requesting agent is resolved FROM its
// connect token (never a client param); a read/write is authorized against the
// specific bytes — an attachment only by the agent PLACED on its task, a config
// artifact only when its sha is in that agent's DESIRED SET. A correctly-placed
// agent's own task attachments still flow; cross-agent access is denied.

const REGISTER = "reg-secret-att";
const CHANNEL = "chan-secret-att";
const stamp = `${process.pid}-${Date.now()}`;
const dbPath = resolve(tmpdir(), `gino-att-${stamp}.db`);
const storeDir = resolve(tmpdir(), `gino-att-store-${stamp}`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_STORE_DIR: storeDir,
  GINO_REGISTER_TOKEN: REGISTER,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL,
  // HOST-XC-02: explicit same-secret reuse so one bearer drives both surfaces in this test.
  GINO_HTTP_ADMIN_TOKEN: CHANNEL,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: AppDatabase;
let storage: Storage;
let app: ReturnType<typeof createApp>;

beforeAll(() => {
  db = createDb(config.DB_PATH);
  storage = sqliteStorage(db);
  app = createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
});

afterAll(() => {
  db.close();
  rmSync(storeDir, { recursive: true, force: true });
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      rmSync(`${dbPath}${suffix}`, { force: true });
    }
    catch {}
  }
});

// ── helpers ──────────────────────────────────────────────────────────────────

async function register(capabilities: string[] = ["claude"]): Promise<{ agentId: string; connectToken: string }> {
  const res = await app.fetch(new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER, protocolVersion: 2, source: "manual", capabilities }),
  }));
  expect(res.status).toBe(200);
  return await res.json() as { agentId: string; connectToken: string };
}

/** Create a task and pin it to `agentId` (the placement the byte channel authorizes against). */
function placeTaskOn(agentId: string): string {
  const task = createTask(storage, { ownerId: `owner-${agentId}`, engine: "claude", model: "sonnet" });
  setPlacement(storage, task.id, agentId);
  return task.id;
}

function tok(token: string) {
  return { "x-internal-token": token };
}

function ingest(token: string, query: string, body: BodyInit | null) {
  return app.fetch(new Request(`http://local/api/agent/attachments?${query}`, { method: "POST", headers: tok(token), body }));
}

function downloadById(token: string, id: string) {
  return app.fetch(new Request(`http://local/api/agent/attachments/${id}`, { headers: tok(token) }));
}

function downloadBySha(token: string, sha256: string) {
  return app.fetch(new Request(`http://local/api/agent/attachments?sha256=${sha256}`, { headers: tok(token) }));
}

function admin(path: string, init?: RequestInit) {
  return app.fetch(new Request(`http://local/api/admin${path}`, {
    ...init,
    headers: { "authorization": `Bearer ${CHANNEL}`, "content-type": "application/json", ...(init?.headers ?? {}) },
  }));
}

// ── connect-token gate ─────────────────────────────────────────────────────────

describe("byte channel gate — connect token resolves the requesting agent", () => {
  it("401 without a connect token", async () => {
    const res = await app.fetch(new Request("http://local/api/agent/attachments?name=a.txt&task_id=t1&thread_id=th1", { method: "POST", body: "x" }));
    expect(res.status).toBe(401);
  });

  it("401 on a connect token that matches no enrolled agent", async () => {
    const res = await ingest("not-a-real-token", "name=a.txt&task_id=t1&thread_id=th1", "x");
    expect(res.status).toBe(401);
  });
});

// ── legit flow — a placed agent reads/writes its OWN task's attachments ──────────

describe("POST /agent/attachments — a placed agent ingests for its own task", () => {
  it("ingests bytes → store + descriptor, then serves them back verified", async () => {
    const a = await register();
    const taskId = placeTaskOn(a.agentId);

    const payload = "the quick brown fox";
    const res = await ingest(a.connectToken, `name=report.txt&task_id=${taskId}&thread_id=th1&content_type=text/plain`, payload);
    expect(res.status).toBe(201);
    const data = await res.json() as { id: string; sha256: string; ref: string; size: number; direction: string };
    expect(data.size).toBe(payload.length);
    expect(data.direction).toBe("outbound");
    // The wire ref is the RESOLVABLE byte-channel URL, never the store path.
    expect(data.ref).toBe(`/agent/attachments/${data.id}`);

    // Download by id (own task) → 200 + verified bytes.
    const byId = await downloadById(a.connectToken, data.id);
    expect(byId.status).toBe(200);
    expect(byId.headers.get("x-content-sha256")).toBe(data.sha256);
    expect(await byId.text()).toBe(payload);

    // Download by sha256 (own task) → 200.
    const bySha = await downloadBySha(a.connectToken, data.sha256);
    expect(bySha.status).toBe(200);
    expect(await bySha.text()).toBe(payload);
  });

  it("requires a name (query or x-attachment-name header) and paired task/thread ids (400)", async () => {
    const a = await register();
    // No name anywhere → 400.
    expect((await ingest(a.connectToken, "content_type=text/plain", "x")).status).toBe(400);
    // task_id without thread_id (or vice versa) → 400.
    expect((await ingest(a.connectToken, "name=a.txt&task_id=t1", "x")).status).toBe(400);
    expect((await ingest(a.connectToken, "name=a.txt&thread_id=th1", "x")).status).toBe(400);
  });

  it("accepts the runner's shape: metadata via headers, no task binding, flat response", async () => {
    const a = await register();
    const payload = "runner output bytes";
    const sha = new Bun.CryptoHasher("sha256").update(payload).digest("hex");
    const res = await app.fetch(new Request("http://local/api/agent/attachments", {
      method: "POST",
      headers: {
        ...tok(a.connectToken),
        "x-attachment-name": "out/report.txt",
        "x-attachment-sha256": sha,
        "content-type": "text/plain",
      },
      body: payload,
    }));
    expect(res.status).toBe(201);
    // Flat body (the runner parses top-level {id, ref}), resolvable ref,
    // path-shaped name flattened to a store segment.
    const data = await res.json() as { id: string; name: string; ref: string; sha256: string };
    expect(data.sha256).toBe(sha);
    expect(data.ref).toBe(`/agent/attachments/${data.id}`);
    expect(data.name).toBe("out__report.txt");
    // No task binding at upload time → no descriptor row yet (that arrives on
    // thread_result), so a by-id download is a 404 until then.
    expect((await downloadById(a.connectToken, data.id)).status).toBe(404);
  });

  it("rejects a declared sha256 that does not match the bytes (400) — stores nothing", async () => {
    const a = await register();
    const taskId = placeTaskOn(a.agentId);
    const res = await ingest(a.connectToken, `name=bad.txt&task_id=${taskId}&thread_id=th1&sha256=${"f".repeat(64)}`, "mismatch");
    expect(res.status).toBe(400);
    expect((await res.json() as { error: { code: string } }).error.code).toBe("SHA_MISMATCH");
  });
});

describe("GET /agent/attachments — a placed agent downloads", () => {
  it("404s an unknown id / sha256", async () => {
    const a = await register();
    expect((await downloadById(a.connectToken, "nope")).status).toBe(404);
    expect((await downloadBySha(a.connectToken, "0".repeat(64))).status).toBe(404);
  });

  it("400s a query with neither id nor sha256", async () => {
    const a = await register();
    expect((await app.fetch(new Request("http://local/api/agent/attachments", { headers: tok(a.connectToken) }))).status).toBe(400);
  });

  it("409s when stored bytes fail the sha256 integrity check (tampered blob)", async () => {
    const a = await register();
    const taskId = placeTaskOn(a.agentId);
    const res = await ingest(a.connectToken, `name=corrupt.txt&task_id=${taskId}&thread_id=th1`, "original");
    const data = await res.json() as { id: string; sha256: string; ref: string };
    // Corrupt the stored blob underneath the descriptor. The wire `ref` is the
    // byte-channel URL; the blob lives at the store ref `<name>/<sha256[:12]>`.
    writeFileSync(resolve(storeDir, `corrupt.txt/${data.sha256.slice(0, 12)}`), "TAMPERED");

    const got = await downloadById(a.connectToken, data.id);
    expect(got.status).toBe(409);
    expect((await got.json() as { error: { code: string } }).error.code).toBe("BLOB_INTEGRITY");
  });

  it("404s (not 500) when a descriptor row points at a deleted blob — HOST-M8-12", async () => {
    const a = await register();
    const taskId = placeTaskOn(a.agentId);
    const res = await ingest(a.connectToken, `name=dangling.txt&task_id=${taskId}&thread_id=th1`, "soon to vanish");
    const data = await res.json() as { id: string; sha256: string; ref: string };
    // Simulate a crash between a GC row delete and blob delete (or manual FS surgery).
    rmSync(resolve(storeDir, `dangling.txt/${data.sha256.slice(0, 12)}`), { force: true });

    const got = await downloadById(a.connectToken, data.id);
    expect(got.status).toBe(404);
  });

  it("serves immutable cache headers + a sha256 ETag, and 304s a matching If-None-Match — HOST-M8-14", async () => {
    const a = await register();
    const taskId = placeTaskOn(a.agentId);
    const payload = "cache me by content address";
    const res = await ingest(a.connectToken, `name=cached.txt&task_id=${taskId}&thread_id=th1`, payload);
    const data = await res.json() as { id: string; sha256: string };

    const full = await downloadById(a.connectToken, data.id);
    expect(full.status).toBe(200);
    expect(full.headers.get("cache-control")).toBe("private, max-age=31536000, immutable");
    expect(full.headers.get("etag")).toBe(`"${data.sha256}"`);

    // Revalidation with the ETag → bodyless 304 (headers intact, no bytes shipped).
    const revalidate = await app.fetch(new Request(`http://local/api/agent/attachments/${data.id}`, {
      headers: { ...tok(a.connectToken), "if-none-match": `"${data.sha256}"` },
    }));
    expect(revalidate.status).toBe(304);
    expect(revalidate.headers.get("etag")).toBe(`"${data.sha256}"`);
    expect(await revalidate.text()).toBe("");

    // A weak validator for the same sha also revalidates; a stale one re-serves.
    const weak = await app.fetch(new Request(`http://local/api/agent/attachments/${data.id}`, {
      headers: { ...tok(a.connectToken), "if-none-match": `W/"${data.sha256}"` },
    }));
    expect(weak.status).toBe(304);
    const stale = await app.fetch(new Request(`http://local/api/agent/attachments/${data.id}`, {
      headers: { ...tok(a.connectToken), "if-none-match": `"${"0".repeat(64)}"` },
    }));
    expect(stale.status).toBe(200);
    expect(await stale.text()).toBe(payload);
  });
});

// ── IDOR regression — cross-agent access is denied ──────────────────────────────

describe("IDOR regression — an agent cannot reach another agent's bytes", () => {
  it("agent B cannot GET agent A's task attachment (by id → 403, by sha → 403)", async () => {
    const a = await register();
    const b = await register();
    const taskA = placeTaskOn(a.agentId);
    placeTaskOn(b.agentId); // B is a real placed agent, just not on A's task

    const res = await ingest(a.connectToken, `name=secret.txt&task_id=${taskA}&thread_id=th1`, "A's private bytes");
    const data = await res.json() as { id: string; sha256: string };

    // A (placed on the task) reads fine.
    expect((await downloadById(a.connectToken, data.id)).status).toBe(200);

    // B (a valid agent, NOT placed on A's task) is denied — known resource, 403.
    expect((await downloadById(b.connectToken, data.id)).status).toBe(403);
    expect((await downloadBySha(b.connectToken, data.sha256)).status).toBe(403);
  });

  it("agent B cannot POST a descriptor for a task placed on agent A (403)", async () => {
    const a = await register();
    const b = await register();
    const taskA = placeTaskOn(a.agentId);

    const res = await ingest(b.connectToken, `name=evil.txt&task_id=${taskA}&thread_id=th1`, "cross-agent write");
    expect(res.status).toBe(403);
  });

  it("an agent cannot POST a descriptor for a task that does not exist (403)", async () => {
    const a = await register();
    const res = await ingest(a.connectToken, "name=ghost.txt&task_id=no-such-task&thread_id=th1", "x");
    expect(res.status).toBe(403);
  });
});

// ── config-artifact desired-set gating (sha must be declared for the agent) ──────

describe("config artifact by sha — gated by the agent's desired set", () => {
  it("403 for a registered artifact NOT in the agent's desired set; 200 once declared global", async () => {
    const a = await register();

    // Register a config artifact (bytes in the store + config_artifacts row).
    const bytes = "# a global skill the host may provision";
    const up = await admin(`/config/artifacts?${new URLSearchParams({ name: "skill.md", kind: "file", content_type: "text/markdown" })}`, {
      method: "POST",
      headers: { "content-type": "text/markdown" },
      body: bytes,
    });
    expect(up.status).toBe(201);
    const sha = (await up.json() as { data: { sha256: string } }).data.sha256;

    // Not yet in any desired set → a known artifact the agent may NOT fetch → 403
    // (NOT 404 — the artifact exists; it is simply not authorized for this agent).
    expect((await downloadBySha(a.connectToken, sha)).status).toBe(403);

    // Declare it in the GLOBAL desired set → now in the agent's desired set → 200.
    const put = await admin("/config/entries", {
      method: "PUT",
      body: JSON.stringify({ scope: "global", entries: [{ name: "skill.md", sha256: sha, kind: "file" }] }),
    });
    expect(put.status).toBe(200);

    const got = await downloadBySha(a.connectToken, sha);
    expect(got.status).toBe(200);
    expect(await got.text()).toBe(bytes);
  });

  it("403 for a task-scoped artifact when the requesting agent is NOT placed on that task", async () => {
    const a = await register();
    const b = await register();
    const taskA = placeTaskOn(a.agentId);

    const bytes = "# a task-scoped skill for A only";
    const up = await admin(`/config/artifacts?${new URLSearchParams({ name: "task-skill.md", kind: "file" })}`, {
      method: "POST",
      headers: { "content-type": "text/markdown" },
      body: bytes,
    });
    const sha = (await up.json() as { data: { sha256: string } }).data.sha256;

    // Declare it in A's TASK scope.
    expect((await admin("/config/entries", {
      method: "PUT",
      body: JSON.stringify({ scope: taskA, entries: [{ name: "task-skill.md", sha256: sha, kind: "file" }] }),
    })).status).toBe(200);

    // A (placed on the task) may fetch it; B (not placed) may not.
    expect((await downloadBySha(a.connectToken, sha)).status).toBe(200);
    expect((await downloadBySha(b.connectToken, sha)).status).toBe(403);
  });
});
