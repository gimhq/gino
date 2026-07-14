import type { AppDatabase } from "@/db";
import type { FrameSender } from "@/protocol/outbox";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp } from "@/app";
import { loadConfigStrict } from "@/config";
import { listArtifacts } from "@/configstore";
import { createDb } from "@/db";
import { createLogger } from "@/shared/lib/logger";
import { createTask, emitTaskUpsert, getTask, setPlacement } from "@/tasks";

// M8/B6 end-to-end provisioning (host = single source of truth):
//   1. POST /admin/config/artifacts  → content-addressed store + config_artifacts
//   2. PUT  /admin/config/entries    → the desired set per scope (host-resolved target)
//   3. GLOBAL set → config.entries on register / GET /agent/config
//      TASK set   → task_upsert.data.entries (orchestrator seam filled)

const REGISTER = "reg-secret-prov";
const CHANNEL = "chan-secret-prov";
const stamp = `${process.pid}-${Date.now()}`;
const dbPath = resolve(tmpdir(), `gino-prov-${stamp}.db`);
const storeDir = resolve(tmpdir(), `gino-prov-store-${stamp}`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_STORE_DIR: storeDir,
  GINO_REGISTER_TOKEN: REGISTER,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: AppDatabase;
let app: ReturnType<typeof createApp>;

/** Offline sender: enqueue persists to the outbox regardless (we read it back). */
const sender: FrameSender = { isOnline: () => false, send: () => false };

beforeAll(() => {
  db = createDb(config.DB_PATH);
  app = createApp({ config, db, logger: createLogger(config) });
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

const admin = { authorization: `Bearer ${CHANNEL}` };

function uploadArtifact(name: string, kind: string, bytes: string, contentType?: string) {
  const q = new URLSearchParams({ name, kind, ...(contentType ? { content_type: contentType } : {}) });
  return app.fetch(new Request(`http://local/api/admin/config/artifacts?${q}`, { method: "POST", headers: admin, body: bytes }));
}

function putEntries(scope: string, entries: unknown[], engine?: string) {
  return app.fetch(new Request("http://local/api/admin/config/entries", {
    method: "PUT",
    headers: { ...admin, "content-type": "application/json" },
    body: JSON.stringify({ scope, ...(engine ? { engine } : {}), entries }),
  }));
}

function register(capabilities: string[]) {
  return app.fetch(new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER, protocolVersion: 2, source: "manual", capabilities }),
  }));
}

describe("admin config — artifact upload (content-addressed + deduped)", () => {
  it("stores a file + a dir/skill artifact, deduping a re-upload by sha256", async () => {
    const a = await uploadArtifact("CLAUDE.md", "file", "# global instructions", "text/markdown");
    expect(a.status).toBe(201);
    const fileSha = (await a.json() as { data: { sha256: string; kind: string } }).data;
    expect(fileSha.kind).toBe("file");

    const b = await uploadArtifact("reviewer", "dir", "skill-bundle-bytes", "application/gzip");
    expect(b.status).toBe(201);
    expect((await b.json() as { data: { kind: string } }).data.kind).toBe("dir");

    // Re-upload identical (name, content) → dedup, no second row.
    const again = await uploadArtifact("CLAUDE.md", "file", "# global instructions", "text/markdown");
    expect(again.status).toBe(201);
    expect((await again.json() as { data: { sha256: string } }).data.sha256).toBe(fileSha.sha256);
    expect(listArtifacts(db).filter(x => x.name === "CLAUDE.md")).toHaveLength(1);
  });

  it("rejects a bad kind / missing metadata (400)", async () => {
    expect((await uploadArtifact("x", "weird", "y")).status).toBe(400);
    const noName = await app.fetch(new Request("http://local/api/admin/config/artifacts", { method: "POST", headers: admin, body: "z" }));
    expect(noName.status).toBe(400);
  });
});

describe("global delivery — register / GET /agent/config carry config.entries", () => {
  it("a registering agent receives the resolved GLOBAL set for its engine", async () => {
    const clHash = "a".repeat(64);
    const skHash = "b".repeat(64);
    await uploadArtifact("CLAUDE.md", "file", "# global instructions", "text/markdown");
    await uploadArtifact("reviewer", "dir", "skill-bundle-bytes", "application/gzip");
    const put = await putEntries("global", [
      { name: "CLAUDE.md", sha256: clHash, kind: "file" },
      { name: "reviewer", sha256: skHash, kind: "dir" },
    ]);
    expect(put.status).toBe(200);
    // A GLOBAL write mints a monotonic version (HOST-M8-01) and reports it.
    const putBody = await put.json() as { data: { version: number } };
    expect(putBody.data.version).toBeGreaterThan(0);

    const reg = await register(["claude"]);
    expect(reg.status).toBe(200);
    const body = await reg.json() as { agentId: string; connectToken: string; config: { entries: { target: string }[]; version: number } };
    expect(body.config.entries.map(e => e.target).sort()).toEqual([".claude/skills/reviewer", "CLAUDE.md"]);
    expect(body.config.version).toBe(putBody.data.version);

    // The same set is mirrored on GET /agent/config (additive; agent reconciles).
    const cfg = await app.fetch(new Request(`http://local/api/agent/config/${body.agentId}`, { headers: { "x-internal-token": body.connectToken } }));
    expect(cfg.status).toBe(200);
    const cfgBody = await cfg.json() as { entries: { target: string }[]; version: number };
    expect(cfgBody.entries.map(e => e.target).sort()).toEqual([".claude/skills/reviewer", "CLAUDE.md"]);
    expect(cfgBody.version).toBe(putBody.data.version);
  });

  it("every global PUT advances the version; a task-scope PUT does not", async () => {
    const v1 = (await (await putEntries("global", [{ name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" }])).json() as { data: { version: number } }).data.version;
    const v2 = (await (await putEntries("global", [{ name: "CLAUDE.md", sha256: "c".repeat(64), kind: "file" }])).json() as { data: { version: number } }).data.version;
    expect(v2).toBe(v1 + 1);

    // Task scope: no version minted (the response carries none).
    const reg = await register(["claude"]);
    const { agentId } = await reg.json() as { agentId: string };
    const task = createTask(db, { ownerId: "owner-v", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);
    const taskPut = await (await putEntries(task.id, [{ name: "reviewer", sha256: "b".repeat(64), kind: "dir" }])).json() as { data: { version?: number } };
    expect(taskPut.data.version).toBeUndefined();
  });

  it("a codex agent receives codex-resolved targets, never another engine's", async () => {
    // A multi-engine global pair: explicit CLAUDE.md + AGENTS.md targets.
    await putEntries("global", [
      { name: "CLAUDE.md", sha256: "a".repeat(64), target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: "a".repeat(64), target: "AGENTS.md", kind: "file" },
    ]);
    const reg = await register(["codex"]);
    const body = await reg.json() as { config: { entries: { target: string }[] } };
    expect(body.config.entries.map(e => e.target)).toEqual(["AGENTS.md"]);
  });
});

describe("task delivery — task_upsert.data.entries (orchestrator seam filled)", () => {
  it("a task carries its scope's resolved entries on task_upsert", async () => {
    // Register an agent to place the task on.
    const reg = await register(["claude"]);
    const { agentId } = await reg.json() as { agentId: string };

    const task = createTask(db, { ownerId: "owner-1", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);

    // Declare the task-scope desired set via the admin surface (scope = task id).
    const put = await putEntries(task.id, [{ name: "reviewer", sha256: "b".repeat(64), kind: "dir" }]);
    expect(put.status).toBe(200);

    // The orchestrator resolves config_entries WHERE scope=<task_id> into the frame.
    const row = emitTaskUpsert(db, sender, getTask(db, task.id)!);
    const data = row.dataJson as { entries?: { name: string; sha256: string; target: string; kind: string }[] };
    expect(data.entries).toEqual([{ name: "reviewer", sha256: "b".repeat(64), target: ".claude/skills/reviewer", kind: "dir" }]);
  });

  it("a task with no declared set emits no entries (backward-compatible)", async () => {
    const reg = await register(["claude"]);
    const { agentId } = await reg.json() as { agentId: string };
    const task = createTask(db, { ownerId: "owner-2", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);
    const row = emitTaskUpsert(db, sender, getTask(db, task.id)!);
    expect((row.dataJson as { entries?: unknown }).entries).toBeUndefined();
  });
});
