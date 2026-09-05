import type { AppDatabase } from "@/db";
import type { AgentTransport } from "@/host/ports/transport";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { getEntries, listArtifacts } from "@/configstore";
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
  // HOST-XC-02: explicit same-secret reuse so one bearer drives both surfaces in this test.
  GINO_HTTP_ADMIN_TOKEN: CHANNEL,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: AppDatabase;
let app: ReturnType<typeof createApp>;

/** Offline sender: enqueue persists to the outbox regardless (we read it back). */
const sender: AgentTransport = { isOnline: () => false, send: () => false, closeAgent: () => false };

beforeAll(() => {
  db = createDb(config.DB_PATH);
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

const admin = { authorization: `Bearer ${CHANNEL}` };

function uploadArtifact(name: string, kind: string, bytes: string, contentType?: string) {
  const q = new URLSearchParams({ name, kind, ...(contentType ? { content_type: contentType } : {}) });
  return app.fetch(new Request(`http://local/api/admin/config/artifacts?${q}`, { method: "POST", headers: admin, body: bytes }));
}

/** Upload an artifact and return its real content sha256 (entries validate against the registry, HOST-M8-07). */
async function uploadSha(name: string, kind: string, bytes: string): Promise<string> {
  const res = await uploadArtifact(name, kind, bytes);
  expect(res.status).toBe(201);
  return ((await res.json()) as { data: { sha256: string } }).data.sha256;
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

  it("re-uploading identical bytes with a different kind updates the row (HOST-M8-13)", async () => {
    const first = await uploadArtifact("mislabelled", "file", "actually-a-bundle");
    expect(first.status).toBe(201);
    expect((await first.json() as { data: { kind: string } }).data.kind).toBe("file");

    // The corrective re-ingest wins — not silently ignored.
    const fixed = await uploadArtifact("mislabelled", "dir", "actually-a-bundle");
    expect(fixed.status).toBe(201);
    expect((await fixed.json() as { data: { kind: string } }).data.kind).toBe("dir");
    expect(listArtifacts(db).filter(x => x.name === "mislabelled")).toHaveLength(1);
  });

  it("rejects a bad kind / missing metadata (400)", async () => {
    expect((await uploadArtifact("x", "weird", "y")).status).toBe(400);
    const noName = await app.fetch(new Request("http://local/api/admin/config/artifacts", { method: "POST", headers: admin, body: "z" }));
    expect(noName.status).toBe(400);
  });
});

describe("global delivery — register / GET /agent/config carry config.entries", () => {
  it("a registering agent receives the resolved GLOBAL set for its engine", async () => {
    const clSha = await uploadSha("CLAUDE.md", "file", "# global instructions");
    const skSha = await uploadSha("reviewer", "dir", "skill-bundle-bytes");
    const put = await putEntries("global", [
      { name: "CLAUDE.md", sha256: clSha, kind: "file" },
      { name: "reviewer", sha256: skSha, kind: "dir" },
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
    const shaA = await uploadSha("ver-a.md", "file", "version bump a");
    const shaB = await uploadSha("ver-b.md", "file", "version bump b");
    const v1 = (await (await putEntries("global", [{ name: "ver-a.md", sha256: shaA, kind: "file" }])).json() as { data: { version: number } }).data.version;
    const v2 = (await (await putEntries("global", [{ name: "ver-b.md", sha256: shaB, kind: "file" }])).json() as { data: { version: number } }).data.version;
    expect(v2).toBe(v1 + 1);

    // Task scope: no version minted (the response carries none).
    const reg = await register(["claude"]);
    const { agentId } = await reg.json() as { agentId: string };
    const task = createTask(db, { ownerId: "owner-v", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);
    const skSha = await uploadSha("reviewer", "dir", "skill-bundle-bytes");
    const taskPut = await (await putEntries(task.id, [{ name: "reviewer", sha256: skSha, kind: "dir" }])).json() as { data: { version?: number } };
    expect(taskPut.data.version).toBeUndefined();
  });

  it("a codex agent receives codex-resolved targets, never another engine's", async () => {
    // A multi-engine global pair: explicit CLAUDE.md + AGENTS.md targets.
    const sha = await uploadSha("pair.md", "file", "multi-engine pair");
    await putEntries("global", [
      { name: "CLAUDE.md", sha256: sha, target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: sha, target: "AGENTS.md", kind: "file" },
    ]);
    const reg = await register(["codex"]);
    const body = await reg.json() as { config: { entries: { target: string }[] } };
    expect(body.config.entries.map(e => e.target)).toEqual(["AGENTS.md"]);
  });

  it("a MULTI-engine agent receives the union of its engines' targets (HOST-M8-03)", async () => {
    const sha = await uploadSha("pair.md", "file", "multi-engine pair");
    await putEntries("global", [
      { name: "CLAUDE.md", sha256: sha, target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: sha, target: "AGENTS.md", kind: "file" },
    ]);
    const reg = await register(["claude", "codex"]);
    expect(reg.status).toBe(200);
    const body = await reg.json() as { agentId: string; connectToken: string; config: { entries: { target: string }[] } };
    // NOT just engines[0]: both engines' rows are delivered on register…
    expect(body.config.entries.map(e => e.target).sort()).toEqual(["AGENTS.md", "CLAUDE.md"]);

    // …and on the config pull mirror.
    const cfg = await app.fetch(new Request(`http://local/api/agent/config/${body.agentId}`, { headers: { "x-internal-token": body.connectToken } }));
    const cfgBody = await cfg.json() as { entries: { target: string }[] };
    expect(cfgBody.entries.map(e => e.target).sort()).toEqual(["AGENTS.md", "CLAUDE.md"]);
  });
});

describe("entry write validation — the admin surface 422s bad desired sets", () => {
  it("rejects an entry whose sha256 was never ingested (HOST-M8-07)", async () => {
    const res = await putEntries("global", [{ name: "ghost.md", sha256: "e".repeat(64), kind: "file" }]);
    expect(res.status).toBe(422);
    const body = await res.json() as { error: { details: { problems: string[] } } };
    expect(body.error.details.problems.join(" ")).toContain("e".repeat(64));
  });

  it("rejects a scope that is neither 'global' nor an existing task id (HOST-M8-07)", async () => {
    const sha = await uploadSha("pair.md", "file", "multi-engine pair");
    const res = await putEntries("no-such-task", [{ name: "pair.md", sha256: sha, kind: "file" }]);
    expect(res.status).toBe(422);
  });

  it("rejects traversal-shaped targets and names (HOST-M8-08)", async () => {
    const sha = await uploadSha("pair.md", "file", "multi-engine pair");
    for (const target of ["../../home/user/.bashrc", "/etc/passwd", "C:\\windows\\system32\\evil", "\\\\host\\share\\x"]) {
      const res = await putEntries("global", [{ name: "pair.md", sha256: sha, target, kind: "file" }]);
      expect(res.status).toBe(422);
    }
    const badName = await putEntries("global", [{ name: "..", sha256: sha, kind: "dir" }]);
    expect(badName.status).toBe(422);
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
    const skSha = await uploadSha("reviewer", "dir", "skill-bundle-bytes");
    const put = await putEntries(task.id, [{ name: "reviewer", sha256: skSha, kind: "dir" }]);
    expect(put.status).toBe(200);

    // The orchestrator resolves config_entries WHERE scope=<task_id> into the frame.
    const row = emitTaskUpsert(db, sender, getTask(db, task.id)!);
    const data = row.dataJson as { entries?: { name: string; sha256: string; target: string; kind: string }[] };
    expect(data.entries).toEqual([{ name: "reviewer", sha256: skSha, target: ".claude/skills/reviewer", kind: "dir" }]);
  });

  it("a task with no declared set emits no entries (backward-compatible)", async () => {
    const reg = await register(["claude"]);
    const { agentId } = await reg.json() as { agentId: string };
    const task = createTask(db, { ownerId: "owner-2", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);
    const row = emitTaskUpsert(db, sender, getTask(db, task.id)!);
    expect((row.dataJson as { entries?: unknown }).entries).toBeUndefined();
  });

  it("closing a task via the admin surface drops its task-scope entries (HOST-M8-10)", async () => {
    const reg = await register(["claude"]);
    const { agentId } = await reg.json() as { agentId: string };
    const task = createTask(db, { ownerId: "owner-3", engine: "claude", model: "sonnet" });
    setPlacement(db, task.id, agentId);

    const skSha = await uploadSha("reviewer", "dir", "skill-bundle-bytes");
    expect((await putEntries(task.id, [{ name: "reviewer", sha256: skSha, kind: "dir" }])).status).toBe(200);
    expect(getEntries(db, task.id)).toHaveLength(1);

    const closed = await app.fetch(new Request(`http://local/api/admin/tasks/${task.id}/close`, { method: "POST", headers: admin }));
    expect(closed.status).toBe(200);

    // The orphaned scope no longer pins the artifact in the GC root set.
    expect(getEntries(db, task.id)).toHaveLength(0);
  });
});
