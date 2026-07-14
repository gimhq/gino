import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { createDb } from "@/db";
import {
  agentGrants,
  agents,
  attachments,
  channelBindings,
  channelCursors,
  channels,
  configArtifacts,
  configEntries,
  crons,
  messages,
  outbox,
  routerState,
  tasks,
  threadRuns,
} from "@/db/schema";
import { ulid } from "@/shared/lib/id";

// The 14 §6 tables (docs/host-v2-design.md). The schema is "clean v2": no v1
// legacy tables (scheduled_tasks / task_run_logs / sessions / registered_groups).
const EXPECTED_TABLES = [
  "agents",
  "agent_grants",
  "tasks",
  "crons",
  "thread_runs",
  "messages",
  "attachments",
  "outbox",
  "channels",
  "channel_bindings",
  "config_artifacts",
  "config_entries",
  "channel_cursors",
  "router_state",
] as const;

const LEGACY_TABLES = ["scheduled_tasks", "task_run_logs", "sessions", "registered_groups"];

const dbPath = resolve(tmpdir(), `gino-db-schema-${process.pid}-${Date.now()}.db`);
let db: ReturnType<typeof createDb>;

beforeAll(() => {
  // createDb boots bun:sqlite, applies PRAGMAs (foreign_keys ON), and migrates.
  db = createDb(dbPath);
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

function tableNames(): Set<string> {
  const rows = db.all(
    sql`SELECT name FROM sqlite_master WHERE type = 'table'`,
  ) as { name: string }[];
  return new Set(rows.map(r => r.name));
}

describe("v2 host schema (§6)", () => {
  it("migrates a fresh db and creates all 14 §6 tables", () => {
    const names = tableNames();
    for (const t of EXPECTED_TABLES) {
      expect(names.has(t)).toBe(true);
    }
    // Exactly the §6 set, plus drizzle's bookkeeping table — no extras.
    const appTables = [...names].filter(n => !n.startsWith("__drizzle") && !n.startsWith("sqlite_"));
    expect(appTables.sort()).toEqual([...EXPECTED_TABLES].sort());
  });

  it("carries NO v1 legacy tables", () => {
    const names = tableNames();
    for (const t of LEGACY_TABLES) {
      expect(names.has(t)).toBe(false);
    }
  });

  it("foreign_keys enforcement is ON", () => {
    const fk = db.all(sql`PRAGMA foreign_keys`) as { foreign_keys: number }[];
    expect(fk[0]?.foreign_keys).toBe(1);
  });

  it("round-trips an agent with a JSON capabilities column", () => {
    const id = ulid();
    db.insert(agents).values({
      id,
      source: "manual",
      access: "dedicated",
      connectTokenHash: "hash-abc",
      capabilities: { engines: ["claude"], max_threads: 4 },
      status: "enrolled",
    }).run();

    const row = db.select().from(agents).where(sql`${agents.id} = ${id}`).get();
    expect(row?.capabilities).toEqual({ engines: ["claude"], max_threads: 4 });
    expect(row?.activeThreads).toBe(0); // SQL default
    expect(typeof row?.createdAt).toBe("number"); // $defaultFn epoch-ms
  });

  it("enforces the agent_grants composite PK and agent FK", () => {
    const agentId = ulid();
    db.insert(agents).values({
      id: agentId,
      source: "manual",
      access: "dedicated",
      connectTokenHash: "h",
      capabilities: { engines: [] },
      status: "enrolled",
    }).run();

    db.insert(agentGrants).values({ agentId, ownerId: "owner-1" }).run();
    // Duplicate (agent_id, owner_id) violates the composite PK.
    expect(() => db.insert(agentGrants).values({ agentId, ownerId: "owner-1" }).run()).toThrow();
    // A grant for a non-existent agent violates the FK.
    expect(() =>
      db.insert(agentGrants).values({ agentId: "missing", ownerId: "owner-2" }).run(),
    ).toThrow();
  });

  it("enforces the tasks → agents FK (NULL agent_id allowed = pending placement)", () => {
    const agentId = ulid();
    db.insert(agents).values({
      id: agentId,
      source: "manual",
      access: "public",
      connectTokenHash: "h2",
      capabilities: { engines: ["claude"] },
      status: "online",
    }).run();

    // Placed task — FK satisfied.
    const placed = ulid();
    db.insert(tasks).values({
      id: placed,
      ownerId: "owner-1",
      engine: "claude",
      model: "opus",
      status: "active",
      agentId,
    }).run();
    expect(db.select().from(tasks).where(sql`${tasks.id} = ${placed}`).get()?.agentId).toBe(agentId);

    // Pending task — NULL agent_id is allowed.
    db.insert(tasks).values({
      id: ulid(),
      ownerId: "owner-1",
      engine: "claude",
      model: "opus",
      status: "pending",
    }).run();

    // Dangling placement — FK violation.
    expect(() =>
      db.insert(tasks).values({
        id: ulid(),
        ownerId: "owner-1",
        engine: "claude",
        model: "opus",
        status: "active",
        agentId: "no-such-agent",
      }).run(),
    ).toThrow();
  });

  it("enforces the messages (thread_id, seq) unique key", () => {
    const taskId = ulid();
    const threadId = ulid();
    db.insert(messages).values({ taskId, threadId, seq: 1, kind: "user", content: "hi" }).run();
    db.insert(messages).values({ taskId, threadId, seq: 2, kind: "assistant", content: "yo" }).run();
    // Same (thread_id, seq) collides on the unique index.
    expect(() =>
      db.insert(messages).values({ taskId, threadId, seq: 1, kind: "user", content: "dup" }).run(),
    ).toThrow();
    const count = db.all(
      sql`SELECT count(*) AS c FROM messages WHERE thread_id = ${threadId}`,
    ) as { c: number }[];
    expect(count[0]?.c).toBe(2);
  });

  it("enforces the config_entries UNIQUE(scope, target)", () => {
    db.insert(configEntries).values({
      scope: "global",
      name: "AGENTS.md",
      sha256: "aaa",
      target: "AGENTS.md",
      kind: "skill",
    }).run();
    // Same (scope, target) — even with a different sha — violates UNIQUE.
    expect(() =>
      db.insert(configEntries).values({
        scope: "global",
        name: "AGENTS.md",
        sha256: "bbb",
        target: "AGENTS.md",
        kind: "skill",
      }).run(),
    ).toThrow();
    // Different scope is fine.
    db.insert(configEntries).values({
      scope: "task-123",
      name: "AGENTS.md",
      sha256: "aaa",
      target: "AGENTS.md",
      kind: "skill",
    }).run();
  });

  it("auto-increments outbox.id and stores data_json", () => {
    const agentId = ulid();
    db.insert(agents).values({
      id: agentId,
      source: "docker",
      access: "public",
      connectTokenHash: "h3",
      capabilities: { engines: ["claude"] },
      status: "online",
    }).run();

    const first = db.insert(outbox).values({
      agentId,
      type: "task.assign",
      dataJson: { taskId: "t1" },
    }).returning().get();
    const second = db.insert(outbox).values({
      agentId,
      type: "task.cancel",
      dataJson: { taskId: "t1" },
    }).returning().get();

    expect(typeof first?.id).toBe("number");
    expect(second!.id).toBeGreaterThan(first!.id);
    expect(first?.dataJson).toEqual({ taskId: "t1" });
  });

  it("carries the outbox replay + msg_ack indexes (HOST-M9-07)", () => {
    const rows = db.all(
      sql`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'outbox'`,
    ) as { name: string }[];
    const names = new Set(rows.map(r => r.name));
    expect(names.has("outbox_agent_unacked_idx")).toBe(true);
    expect(names.has("outbox_msg_ack_idx")).toBe(true);
  });

  it("accepts representative rows for the remaining §6 tables", () => {
    // crons / thread_runs → tasks; channels → channel_bindings/cursors;
    // attachments; config_artifacts; router_state. Exercises every table's
    // primary write path against the migrated schema.
    const taskId = ulid();
    db.insert(tasks).values({
      id: taskId,
      ownerId: "o",
      engine: "claude",
      model: "opus",
      status: "active",
    }).run();

    const cronId = ulid();
    db.insert(crons).values({
      id: cronId,
      taskId,
      kind: "interval",
      prompt: "tick",
      continuity: "fresh",
    }).run();

    const threadId = ulid();
    db.insert(threadRuns).values({
      threadId,
      taskId,
      cronId,
      source: "cron",
      status: "running",
    }).run();

    db.insert(attachments).values({
      id: ulid(),
      taskId,
      threadId,
      name: "out.txt",
      contentType: "text/plain",
      size: 12,
      sha256: "deadbeef",
      ref: "blob://deadbeef",
      direction: "outbound",
    }).run();

    const channelId = ulid();
    db.insert(channels).values({
      id: channelId,
      name: "ops",
      type: "slack",
      ownerId: "o",
      config: { workspace: "T1" },
    }).run();
    db.insert(channelBindings).values({ channelId, conversationRef: "C1", taskId }).run();
    db.insert(channelCursors).values({ channelId, taskId, lastSeq: 0 }).run();

    db.insert(configArtifacts).values({
      name: "AGENTS.md",
      sha256: "aaa",
      kind: "skill",
      size: 3,
      storeRef: "store://aaa",
    }).run();

    db.insert(routerState).values({ key: "epoch", value: "1" }).run();

    // Spot-check the cross-table read path.
    expect(db.select().from(crons).where(sql`${crons.id} = ${cronId}`).get()?.taskId).toBe(taskId);
    expect(db.select().from(routerState).get()?.value).toBe("1");
  });
});
