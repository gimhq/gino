import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { createDb } from "@/db";
import { crons, tasks } from "@/db/schema";
import {
  getCron,
  getTaskThreads,
  getThreadRun,
  getTranscript,
  writeAttachmentDescriptors,
  writeCronPrompt,
  writeCronState,
  writeThreadMessage,
  writeThreadResult,
} from "@/msgstore";
import { ulid } from "@/shared/lib/id";

// M5 is the SINK: writers persist ONLY what an inbound frame carries, are
// idempotent (at-least-once replay can re-deliver any frame), and never touch
// channels/delivery. These tests exercise a real migrated bun:sqlite db (the
// same path createDb takes in production) against every writer + read helper.

const dbPath = resolve(tmpdir(), `gino-msgstore-${process.pid}-${Date.now()}.db`);
let db: ReturnType<typeof createDb>;

beforeAll(() => {
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

/** thread_runs / crons carry a FK to tasks — seed one and return its id. */
function seedTask(): string {
  const taskId = ulid();
  db.insert(tasks).values({
    id: taskId,
    ownerId: "owner-1",
    engine: "claude",
    model: "opus",
    status: "active",
  }).run();
  return taskId;
}

function countMessages(threadId: string): number {
  const rows = db.all(
    sql`SELECT count(*) AS c FROM messages WHERE thread_id = ${threadId}`,
  ) as { c: number }[];
  return rows[0]?.c ?? 0;
}

describe("writeThreadMessage → messages (transcript)", () => {
  it("orders by seq regardless of arrival order", () => {
    const taskId = seedTask();
    const threadId = ulid();
    // Arrive out of order: 2 before 1.
    writeThreadMessage(db, { taskId, threadId, data: { seq: 2, kind: "assistant_text", content: "second" } });
    writeThreadMessage(db, { taskId, threadId, data: { seq: 1, kind: "user_input", content: "first" } });
    writeThreadMessage(db, { taskId, threadId, data: { seq: 3, kind: "status", content: "done" } });

    const transcript = getTranscript(db, threadId);
    expect(transcript.map(m => m.seq)).toEqual([1, 2, 3]);
    expect(transcript.map(m => m.content)).toEqual(["first", "second", "done"]);
  });

  it("is idempotent on (thread_id, seq): a replay neither duplicates nor errors", () => {
    const taskId = seedTask();
    const threadId = ulid();
    writeThreadMessage(db, { taskId, threadId, data: { seq: 1, kind: "user_input", content: "hi" } });
    // Replay the identical frame — at-least-once delivery.
    expect(() =>
      writeThreadMessage(db, { taskId, threadId, data: { seq: 1, kind: "user_input", content: "hi" } }),
    ).not.toThrow();
    expect(countMessages(threadId)).toBe(1);
  });

  it("dedupes strictly on (thread_id, seq): first write wins, no overwrite", () => {
    const taskId = seedTask();
    const threadId = ulid();
    writeThreadMessage(db, { taskId, threadId, data: { seq: 5, kind: "assistant_text", content: "original" } });
    // A re-sent seq with DIFFERENT content must be ignored (not overwrite).
    writeThreadMessage(db, { taskId, threadId, data: { seq: 5, kind: "assistant_text", content: "tampered" } });
    expect(countMessages(threadId)).toBe(1);
    expect(getTranscript(db, threadId)[0]?.content).toBe("original");
  });

  it("round-trips tool_use JSON input/output and persists task_id", () => {
    const taskId = seedTask();
    const threadId = ulid();
    writeThreadMessage(db, {
      taskId,
      threadId,
      data: { seq: 1, kind: "tool_use", role: "assistant", tool: "bash", input: { cmd: "ls", args: ["-la"] } },
    });
    writeThreadMessage(db, {
      taskId,
      threadId,
      data: { seq: 2, kind: "tool_result", tool: "bash", output: { code: 0, lines: 3 } },
    });
    const [a, b] = getTranscript(db, threadId);
    expect(a?.taskId).toBe(taskId);
    expect(a?.tool).toBe("bash");
    expect(a?.input).toEqual({ cmd: "ls", args: ["-la"] });
    expect(b?.output).toEqual({ code: 0, lines: 3 });
  });
});

describe("writeThreadResult → thread_runs (UPSERT by thread_id)", () => {
  it("inserts a run and stores the terminal mirror fields", () => {
    const taskId = seedTask();
    const threadId = ulid();
    writeThreadResult(db, {
      taskId,
      threadId,
      data: { status: "succeeded", result: "ok", engine_session_id: "sess-1", duration_ms: 1234, final_seq: 9 },
    });
    const run = getThreadRun(db, threadId);
    expect(run?.status).toBe("succeeded");
    expect(run?.engineSessionId).toBe("sess-1");
    expect(run?.durationMs).toBe(1234);
    expect(run?.finalSeq).toBe(9);
    expect(run?.source).toBe("chat"); // default context
    expect(typeof run?.finishedAt).toBe("number");
  });

  it("carries explicit source + cronId context from the dispatch", () => {
    const taskId = seedTask();
    const cronId = ulid();
    db.insert(crons).values({ id: cronId, taskId, kind: "interval", prompt: "tick", continuity: "fresh" }).run();
    const threadId = ulid();
    writeThreadResult(db, {
      taskId,
      threadId,
      source: "cron",
      cronId,
      data: { status: "succeeded", duration_ms: 10, final_seq: 1 },
    });
    const run = getThreadRun(db, threadId);
    expect(run?.source).toBe("cron");
    expect(run?.cronId).toBe(cronId);
  });

  it("is idempotent: an identical replay leaves the row untouched", () => {
    const taskId = seedTask();
    const threadId = ulid();
    const frame = {
      taskId,
      threadId,
      data: { status: "succeeded" as const, engine_session_id: "s", duration_ms: 50, final_seq: 4 },
    };
    writeThreadResult(db, frame);
    const first = getThreadRun(db, threadId);
    writeThreadResult(db, frame); // replay
    const second = getThreadRun(db, threadId);
    expect(second).toEqual(first); // every column preserved (incl. timestamps)
    expect(countThreadRuns(threadId)).toBe(1);
  });

  it("overwrites the terminal fields on a state change (never a dup row)", () => {
    const taskId = seedTask();
    const threadId = ulid();
    writeThreadResult(db, { taskId, threadId, data: { status: "succeeded", duration_ms: 5, final_seq: 2 } });
    writeThreadResult(db, {
      taskId,
      threadId,
      data: { status: "failed", failure_reason: "agent.timeout", duration_ms: 7, final_seq: 3 },
    });
    const run = getThreadRun(db, threadId);
    expect(run?.status).toBe("failed");
    expect(run?.failureReason).toBe("agent.timeout");
    expect(run?.finalSeq).toBe(3);
    expect(countThreadRuns(threadId)).toBe(1);
  });
});

function countThreadRuns(threadId: string): number {
  const rows = db.all(
    sql`SELECT count(*) AS c FROM thread_runs WHERE thread_id = ${threadId}`,
  ) as { c: number }[];
  return rows[0]?.c ?? 0;
}

describe("writeCronState → crons (read-only mirror, UPSERT by id)", () => {
  it("inserts a mirror, coercing next_run to epoch-ms", () => {
    const taskId = seedTask();
    const cronId = ulid();
    writeCronState(db, {
      taskId,
      cronId,
      data: {
        kind: "interval",
        schedule_type: "every",
        schedule_value: "5m",
        continuity: "fresh",
        next_run: "1700000000000",
        status: "scheduled",
        round_count: 0,
        skipped_count: 0,
      },
    });
    const cron = getCron(db, cronId);
    expect(cron?.kind).toBe("interval");
    expect(cron?.scheduleValue).toBe("5m");
    expect(cron?.continuity).toBe("fresh");
    expect(cron?.nextRun).toBe(1700000000000);
    expect(cron?.roundCount).toBe(0);
  });

  it("coerces an ISO next_run to epoch-ms and a non-parseable one to null", () => {
    const taskId = seedTask();
    const iso = ulid();
    writeCronState(db, {
      taskId,
      cronId: iso,
      data: { kind: "cron", continuity: "fresh", next_run: "2026-01-01T00:00:00.000Z", round_count: 0, skipped_count: 0 },
    });
    expect(getCron(db, iso)?.nextRun).toBe(Date.parse("2026-01-01T00:00:00.000Z"));

    const bad = ulid();
    writeCronState(db, {
      taskId,
      cronId: bad,
      data: { kind: "cron", continuity: "fresh", next_run: "not-a-date", round_count: 0, skipped_count: 0 },
    });
    expect(getCron(db, bad)?.nextRun).toBeNull();
  });

  it("reads a timezone-less ISO next_run as UTC regardless of host TZ (HOST-XC-01)", () => {
    const taskId = seedTask();
    const priorTz = process.env.TZ;
    try {
      process.env.TZ = "America/New_York"; // UTC-4/-5 — local parse would shift the epoch
      const tzLess = ulid();
      writeCronState(db, {
        taskId,
        cronId: tzLess,
        data: { kind: "cron", continuity: "fresh", next_run: "2026-07-10T09:00:00", round_count: 0, skipped_count: 0 },
      });
      expect(getCron(db, tzLess)?.nextRun).toBe(Date.UTC(2026, 6, 10, 9, 0, 0));

      // An explicit offset is still honored, not double-shifted.
      const offset = ulid();
      writeCronState(db, {
        taskId,
        cronId: offset,
        data: { kind: "cron", continuity: "fresh", next_run: "2026-07-10T09:00:00+02:00", round_count: 0, skipped_count: 0 },
      });
      expect(getCron(db, offset)?.nextRun).toBe(Date.UTC(2026, 6, 10, 7, 0, 0));
    }
    finally {
      if (priorTz === undefined) {
        delete process.env.TZ;
      }
      else {
        process.env.TZ = priorTz;
      }
    }
  });

  it("mirrors updated round_count/status and preserves prompt across updates", () => {
    const taskId = seedTask();
    const cronId = ulid();
    const base = {
      kind: "interval",
      continuity: "fresh",
      status: "scheduled",
      round_count: 0,
      skipped_count: 0,
    };
    writeCronState(db, { taskId, cronId, data: { ...base } });
    // A later mirror with advanced counters + new status.
    writeCronState(db, {
      taskId,
      cronId,
      data: { ...base, status: "running", last_status: "succeeded", round_count: 3, skipped_count: 1 },
    });
    const cron = getCron(db, cronId);
    expect(cron?.roundCount).toBe(3);
    expect(cron?.skippedCount).toBe(1);
    expect(cron?.status).toBe("running");
    expect(cron?.lastStatus).toBe("succeeded");
    expect(cron?.prompt).toBe(""); // mirror never carries/overwrites the command
    expect(cron?.taskId).toBe(taskId); // placement preserved
    expect(countCrons(cronId)).toBe(1);
  });

  it("is idempotent on the cron id (no dup rows; values stable bar updated_at)", () => {
    const taskId = seedTask();
    const cronId = ulid();
    const data = {
      kind: "cron",
      schedule_type: "cron",
      schedule_value: "0 * * * *",
      continuity: "shared",
      status: "scheduled",
      round_count: 2,
      skipped_count: 0,
    };
    writeCronState(db, { taskId, cronId, data });
    const { updatedAt: _a, ...first } = getCron(db, cronId)!;
    writeCronState(db, { taskId, cronId, data }); // replay
    const { updatedAt: _b, ...second } = getCron(db, cronId)!;
    expect(second).toEqual(first);
    expect(countCrons(cronId)).toBe(1);
  });
});

function countCrons(cronId: string): number {
  const rows = db.all(
    sql`SELECT count(*) AS c FROM crons WHERE id = ${cronId}`,
  ) as { c: number }[];
  return rows[0]?.c ?? 0;
}

describe("writeCronPrompt → crons.prompt (dispatch-side write, HOST-M5M6-09)", () => {
  it("seeds the prompt before the first cron_state; the mirror preserves it", () => {
    const taskId = seedTask();
    const cronId = ulid();
    writeCronPrompt(db, { taskId, cronId, prompt: "send the daily digest" });
    expect(getCron(db, cronId)?.prompt).toBe("send the daily digest");

    writeCronState(db, {
      taskId,
      cronId,
      data: { kind: "schedule", continuity: "isolated", status: "scheduled", round_count: 1, skipped_count: 0 },
    });
    const cron = getCron(db, cronId);
    expect(cron?.prompt).toBe("send the daily digest");
    expect(cron?.kind).toBe("schedule");
    expect(cron?.roundCount).toBe(1);
    expect(countCrons(cronId)).toBe(1);
  });

  it("updates only the prompt when the mirror row already exists", () => {
    const taskId = seedTask();
    const cronId = ulid();
    writeCronState(db, {
      taskId,
      cronId,
      data: { kind: "interval", schedule_type: "every", schedule_value: "5m", continuity: "fresh", round_count: 2, skipped_count: 0 },
    });
    writeCronPrompt(db, { taskId, cronId, prompt: "rotated command" });
    const cron = getCron(db, cronId);
    expect(cron?.prompt).toBe("rotated command");
    expect(cron?.kind).toBe("interval");
    expect(cron?.scheduleValue).toBe("5m");
    expect(cron?.roundCount).toBe(2);
    expect(countCrons(cronId)).toBe(1);
  });
});

describe("writeAttachmentDescriptors → attachments (idempotent on id)", () => {
  it("inserts descriptors, defaulting direction to outbound", () => {
    const taskId = seedTask();
    const threadId = ulid();
    const a = ulid();
    const b = ulid();
    writeAttachmentDescriptors(db, {
      taskId,
      threadId,
      descriptors: [
        { id: a, name: "out.txt", content_type: "text/plain", size: 12, sha256: "deadbeef", ref: "blob://deadbeef" },
        { id: b, name: "in.png", content_type: "image/png", size: 99, sha256: "cafe", ref: "blob://cafe", direction: "inbound" },
      ],
    });
    const rows = db.all(
      sql`SELECT id, direction FROM attachments WHERE id IN (${a}, ${b}) ORDER BY id`,
    ) as { id: string; direction: string }[];
    const byId = new Map(rows.map(r => [r.id, r.direction]));
    expect(byId.get(a)).toBe("outbound"); // defaulted
    expect(byId.get(b)).toBe("inbound"); // honored
  });

  it("is idempotent on id: a replay neither duplicates nor overwrites", () => {
    const taskId = seedTask();
    const threadId = ulid();
    const id = ulid();
    writeAttachmentDescriptors(db, {
      taskId,
      threadId,
      descriptors: [{ id, name: "orig.txt", content_type: "text/plain", size: 1, sha256: "a", ref: "blob://a" }],
    });
    // Replay with the SAME id but tampered fields — must be ignored.
    writeAttachmentDescriptors(db, {
      taskId,
      threadId,
      descriptors: [{ id, name: "tampered.txt", content_type: "text/plain", size: 2, sha256: "b", ref: "blob://b" }],
    });
    const rows = db.all(
      sql`SELECT name FROM attachments WHERE id = ${id}`,
    ) as { name: string }[];
    expect(rows.length).toBe(1);
    expect(rows[0]?.name).toBe("orig.txt");
  });

  it("no-ops on an empty descriptor list", () => {
    const taskId = seedTask();
    const threadId = ulid();
    expect(() => writeAttachmentDescriptors(db, { taskId, threadId, descriptors: [] })).not.toThrow();
  });
});

describe("read models", () => {
  it("getTaskThreads lists a task's threads ordered by first appearance", () => {
    const taskId = seedTask();
    const t1 = ulid();
    const t2 = ulid();
    // t1 gets a message first, then t2 — getTaskThreads orders by earliest.
    writeThreadMessage(db, { taskId, threadId: t1, data: { seq: 1, kind: "user_input", content: "a" } });
    writeThreadMessage(db, { taskId, threadId: t2, data: { seq: 1, kind: "user_input", content: "b" } });
    // A thread known only via thread_runs is still surfaced.
    const t3 = ulid();
    writeThreadResult(db, { taskId, threadId: t3, data: { status: "succeeded", duration_ms: 1, final_seq: 1 } });

    const threads = getTaskThreads(db, taskId);
    expect(threads).toContain(t1);
    expect(threads).toContain(t2);
    expect(threads).toContain(t3);
    expect(threads.indexOf(t1)).toBeLessThan(threads.indexOf(t2));
  });

  it("getThreadRun / getCron return undefined when nothing has been mirrored", () => {
    expect(getThreadRun(db, ulid())).toBeUndefined();
    expect(getCron(db, ulid())).toBeUndefined();
  });

  it("getTranscript is scoped to one thread", () => {
    const taskId = seedTask();
    const mine = ulid();
    const other = ulid();
    writeThreadMessage(db, { taskId, threadId: mine, data: { seq: 1, kind: "user_input", content: "mine" } });
    writeThreadMessage(db, { taskId, threadId: other, data: { seq: 1, kind: "user_input", content: "other" } });
    const transcript = getTranscript(db, mine);
    expect(transcript.length).toBe(1);
    expect(transcript[0]?.content).toBe("mine");
  });
});
