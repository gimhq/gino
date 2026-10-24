import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Storage } from "../../src/host/ports/storage";
import type { AgentTransport } from "../../src/host/ports/transport";
import type { Logger } from "../../src/logger";
import type { DecodedFrame } from "../../src/protocol/envelope";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { enrollAgent } from "../../src/agents/registry";
import { checkTranscriptCompleteness, getThreadRun, getTranscript, writeThreadMessage } from "../../src/msgstore";
import { applyFrame, handleInboundFrame, ingestHttpFrame, MalformedFrameError } from "../../src/protocol/inbound";
import { createTask, setPlacement } from "../../src/tasks";

const oid = (n: number): string => `01jab3x7k2m4n5p6q7r8s9${String(n).padStart(4, "0")}`;

// Inbound frame lifecycle regressions:
//   Poison policy: a PERMANENTLY unprocessable frame
//     (schema-invalid data, missing correlation ids, authorize denial) is
//     acked-and-dropped so the agent stops replaying it forever; a TRANSIENT
//     storage failure stays un-acked so the agent replays.
//   `ingestHttpFrame` throws the tagged `MalformedFrameError`
//     only for an undecodable frame; a storage failure propagates untagged
//     (the route maps only the tag to 400 — everything else is a 5xx retry).
//   `thread_result` ingest verifies the stored transcript
//     covers `1..=final_seq` and warn-logs a gap; it never blocks the ack.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** Enroll a fresh agent, create a task, and place it on that agent. */
function placedAgentTask(): { agentId: string; taskId: string } {
  const agentId = enrollAgent(storage, { capabilities: { engines: ["claude"] } }).agent.id;
  const taskId = createTask(storage, { ownerId: "owner", engine: "claude", model: "sonnet", status: "active" }).id;
  setPlacement(storage, taskId, agentId);
  return { agentId, taskId };
}

/** A Logger fake that records every warn/error payload. */
function spyLogger(): { logger: Logger; warns: unknown[][]; errors: unknown[][] } {
  const warns: unknown[][] = [];
  const errors: unknown[][] = [];
  const noop = (): void => {};
  const logger = {
    debug: noop,
    info: noop,
    warn: (...args: unknown[]) => warns.push(args),
    error: (...args: unknown[]) => errors.push(args),
    fatal: noop,
    flush: noop,
    reopen: noop,
  } as unknown as Logger;
  return { logger, warns, errors };
}

/** An AgentTransport fake that records every frame sent back to the agent. */
function spySender(): { sender: AgentTransport; sent: Record<string, unknown>[] } {
  const sent: Record<string, unknown>[] = [];
  const sender: AgentTransport = {
    isOnline: () => true,
    send: (_agentId, frame) => {
      sent.push(frame);
      return true;
    },
    closeAgent: () => false,
  };
  return { sender, sent };
}

describe("HOST-M1M2-04 — poison frames are acked-and-dropped; transient failures are not acked", () => {
  it("acks-and-drops a thread_message whose data fails the schema (agent stops replaying)", () => {
    const { agentId, taskId } = placedAgentTask();
    const { logger, errors } = spyLogger();
    // `seq` must be an int — a string makes the frame permanently unprocessable.
    const poison: DecodedFrame = { type: "thread_message", outboxId: oid(7), taskId, threadId: "thr-1", data: { seq: "not-a-number", kind: "assistant_text" } };

    const ack = applyFrame({ storage, agentId, logger }, poison);
    expect(ack).toEqual({ type: "ack", id: oid(7) }); // acked → outbox row clears, no replay
    expect(getTranscript(storage, "thr-1")).toHaveLength(0); // dropped, never written
    expect(errors.some(args => String(args[1]).includes("acked and dropped"))).toBe(true); // logged loudly
  });

  it("acks-and-drops a thread_result whose data fails the schema", () => {
    const { agentId, taskId } = placedAgentTask();
    const poison: DecodedFrame = { type: "thread_result", outboxId: oid(8), taskId, threadId: "thr-2", data: { status: "exploded", duration_ms: 1, final_seq: 1 } };

    expect(applyFrame({ storage, agentId }, poison)).toEqual({ type: "ack", id: oid(8) });
    expect(getThreadRun(storage, "thr-2")).toBeUndefined();
  });

  it("acks-and-drops a cron_state whose data fails the schema", () => {
    const { agentId, taskId } = placedAgentTask();
    const poison: DecodedFrame = { type: "cron_state", outboxId: oid(9), taskId, cronId: "cron-1", data: { kind: "interval", round_count: "NaN", skipped_count: 0 } };

    expect(applyFrame({ storage, agentId }, poison)).toEqual({ type: "ack", id: oid(9) });
  });

  it("acks-and-drops a store-write frame missing its correlation ids (permanently unroutable)", () => {
    const { agentId } = placedAgentTask();
    const poison: DecodedFrame = { type: "thread_message", outboxId: oid(10), data: { seq: 1, kind: "assistant_text", content: "x" } };

    expect(applyFrame({ storage, agentId }, poison)).toEqual({ type: "ack", id: oid(10) });
  });

  it("over WS, the poison ack reaches the sender exactly like a stored ack (replay loop ends)", () => {
    const { agentId, taskId } = placedAgentTask();
    const { sender, sent } = spySender();
    const raw = JSON.stringify({ type: "thread_message", outboxId: oid(11), taskId, threadId: "thr-3", data: { seq: false } });

    handleInboundFrame({ storage, agentId, sender }, raw);
    expect(sent).toEqual([{ type: "ack", id: oid(11) }]);
    expect(getTranscript(storage, "thr-3")).toHaveLength(0);
  });

  it("does NOT ack a valid frame when the storage write fails transiently (agent must replay)", () => {
    const { agentId, taskId } = placedAgentTask();
    const { sender, sent } = spySender();
    const { logger, warns } = spyLogger();
    db.run(sql`DROP TABLE messages`); // deterministic stand-in for a transient write failure
    const raw = JSON.stringify({ type: "thread_message", outboxId: oid(12), taskId, threadId: "thr-4", data: { seq: 1, kind: "assistant_text", content: "x" } });

    handleInboundFrame({ storage, agentId, sender, logger }, raw); // must not throw to the socket loop
    expect(sent).toHaveLength(0); // no ack → the agent keeps the frame and replays
    expect(warns.some(args => String(args[1]).includes("not acked"))).toBe(true);
  });

  it("stores an UNRECOGNIZED thread_message kind opaquely instead of poisoning it (version drift)", () => {
    const { agentId, taskId } = placedAgentTask();
    const { logger, warns } = spyLogger();
    const frame: DecodedFrame = { type: "thread_message", outboxId: oid(13), taskId, threadId: "thr-5", data: { seq: 1, kind: "usage", content: "tokens: 42" } };

    expect(applyFrame({ storage, agentId, logger }, frame)).toEqual({ type: "ack", id: oid(13) });
    expect(getTranscript(storage, "thr-5").map(m => m.kind)).toEqual(["usage"]); // written, not dropped
    expect(warns.some(args => String(args[1]).includes("unrecognized kind"))).toBe(true); // flagged
  });
});

describe("HOST-M1M2-09 — ingestHttpFrame tags only the malformed-frame error", () => {
  it("throws MalformedFrameError for an undecodable body (route maps this — and only this — to 400)", () => {
    const { agentId } = placedAgentTask();
    expect(() => ingestHttpFrame({ storage, agentId }, "not json at all")).toThrow(MalformedFrameError);
    expect(() => ingestHttpFrame({ storage, agentId }, JSON.stringify({ noType: true }))).toThrow(MalformedFrameError);
  });

  it("lets a storage failure propagate UNTAGGED (route rethrows → 5xx, agent retries)", () => {
    const { agentId, taskId } = placedAgentTask();
    db.run(sql`DROP TABLE messages`);
    const raw = JSON.stringify({ type: "thread_message", outboxId: oid(1), taskId, threadId: "thr-6", data: { seq: 1, kind: "assistant_text", content: "x" } });

    let thrown: unknown;
    try {
      ingestHttpFrame({ storage, agentId }, raw);
    }
    catch (err) {
      thrown = err;
    }
    expect(thrown).toBeDefined();
    expect(thrown).not.toBeInstanceOf(MalformedFrameError);
  });

  it("returns the poison ack (2xx) for a decodable frame with permanently bad data — same policy as WS", () => {
    const { agentId, taskId } = placedAgentTask();
    const raw = JSON.stringify({ type: "thread_result", outboxId: oid(2), taskId, threadId: "thr-7", data: { status: "exploded", duration_ms: 1, final_seq: 1 } });
    expect(ingestHttpFrame({ storage, agentId }, raw)).toEqual({ type: "ack", id: oid(2) });
  });
});

describe("HOST-M1M2-13 — final_seq completeness is verified on thread_result ingest", () => {
  const msg = (threadId: string, taskId: string, seq: number) =>
    writeThreadMessage(storage, { taskId, threadId, data: { seq, kind: "assistant_text", content: `m${seq}` } });

  it("warn-logs a transcript gap when a middle seq is missing, and still acks", () => {
    const { agentId, taskId } = placedAgentTask();
    const { logger, warns } = spyLogger();
    msg("thr-gap", taskId, 1);
    msg("thr-gap", taskId, 2);
    msg("thr-gap", taskId, 4); // hole at seq 3
    const frame: DecodedFrame = { type: "thread_result", outboxId: oid(20), taskId, threadId: "thr-gap", data: { status: "succeeded", duration_ms: 5, final_seq: 4 } };

    expect(applyFrame({ storage, agentId, logger }, frame)).toEqual({ type: "ack", id: oid(20) }); // never blocks
    const gapWarn = warns.find(args => String(args[1]).includes("transcript gap"));
    expect(gapWarn).toBeDefined();
    expect(gapWarn?.[0]).toMatchObject({ threadId: "thr-gap", messageCount: 3, maxSeq: 4, finalSeq: 4 });
  });

  it("warn-logs a gap when the tail is missing (max seq < final_seq)", () => {
    const { agentId, taskId } = placedAgentTask();
    const { logger, warns } = spyLogger();
    msg("thr-tail", taskId, 1);
    msg("thr-tail", taskId, 2);
    const frame: DecodedFrame = { type: "thread_result", taskId, threadId: "thr-tail", data: { status: "succeeded", duration_ms: 5, final_seq: 4 } };

    applyFrame({ storage, agentId, logger }, frame);
    expect(warns.some(args => String(args[1]).includes("transcript gap"))).toBe(true);
  });

  it("stays quiet on a complete transcript (exactly 1..=final_seq)", () => {
    const { agentId, taskId } = placedAgentTask();
    const { logger, warns } = spyLogger();
    msg("thr-ok", taskId, 1);
    msg("thr-ok", taskId, 2);
    msg("thr-ok", taskId, 3);
    const frame: DecodedFrame = { type: "thread_result", outboxId: oid(21), taskId, threadId: "thr-ok", data: { status: "succeeded", duration_ms: 5, final_seq: 3 } };

    expect(applyFrame({ storage, agentId, logger }, frame)).toEqual({ type: "ack", id: oid(21) });
    expect(warns.filter(args => String(args[1]).includes("transcript gap"))).toHaveLength(0);
  });

  it("checkTranscriptCompleteness: empty transcript is complete iff final_seq is 0", () => {
    expect(checkTranscriptCompleteness(storage, "thr-none", 0)).toMatchObject({ complete: true, messageCount: 0, maxSeq: null });
    expect(checkTranscriptCompleteness(storage, "thr-none", 2)).toMatchObject({ complete: false, messageCount: 0, maxSeq: null });
  });
});
