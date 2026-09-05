import type { AppDatabase } from "@/db";
import type { Storage } from "@/host/ports/storage";
import type { DecodedFrame } from "@/protocol/envelope";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent } from "@/agents/registry";
import { createDb } from "@/db";
import { getCron, getThreadRun, getTranscript, writeCronState, writeThreadMessage } from "@/msgstore";
import { applyFrame } from "@/protocol/inbound";
import { sqliteStorage } from "@/store/sqlite";
import { createTask, setPlacement } from "@/tasks";

// D-SEC SECURITY regression — IDOR on the M2/M5 inbound WRITE path (the sibling
// of B4s egress + B6s byte-channel). `applyFrame` wrote thread_message /
// thread_result / cron_state keyed by the FRAME's caller-supplied taskId /
// threadId / cronId with NO check against the connect-token-resolved agentId. A
// compromised/malicious agent could therefore inject into — or overwrite — ANOTHER
// agent's task transcript, terminal result, or cron mirror by forging the
// envelope ids. The fix authorizes every store-write against `tasks.agent_id`
// (set-once placement) AND binds the supplied thread/cron id to that same task
// (a thread/cron belongs to exactly one task): a denied frame is DROPPED — never
// written. A denial is PERMANENT, so under the poison policy (HOST-M1M2-04) the
// drop is ACKED — the sender stops replaying it, but nothing ever reaches the
// store.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** Enroll a fresh agent and return its id (placement subject). */
function agent(): string {
  return enrollAgent(db, { capabilities: { engines: ["claude"] } }).agent.id;
}

/** Create a task and pin it to `agentId` (set-once placement). */
function placedTask(ownerId: string, agentId: string): string {
  const id = createTask(db, { ownerId, engine: "claude", model: "sonnet", status: "active" }).id;
  setPlacement(db, id, agentId);
  return id;
}

const tmsg = (seq: number, content: string) => ({ seq, kind: "assistant_text" as const, content });
const tresult = { status: "succeeded" as const, duration_ms: 1, final_seq: 1 };
const cstate = (round: number, status: string) => ({ kind: "interval", round_count: round, skipped_count: 0, status });

/** Apply one inbound frame as `agentId` (no sender/logger — returns the ack or null). */
function apply(agentId: string, frame: DecodedFrame): Record<string, unknown> | null {
  return applyFrame({ db, agentId }, frame);
}

describe("D-SEC — inbound write IDOR: a frame for ANOTHER agent's task is dropped", () => {
  it("DROPS a thread_message for a task placed on another agent (no write; poison-acked); accepts the owner's", () => {
    const attacker = agent();
    const victim = agent();
    const victimTask = placedTask("owner-V", victim);

    // The IDOR: attacker forges the victim's taskId → must be dropped. The
    // denial is permanent, so the drop is acked (poison policy, HOST-M1M2-04).
    const denied = apply(attacker, { type: "thread_message", outboxId: 9, taskId: victimTask, threadId: "thr-V", data: tmsg(1, "injected") });
    expect(denied).toEqual({ type: "ack", id: 9 }); // acked-and-dropped
    expect(getTranscript(storage, "thr-V")).toHaveLength(0); // not written

    // The placed owner writes its OWN task → accepted + acked.
    const ok = apply(victim, { type: "thread_message", outboxId: 10, taskId: victimTask, threadId: "thr-V", data: tmsg(1, "real") });
    expect(ok).toEqual({ type: "ack", id: 10 });
    expect(getTranscript(storage, "thr-V").map(m => m.content)).toEqual(["real"]);
  });

  it("DROPS a thread_result for another agent's task; accepts the owner's", () => {
    const attacker = agent();
    const victim = agent();
    const victimTask = placedTask("owner-V", victim);

    const denied = apply(attacker, { type: "thread_result", outboxId: 1, taskId: victimTask, threadId: "thr-R", data: tresult });
    expect(denied).toEqual({ type: "ack", id: 1 }); // acked-and-dropped
    expect(getThreadRun(storage, "thr-R")).toBeUndefined();

    const ok = apply(victim, { type: "thread_result", outboxId: 2, taskId: victimTask, threadId: "thr-R", data: tresult });
    expect(ok).toEqual({ type: "ack", id: 2 });
    expect(getThreadRun(storage, "thr-R")?.status).toBe("succeeded");
  });

  it("DROPS a cron_state for another agent's task; accepts the owner's", () => {
    const attacker = agent();
    const victim = agent();
    const victimTask = placedTask("owner-V", victim);

    const denied = apply(attacker, { type: "cron_state", outboxId: 1, taskId: victimTask, cronId: "cron-V", data: cstate(7, "scheduled") });
    expect(denied).toEqual({ type: "ack", id: 1 }); // acked-and-dropped
    expect(getCron(storage, "cron-V")).toBeUndefined();

    const ok = apply(victim, { type: "cron_state", outboxId: 2, taskId: victimTask, cronId: "cron-V", data: cstate(7, "scheduled") });
    expect(ok).toEqual({ type: "ack", id: 2 });
    expect(getCron(storage, "cron-V")?.roundCount).toBe(7);
  });

  it("DROPS a write for an unplaced (pending-assignment) task — no agent owns it yet", () => {
    const attacker = agent();
    const pending = createTask(db, { ownerId: "owner-V", engine: "claude", model: "sonnet" }).id; // not placed

    const denied = apply(attacker, { type: "thread_message", outboxId: 1, taskId: pending, threadId: "thr-P", data: tmsg(1, "x") });
    expect(denied).toEqual({ type: "ack", id: 1 }); // acked-and-dropped
    expect(getTranscript(storage, "thr-P")).toHaveLength(0);
  });

  it("DROPS a write for an unknown taskId (fails closed)", () => {
    const attacker = agent();
    const denied = apply(attacker, { type: "thread_message", outboxId: 1, taskId: "task-does-not-exist", threadId: "thr-X", data: tmsg(1, "x") });
    expect(denied).toEqual({ type: "ack", id: 1 }); // acked-and-dropped
    expect(getTranscript(storage, "thr-X")).toHaveLength(0);
  });
});

describe("D-SEC — inbound write IDOR: a placed agent cannot forge a SIBLING task's thread/cron id", () => {
  it("DROPS a thread_message whose threadId is already owned by another task (no (thread,seq) poisoning)", () => {
    const attacker = agent();
    const victim = agent();
    const attackerTask = placedTask("owner-A", attacker);
    const victimTask = placedTask("owner-V", victim);

    // The victim's thread legitimately exists under the victim's task.
    writeThreadMessage(storage, { taskId: victimTask, threadId: "shared-thr", data: tmsg(1, "victim real") });

    // The attacker is genuinely placed on its OWN task, but forges the victim's
    // threadId tagged with its own taskId — the taskId gate passes, but the thread
    // is bound to the victim's task → denied (no slot squatting, no overwrite race).
    const denied = apply(attacker, { type: "thread_message", outboxId: 3, taskId: attackerTask, threadId: "shared-thr", data: tmsg(2, "attacker inject") });
    expect(denied).toEqual({ type: "ack", id: 3 }); // acked-and-dropped
    expect(getTranscript(storage, "shared-thr").map(m => m.content)).toEqual(["victim real"]); // untouched
  });

  it("DROPS a cron_state whose cronId belongs to another task (no cron-mirror clobber)", () => {
    const attacker = agent();
    const victim = agent();
    const attackerTask = placedTask("owner-A", attacker);
    const victimTask = placedTask("owner-V", victim);

    // The victim's cron mirror exists under the victim's task.
    writeCronState(storage, { taskId: victimTask, cronId: "shared-cron", data: cstate(5, "scheduled") });

    // The attacker (placed on its own task) forges the victim's cronId.
    const denied = apply(attacker, { type: "cron_state", outboxId: 4, taskId: attackerTask, cronId: "shared-cron", data: cstate(999, "hijacked") });
    expect(denied).toEqual({ type: "ack", id: 4 }); // acked-and-dropped

    const cron = getCron(storage, "shared-cron");
    expect(cron?.taskId).toBe(victimTask); // still the victim's
    expect(cron?.roundCount).toBe(5); // not clobbered
    expect(cron?.status).toBe("scheduled");
  });
});
