import type { NewAgentRow, NewTaskRow, Storage } from "@gino/host-core";
import { MissingBlobError, sha256Hex, ShaMismatchError } from "@gino/host-core/domain";

// STORAGE CONFORMANCE — the one spec every `Storage` adapter must pass
// (host-sdk-design §8.3). Each `describe*Conformance(target)` asserts the
// invariants of one repository against a FRESH empty storage per test; the
// sqlite adapter runs the same functions against drizzle. Not a `*.test.ts`
// itself — `memory.test.ts` (and later `sqlite.test.ts`) mount it.

export interface ConformanceTarget {
  name: string;
  open: () => { storage: Storage; tamperBlob: (ref: string) => void; close: () => void };
}

// Method signatures on purpose (bivariant parameters): both `bun:test`'s
// `expect` and the npm `expect` package (Jest's implementation, used by the
// `node --test` runs) must satisfy this surface structurally.
/* eslint-disable ts/method-signature-style */
/** The matchers the suite uses — the common subset of bun:test and Jest `expect`. */
export interface ConformanceMatchers {
  toBe(expected: unknown): void;
  toEqual(expected: unknown): void;
  toMatchObject(expected: object): void;
  toBeUndefined(): void;
  toBeNull(): void;
  toThrow(expected?: unknown): void;
  toHaveLength(length: number): void;
  toBeGreaterThan(expected: number): void;
  readonly not: ConformanceMatchers;
}

export interface ConformanceExpect {
  (actual: unknown): ConformanceMatchers;
  any(constructor: unknown): unknown;
}
/* eslint-enable ts/method-signature-style */

/**
 * The test-runner primitives the suite is written against, so the same file
 * mounts under `bun test` (`import { … } from "bun:test"`) and under
 * `node --test` (`node:test` + the `expect` package).
 */
export interface ConformanceHarness {
  readonly describe: (name: string, fn: () => void) => void;
  readonly test: (name: string, fn: () => void) => void;
  readonly beforeEach: (fn: () => void) => void;
  readonly afterEach: (fn: () => void) => void;
  readonly expect: ConformanceExpect;
}

/** Per-test lifecycle shared by every repo suite: a fresh storage in `beforeEach`, closed in `afterEach`. */
function useStorage(target: ConformanceTarget, { beforeEach, afterEach }: ConformanceHarness): { s: () => Storage; tamper: (ref: string) => void } {
  let opened: ReturnType<ConformanceTarget["open"]> | undefined;
  beforeEach(() => {
    opened = target.open();
  });
  afterEach(() => {
    opened?.close();
    opened = undefined;
  });
  return {
    s: () => opened!.storage,
    tamper: ref => opened!.tamperBlob(ref),
  };
}

function agentRow(id: string, extra: Partial<NewAgentRow> = {}): NewAgentRow {
  return {
    id,
    source: "manual",
    access: "dedicated",
    connectTokenHash: `hash-${id}`,
    capabilities: { engines: ["claude"] },
    status: "enrolled",
    ...extra,
  };
}

function taskRow(id: string, extra: Partial<NewTaskRow> = {}): NewTaskRow {
  return { id, ownerId: "owner-1", engine: "claude", model: "opus", status: "pending-assignment", ...extra };
}

// ─── AgentRepo ───

export function describeAgentRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] AgentRepo`, () => {
    const { s } = useStorage(target, harness);

    test("insert returns the row with defaults applied; get/list round-trip newest first", () => {
      const a = s().agents.insert(agentRow("a1", { createdAt: 100 }));
      const b = s().agents.insert(agentRow("a2", { createdAt: 200 }));
      expect(a).toMatchObject({ id: "a1", name: null, activeThreads: 0, lastSeen: null, launchNonce: null, createdAt: 100 });
      expect(s().agents.get("a1")).toEqual(a);
      expect(s().agents.list().map(x => x.id)).toEqual([b.id, a.id]);
      expect(s().agents.get("nope")).toBeUndefined();
    });

    test("insert rejects a duplicate id with UNIQUE", () => {
      s().agents.insert(agentRow("a1"));
      expect(() => s().agents.insert(agentRow("a1"))).toThrow(/UNIQUE/);
    });

    test("updateStatusIf is compare-and-set: guard mismatch → false and row unchanged", () => {
      s().agents.insert(agentRow("a1"));
      expect(s().agents.updateStatusIf("a1", { from: "offline" }, "online")).toBe(false);
      expect(s().agents.get("a1")!.status).toBe("enrolled");
      expect(s().agents.updateStatusIf("a1", { notFrom: "removed" }, "online")).toBe(true);
      expect(s().agents.get("a1")!.status).toBe("online");
      expect(s().agents.updateStatusIf("a1", { notFrom: "online" }, "offline")).toBe(false);
      expect(s().agents.get("a1")!.status).toBe("online");
      expect(s().agents.updateStatusIf("missing", {}, "online")).toBe(false);
    });

    test("removed is terminal: no guard can transition out of it", () => {
      s().agents.insert(agentRow("a1"));
      s().agents.setRemoved("a1", "revoked");
      expect(s().agents.updateStatusIf("a1", {}, "offline")).toBe(false);
      expect(s().agents.updateStatusIf("a1", { from: "removed" }, "online")).toBe(false);
      expect(s().agents.updateStatusIf("a1", { notFrom: "online" }, "enrolled")).toBe(false);
      expect(s().agents.get("a1")).toMatchObject({ status: "removed", connectTokenHash: "revoked" });
      s().agents.setAccess("a1", "public");
      expect(s().agents.get("a1")!.access).toBe("dedicated");
    });

    test("touchLiveness stamps lastSeen (+activeThreads) without touching status", () => {
      s().agents.insert(agentRow("a1"));
      s().agents.touchLiveness("a1", 5_000);
      expect(s().agents.get("a1")).toMatchObject({ lastSeen: 5_000, activeThreads: 0, status: "enrolled" });
      s().agents.touchLiveness("a1", 6_000, 3);
      expect(s().agents.get("a1")).toMatchObject({ lastSeen: 6_000, activeThreads: 3 });
    });

    test("sweepSilent flips only online agents silent past the cutoff", () => {
      s().agents.insert(agentRow("silent", { status: "online", lastSeen: 1_000 }));
      s().agents.insert(agentRow("fresh", { status: "online", lastSeen: 9_000 }));
      s().agents.insert(agentRow("never", { status: "online" }));
      s().agents.insert(agentRow("enrolled", { status: "enrolled", lastSeen: 1_000 }));
      s().agents.insert(agentRow("offline", { status: "offline", lastSeen: 1_000 }));
      s().agents.insert(agentRow("removed", { status: "removed", lastSeen: 1_000 }));
      expect(s().agents.sweepSilent(5_000)).toEqual(["silent"]);
      expect(s().agents.get("silent")!.status).toBe("offline");
      for (const id of ["fresh", "never"]) {
        expect(s().agents.get(id)!.status).toBe("online");
      }
      expect(s().agents.get("enrolled")!.status).toBe("enrolled");
      expect(s().agents.get("removed")!.status).toBe("removed");
      expect(s().agents.sweepSilent(5_000)).toEqual([]);
    });

    test("getByConnectTokenHash excludes removed agents", () => {
      s().agents.insert(agentRow("a1", { connectTokenHash: "h1" }));
      expect(s().agents.getByConnectTokenHash("h1")!.id).toBe("a1");
      expect(s().agents.getByConnectTokenHash("h2")).toBeUndefined();
      s().agents.setRemoved("a1", "h1"); // even an unchanged hash no longer resolves
      expect(s().agents.getByConnectTokenHash("h1")).toBeUndefined();
    });

    test("setConnectTokenHash rotates and returns the row", () => {
      s().agents.insert(agentRow("a1", { connectTokenHash: "h1" }));
      expect(s().agents.setConnectTokenHash("a1", "h2")!.connectTokenHash).toBe("h2");
      expect(s().agents.getByConnectTokenHash("h1")).toBeUndefined();
      expect(s().agents.getByConnectTokenHash("h2")!.id).toBe("a1");
      expect(s().agents.setConnectTokenHash("missing", "h3")).toBeUndefined();
    });

    test("grants are idempotent and revocable", () => {
      s().agents.insert(agentRow("a1"));
      s().agents.grant("a1", "o1");
      s().agents.grant("a1", "o1");
      s().agents.grant("a1", "o2");
      expect(s().agents.listGrants("a1").map(g => g.ownerId)).toEqual(["o1", "o2"]);
      expect(s().agents.hasGrant("a1", "o1")).toBe(true);
      s().agents.revoke("a1", "o1");
      s().agents.revoke("a1", "o1");
      expect(s().agents.hasGrant("a1", "o1")).toBe(false);
      expect(s().agents.listGrants("a1").map(g => g.ownerId)).toEqual(["o2"]);
    });

    test("recordPending rejects a duplicate nonce with UNIQUE; hasPending/listExpired/deletePending", () => {
      s().agents.recordPending({ nonce: "n1", launcherKind: "docker", launchRef: "c1", createdAt: 1_000 });
      expect(() => s().agents.recordPending({ nonce: "n1", launcherKind: "docker" })).toThrow(/UNIQUE/);
      s().agents.recordPending({ nonce: "n2", launcherKind: "docker", createdAt: 9_000 });
      expect(s().agents.hasPending("n1")).toBe(true);
      expect(s().agents.listExpiredPending(5_000)).toEqual([
        { nonce: "n1", launcherKind: "docker", launchRef: "c1", access: "dedicated", createdAt: 1_000 },
      ]);
      s().agents.deletePending("n1");
      s().agents.deletePending("n1");
      expect(s().agents.hasPending("n1")).toBe(false);
      expect(s().agents.listExpiredPending(10_000).map(p => p.nonce)).toEqual(["n2"]);
    });

    test("correlate consumes the pending launch and binds it onto the agent", () => {
      s().agents.insert(agentRow("a1"));
      s().agents.recordPending({ nonce: "n1", launcherKind: "docker", launchRef: "c1", access: "public" });
      expect(s().agents.correlate("a1", "unknown")).toBeUndefined();
      const consumed = s().agents.correlate("a1", "n1");
      expect(consumed).toMatchObject({ nonce: "n1", launcherKind: "docker", launchRef: "c1", access: "public" });
      expect(s().agents.hasPending("n1")).toBe(false);
      expect(s().agents.get("a1")).toMatchObject({ launcherKind: "docker", launchRef: "c1", launchNonce: "n1", access: "public" });
      expect(s().agents.getByLaunchNonce("n1")!.id).toBe("a1");
    });

    test("launchNonce is unique across agents: a second correlate/insert with the same nonce throws UNIQUE", () => {
      s().agents.insert(agentRow("a1"));
      s().agents.insert(agentRow("a2"));
      s().agents.recordPending({ nonce: "n1", launcherKind: "docker" });
      s().agents.correlate("a1", "n1");
      s().agents.recordPending({ nonce: "n1", launcherKind: "docker" });
      expect(() => s().agents.correlate("a2", "n1")).toThrow(/UNIQUE/);
      expect(s().agents.hasPending("n1")).toBe(true);
      expect(s().agents.get("a2")!.launchNonce).toBeNull();
      expect(() => s().agents.insert(agentRow("a3", { launchNonce: "n1" }))).toThrow(/UNIQUE/);
    });

    test("getByLaunchNonce never resurrects a removed agent", () => {
      s().agents.insert(agentRow("a1", { launchNonce: "n1" }));
      expect(s().agents.getByLaunchNonce("n1")!.id).toBe("a1");
      s().agents.setRemoved("a1", "revoked");
      expect(s().agents.getByLaunchNonce("n1")).toBeUndefined();
    });
  });
}

// ─── TaskRepo ───

export function describeTaskRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] TaskRepo`, () => {
    const { s } = useStorage(target, harness);

    test("insert/get/list with filters, newest first", () => {
      s().tasks.insert(taskRow("t1", { createdAt: 100, agentId: "a1", status: "active" }));
      s().tasks.insert(taskRow("t2", { createdAt: 200, ownerId: "owner-2" }));
      s().tasks.insert(taskRow("t3", { createdAt: 300, agentId: "a1" }));
      expect(s().tasks.get("t1")).toMatchObject({ id: "t1", workdir: null, meta: null, seededAt: null, updatedAt: expect.any(Number) });
      expect(s().tasks.list().map(t => t.id)).toEqual(["t3", "t2", "t1"]);
      expect(s().tasks.list({ agentId: "a1" }).map(t => t.id)).toEqual(["t3", "t1"]);
      expect(s().tasks.list({ ownerId: "owner-1", status: "active" }).map(t => t.id)).toEqual(["t1"]);
      expect(() => s().tasks.insert(taskRow("t1"))).toThrow(/UNIQUE/);
    });

    test("setPlacementOnce is set-once: second call false and agentId unchanged", () => {
      s().tasks.insert(taskRow("t1", { seededAt: 1, currentThreadId: "th-old" }));
      expect(s().tasks.setPlacementOnce("t1", "a1", 5_000)).toBe(true);
      expect(s().tasks.get("t1")).toMatchObject({ agentId: "a1", assignedAt: 5_000, seededAt: null, currentThreadId: null, updatedAt: 5_000 });
      expect(s().tasks.setPlacementOnce("t1", "a2", 6_000)).toBe(false);
      expect(s().tasks.setPlacementOnce("t1", "a1", 6_000)).toBe(false);
      expect(s().tasks.get("t1")).toMatchObject({ agentId: "a1", assignedAt: 5_000, updatedAt: 5_000 });
      expect(s().tasks.setPlacementOnce("missing", "a1", 6_000)).toBe(false);
    });

    test("orphanByAgent clears placement, allowing a fresh set-once placement", () => {
      s().tasks.insert(taskRow("t1", { agentId: "a1" }));
      s().tasks.insert(taskRow("t2", { agentId: "a2" }));
      expect(s().tasks.orphanByAgent("a1", 7_000)).toEqual(["t1"]);
      expect(s().tasks.get("t1")).toMatchObject({ agentId: null, updatedAt: 7_000 });
      expect(s().tasks.get("t2")!.agentId).toBe("a2");
      expect(s().tasks.setPlacementOnce("t1", "a3", 8_000)).toBe(true);
    });

    test("setStatus / setCurrentThread / markSeeded", () => {
      s().tasks.insert(taskRow("t1"));
      expect(s().tasks.setStatus("t1", "active", 1_000)).toMatchObject({ status: "active", updatedAt: 1_000 });
      expect(s().tasks.setCurrentThread("t1", "th1", 2_000)).toMatchObject({ currentThreadId: "th1", updatedAt: 2_000 });
      expect(s().tasks.setStatus("missing", "active", 1_000)).toBeUndefined();
      s().tasks.markSeeded("t1", 3_000);
      s().tasks.markSeeded("t1", 4_000);
      expect(s().tasks.get("t1")!.seededAt).toBe(3_000);
    });
  });
}

// ─── OutboxRepo ───

export function describeOutboxRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] OutboxRepo`, () => {
    const { s } = useStorage(target, harness);
    const frame = (agentId: string, type = "task_upsert") => ({ agentId, type, dataJson: {} });

    test("ids are strictly monotonic across interleaved agents and returned on append", () => {
      const ids = [
        s().outbox.append(frame("a1")),
        s().outbox.append(frame("a2")),
        s().outbox.append(frame("a1")),
        s().outbox.append(frame("a2")),
      ].map(r => r.id);
      for (let i = 1; i < ids.length; i++) {
        expect(ids[i]!).toBeGreaterThan(ids[i - 1]!);
      }
      expect(new Set(ids).size).toBe(4);
      const first = s().outbox.append(frame("a1"));
      expect(first).toMatchObject({ agentId: "a1", ackedAt: null, taskId: null, createdAt: expect.any(Number) });
    });

    test("ids are never reused after a delete", () => {
      const a = s().outbox.append(frame("a1"));
      expect(s().outbox.purge("a1")).toBe(1);
      expect(s().outbox.append(frame("a1")).id).toBeGreaterThan(a.id);
    });

    test("listUnacked replays one agent's rows ascending by id", () => {
      const r1 = s().outbox.append(frame("a1"));
      s().outbox.append(frame("a2"));
      const r3 = s().outbox.append(frame("a1"));
      expect(s().outbox.listUnacked("a1").map(r => r.id)).toEqual([r1.id, r3.id]);
      expect(s().outbox.listAllUnacked().map(r => r.agentId)).toEqual(["a1", "a2", "a1"]);
    });

    test("markAcked is scoped to the agent's rows", () => {
      const r = s().outbox.append(frame("a1"));
      expect(s().outbox.markAcked("a2", r.id, 1_000)).toBe(false);
      expect(s().outbox.listUnacked("a1")).toHaveLength(1);
      expect(s().outbox.markAcked("a1", r.id, 1_000)).toBe(true);
      expect(s().outbox.listUnacked("a1")).toHaveLength(0);
      expect(s().outbox.markAcked("a1", 999_999, 1_000)).toBe(false);
    });

    test("ack (excludeType: message) does not ack a message row; msg_ack acks any type", () => {
      const msg = s().outbox.append(frame("a1", "message"));
      const upsert = s().outbox.append(frame("a1", "task_upsert"));
      expect(s().outbox.markAcked("a1", msg.id, 1_000, { excludeType: "message" })).toBe(false);
      expect(s().outbox.listUnacked("a1").map(r => r.id)).toEqual([msg.id, upsert.id]);
      expect(s().outbox.markAcked("a1", upsert.id, 1_000, { excludeType: "message" })).toBe(true);
      expect(s().outbox.markAcked("a1", msg.id, 2_000)).toBe(true);
      expect(s().outbox.listUnacked("a1")).toEqual([]);
    });

    test("purge deletes only the agent's unacked rows", () => {
      const acked = s().outbox.append(frame("a1"));
      s().outbox.markAcked("a1", acked.id, 1_000);
      s().outbox.append(frame("a1"));
      s().outbox.append(frame("a1"));
      const other = s().outbox.append(frame("a2"));
      expect(s().outbox.purge("a1")).toBe(2);
      expect(s().outbox.listUnacked("a1")).toEqual([]);
      expect(s().outbox.listUnacked("a2").map(r => r.id)).toEqual([other.id]);
      // The acked row survives (retention GC's business, not purge's).
      expect(s().outbox.gc(2_000)).toBe(1);
    });

    test("gc deletes only acked rows older than `before`", () => {
      const old = s().outbox.append(frame("a1"));
      const recent = s().outbox.append(frame("a1"));
      s().outbox.append(frame("a1")); // unacked, however old
      s().outbox.markAcked("a1", old.id, 1_000);
      s().outbox.markAcked("a1", recent.id, 9_000);
      expect(s().outbox.gc(5_000)).toBe(1);
      expect(s().outbox.listUnacked("a1")).toHaveLength(1);
      expect(s().outbox.gc(5_000)).toBe(0);
      expect(s().outbox.gc(10_000)).toBe(1);
      expect(s().outbox.listUnacked("a1")).toHaveLength(1);
    });
  });
}

// ─── TranscriptRepo ───

export function describeTranscriptRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] TranscriptRepo`, () => {
    const { s } = useStorage(target, harness);
    const msg = (threadId: string, seq: number, extra: Record<string, unknown> = {}) =>
      ({ taskId: "t1", threadId, seq, kind: "text", content: `m${seq}`, ...extra });

    test("(threadId, seq) is unique — first writer wins and the original row is retained", () => {
      expect(s().transcript.insertIfAbsent(msg("th1", 1, { content: "first" }))).toBe(true);
      expect(s().transcript.insertIfAbsent(msg("th1", 1, { content: "second" }))).toBe(false);
      expect(s().transcript.insertIfAbsent(msg("th2", 1))).toBe(true);
      const rows = s().transcript.read("th1", { limit: 10 });
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ content: "first", role: null, input: null, output: null });
    });

    test("sub-agent attribution round-trips; absence stores NULL for both ids", () => {
      s().transcript.insertIfAbsent(msg("th1", 1, { kind: "tool_use", toolUseId: "tu-1" }));
      s().transcript.insertIfAbsent(msg("th1", 2, { kind: "assistant_text", toolUseId: "tu-2", parentToolUseId: "tu-1" }));
      s().transcript.insertIfAbsent(msg("th1", 3));
      const rows = s().transcript.read("th1", { limit: 10 });
      expect(rows.map(r => [r.toolUseId, r.parentToolUseId])).toEqual([
        ["tu-1", null],
        ["tu-2", "tu-1"],
        [null, null],
      ]);
      // Same shape on the id-ordered tail read the egress cursor uses.
      expect(s().transcript.readAfterId("t1", 0, 10).map(r => [r.toolUseId, r.parentToolUseId])).toEqual([
        ["tu-1", null],
        ["tu-2", "tu-1"],
        [null, null],
      ]);
    });

    test("read is ordered by seq (never arrival) and honours afterSeq/limit", () => {
      s().transcript.insertIfAbsent(msg("th1", 3));
      s().transcript.insertIfAbsent(msg("th1", 1));
      s().transcript.insertIfAbsent(msg("th1", 2));
      s().transcript.insertIfAbsent(msg("th1", 4));
      expect(s().transcript.read("th1", { limit: 10 }).map(r => r.seq)).toEqual([1, 2, 3, 4]);
      expect(s().transcript.read("th1", { limit: 2 }).map(r => r.seq)).toEqual([1, 2]);
      expect(s().transcript.read("th1", { afterSeq: 2, limit: 10 }).map(r => r.seq)).toEqual([3, 4]);
      expect(s().transcript.read("th1", { afterSeq: 2, limit: 1 }).map(r => r.seq)).toEqual([3]);
      expect(s().transcript.read("other", { limit: 10 })).toEqual([]);
    });

    test("stats counts rows and reports the max seq", () => {
      expect(s().transcript.stats("th1")).toEqual({ messageCount: 0, lastSeq: null });
      s().transcript.insertIfAbsent(msg("th1", 2));
      s().transcript.insertIfAbsent(msg("th1", 5));
      expect(s().transcript.stats("th1")).toEqual({ messageCount: 2, lastSeq: 5 });
    });

    test("threads orders a task's threads by first appearance; taskOf resolves the owner", () => {
      s().transcript.insertIfAbsent(msg("late", 1, { createdAt: 300 }));
      s().transcript.insertIfAbsent(msg("early", 1, { createdAt: 100 }));
      s().transcript.insertIfAbsent(msg("early", 2, { createdAt: 400 }));
      s().transcript.insertIfAbsent(msg("foreign", 1, { taskId: "t2" }));
      expect(s().transcript.threads("t1")).toEqual([{ threadId: "early", firstAt: 100 }, { threadId: "late", firstAt: 300 }]);
      expect(s().transcript.taskOf("foreign")).toBe("t2");
      expect(s().transcript.taskOf("unseen")).toBeUndefined();
    });

    test("readAfterId tails a task by store position (id), ascending, capped", () => {
      s().transcript.insertIfAbsent(msg("th1", 1));
      s().transcript.insertIfAbsent(msg("th2", 1, { taskId: "t2" }));
      s().transcript.insertIfAbsent(msg("th1", 2));
      s().transcript.insertIfAbsent(msg("th3", 1));
      const all = s().transcript.readAfterId("t1", 0, 10);
      expect(all.map(r => [r.threadId, r.seq])).toEqual([["th1", 1], ["th1", 2], ["th3", 1]]);
      for (let i = 1; i < all.length; i++) {
        expect(all[i]!.id).toBeGreaterThan(all[i - 1]!.id);
      }
      expect(s().transcript.readAfterId("t1", all[0]!.id, 1).map(r => r.id)).toEqual([all[1]!.id]);
      expect(s().transcript.readAfterId("t1", all[2]!.id, 10)).toEqual([]);
    });
  });
}

// ─── ThreadRunRepo ───

export function describeThreadRunRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] ThreadRunRepo`, () => {
    const { s } = useStorage(target, harness);

    test("upsert inserts everything first, then updates only the terminal mirror fields", () => {
      s().threadRuns.upsert({ threadId: "th1", taskId: "t1", cronId: "c1", source: "cron", status: "ok", durationMs: 10, finalSeq: 3, finishedAt: 1_000, createdAt: 900 });
      expect(s().threadRuns.get("th1")).toEqual({
        threadId: "th1",
        taskId: "t1",
        cronId: "c1",
        source: "cron",
        status: "ok",
        failureReason: null,
        engineSessionId: null,
        durationMs: 10,
        finalSeq: 3,
        startedAt: null,
        finishedAt: 1_000,
        createdAt: 900,
      });
      s().threadRuns.upsert({ threadId: "th1", taskId: "t2", cronId: null, source: "chat", status: "failed", failureReason: "boom", engineSessionId: "s1", durationMs: 20, finalSeq: 4, finishedAt: 2_000, createdAt: 1_900 });
      expect(s().threadRuns.get("th1")).toEqual({
        threadId: "th1",
        taskId: "t1",
        cronId: "c1",
        source: "cron",
        status: "failed",
        failureReason: "boom",
        engineSessionId: "s1",
        durationMs: 20,
        finalSeq: 4,
        startedAt: null,
        finishedAt: 1_000,
        createdAt: 900,
      });
      expect(s().threadRuns.get("missing")).toBeUndefined();
    });

    test("listByTask returns a task's runs", () => {
      s().threadRuns.upsert({ threadId: "th1", taskId: "t1", source: "chat", status: "ok" });
      s().threadRuns.upsert({ threadId: "th2", taskId: "t2", source: "chat", status: "ok" });
      s().threadRuns.upsert({ threadId: "th3", taskId: "t1", source: "chat", status: "ok" });
      expect(s().threadRuns.listByTask("t1").map(r => r.threadId)).toEqual(["th1", "th3"]);
    });
  });
}

// ─── CronMirrorRepo ───

export function describeCronRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] CronMirrorRepo`, () => {
    const { s } = useStorage(target, harness);
    const state = (overrides: Record<string, unknown> = {}) => ({
      id: "c1",
      taskId: "t1",
      kind: "interval",
      scheduleType: "every",
      scheduleValue: "5m",
      nextRun: 1_000,
      status: "active",
      lastStatus: null,
      roundCount: 1,
      skippedCount: 0,
      updatedAt: 500,
      ...overrides,
    });

    test("upsertState preserves prompt and taskId; continuity only when carried", () => {
      s().crons.upsertPrompt({ id: "c1", taskId: "t1", prompt: "do work", updatedAt: 100 });
      s().crons.upsertState(state({ taskId: "t-other", continuity: "shared" }));
      expect(s().crons.get("c1")).toMatchObject({ taskId: "t1", prompt: "do work", continuity: "shared", kind: "interval", roundCount: 1, updatedAt: 500 });
      s().crons.upsertState(state({ roundCount: 2, updatedAt: 600 }));
      expect(s().crons.get("c1")).toMatchObject({ prompt: "do work", continuity: "shared", roundCount: 2, updatedAt: 600 });
      s().crons.upsertState(state({ continuity: "isolated", updatedAt: 700 }));
      expect(s().crons.get("c1")!.continuity).toBe("isolated");
    });

    test("upsertState inserts with an empty prompt/continuity on first sight", () => {
      s().crons.upsertState(state());
      expect(s().crons.get("c1")).toMatchObject({ prompt: "", continuity: "", taskId: "t1" });
    });

    test("upsertPrompt inserts a stub first, then updates only prompt + updatedAt", () => {
      s().crons.upsertPrompt({ id: "c1", taskId: "t1", prompt: "v1", updatedAt: 100 });
      expect(s().crons.get("c1")).toMatchObject({ kind: "", continuity: "", prompt: "v1", nextRun: null, status: null, updatedAt: 100 });
      s().crons.upsertState(state({ updatedAt: 200 }));
      s().crons.upsertPrompt({ id: "c1", taskId: "t-other", prompt: "v2", updatedAt: 300 });
      expect(s().crons.get("c1")).toMatchObject({ taskId: "t1", prompt: "v2", kind: "interval", scheduleValue: "5m", nextRun: 1_000, status: "active", roundCount: 1, updatedAt: 300 });
    });

    test("listByTask is most-recently-updated first", () => {
      s().crons.upsertState(state({ id: "c1", updatedAt: 100 }));
      s().crons.upsertState(state({ id: "c2", updatedAt: 300 }));
      s().crons.upsertState(state({ id: "c3", updatedAt: 200, taskId: "t2" }));
      expect(s().crons.listByTask("t1").map(c => c.id)).toEqual(["c2", "c1"]);
      expect(s().crons.get("missing")).toBeUndefined();
    });
  });
}

// ─── AttachmentRepo ───

export function describeAttachmentRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] AttachmentRepo`, () => {
    const { s } = useStorage(target, harness);
    const desc = (id: string, sha256 = "s".repeat(64), extra: Record<string, unknown> = {}) =>
      ({ id, taskId: "t1", threadId: "th1", name: "f.txt", contentType: "text/plain", size: 3, sha256, ref: `f.txt/${sha256.slice(0, 12)}`, direction: "outbound", ...extra });

    test("insertIfAbsent is idempotent on id (first writer wins)", () => {
      expect(s().attachments.insertIfAbsent(desc("at1", "a".repeat(64), { name: "first" }))).toBe(true);
      expect(s().attachments.insertIfAbsent(desc("at1", "b".repeat(64), { name: "second" }))).toBe(false);
      expect(s().attachments.getById("at1")).toMatchObject({ name: "first", sha256: "a".repeat(64), createdAt: expect.any(Number) });
      expect(s().attachments.getById("missing")).toBeUndefined();
    });

    test("listBySha / listAll", () => {
      s().attachments.insertIfAbsent(desc("at1", "a".repeat(64)));
      s().attachments.insertIfAbsent(desc("at2", "b".repeat(64)));
      s().attachments.insertIfAbsent(desc("at3", "a".repeat(64), { taskId: "t2" }));
      expect(s().attachments.listBySha("a".repeat(64)).map(a => a.id)).toEqual(["at1", "at3"]);
      expect(s().attachments.listAll().map(a => a.id)).toEqual(["at1", "at2", "at3"]);
    });
  });
}

// ─── ConfigRepo ───

export function describeConfigRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] ConfigRepo`, () => {
    const { s } = useStorage(target, harness);
    const SHA_A = "a".repeat(64);
    const SHA_B = "b".repeat(64);
    const entry = (name: string, sha256: string, target = name) => ({ name, sha256, target, kind: "file" });
    const artifact = (name: string, sha256: string, extra: Record<string, unknown> = {}) =>
      ({ name, sha256, kind: "file", size: 1, storeRef: `${name}/${sha256.slice(0, 12)}`, ...extra });

    test("artifacts: (name, sha256) unique — a repeat upsert updates only kind/contentType", () => {
      const first = s().config.upsertArtifact(artifact("n", SHA_A, { createdAt: 100 }));
      expect(first).toMatchObject({ kind: "file", contentType: null, unreferencedSince: null, createdAt: 100 });
      const again = s().config.upsertArtifact(artifact("n", SHA_A, { kind: "dir", contentType: "x/y", size: 99, storeRef: "other", createdAt: 200 }));
      expect(again).toMatchObject({ kind: "dir", contentType: "x/y", size: 1, storeRef: `n/${SHA_A.slice(0, 12)}`, createdAt: 100 });
      expect(s().config.listArtifacts()).toHaveLength(1);
      s().config.upsertArtifact(artifact("n", SHA_B, { createdAt: 300 }));
      expect(s().config.listArtifacts().map(a => a.sha256)).toEqual([SHA_B, SHA_A]);
      expect(s().config.getArtifact("n", SHA_B)!.sha256).toBe(SHA_B);
      expect(s().config.getArtifact("n", "c".repeat(64))).toBeUndefined();
    });

    test("getArtifactBySha resolves the newest row for a sha", () => {
      s().config.upsertArtifact(artifact("old", SHA_A, { createdAt: 100 }));
      s().config.upsertArtifact(artifact("new", SHA_A, { createdAt: 200 }));
      expect(s().config.getArtifactBySha(SHA_A)!.name).toBe("new");
      expect(s().config.getArtifactBySha(SHA_B)).toBeUndefined();
    });

    test("setUnreferencedSince / deleteArtifact", () => {
      s().config.upsertArtifact(artifact("n", SHA_A));
      s().config.setUnreferencedSince("n", SHA_A, 5_000);
      expect(s().config.getArtifact("n", SHA_A)!.unreferencedSince).toBe(5_000);
      s().config.setUnreferencedSince("n", SHA_A, null);
      expect(s().config.getArtifact("n", SHA_A)!.unreferencedSince).toBeNull();
      s().config.deleteArtifact("n", SHA_A);
      s().config.deleteArtifact("n", SHA_A);
      expect(s().config.getArtifact("n", SHA_A)).toBeUndefined();
    });

    test("replaceScope replaces the set; the global version bumps on every global write and is strictly increasing", () => {
      expect(s().config.globalVersion()).toBe(0);
      expect(s().config.replaceScope("global", [entry("a", SHA_A)])).toBe(1);
      expect(s().config.replaceScope("global", [entry("a", SHA_A), entry("b", SHA_B)])).toBe(2);
      expect(s().config.entriesFor("global").map(e => e.target)).toEqual(["a", "b"]);
      expect(s().config.replaceScope("global", [entry("b", SHA_B)])).toBe(3);
      expect(s().config.entriesFor("global").map(e => e.target)).toEqual(["b"]);
      expect(s().config.globalVersion()).toBe(3);
      // A task scope carries no version.
      expect(s().config.replaceScope("t1", [entry("a", SHA_A)])).toBe(3);
      expect(s().config.entriesFor("t1")).toHaveLength(1);
      expect(s().config.globalVersion()).toBe(3);
      expect(s().config.replaceScope("global", [])).toBe(4);
      expect(s().config.entriesFor("global")).toEqual([]);
      expect(s().config.entriesFor("t1")).toHaveLength(1);
    });

    test("replaceScope is atomic: a batch with a duplicate target leaves the previous set and version untouched", () => {
      s().config.replaceScope("global", [entry("a", SHA_A)]);
      expect(() => s().config.replaceScope("global", [entry("x", SHA_A, "same"), entry("y", SHA_B, "same")])).toThrow(/UNIQUE/);
      expect(s().config.entriesFor("global").map(e => [e.name, e.sha256])).toEqual([["a", SHA_A]]);
      expect(s().config.globalVersion()).toBe(1);
    });

    test("entries: listAllEntries / clearScope / scopesWithSha", () => {
      s().config.replaceScope("global", [entry("a", SHA_A)]);
      s().config.replaceScope("t1", [entry("a", SHA_A), entry("b", SHA_B)]);
      s().config.replaceScope("t2", [entry("b", SHA_B)]);
      expect(s().config.listAllEntries().map(e => e.scope)).toEqual(["global", "t1", "t1", "t2"]);
      expect(s().config.scopesWithSha(SHA_A)).toEqual(["global", "t1"]);
      expect(s().config.scopesWithSha(SHA_B)).toEqual(["t1", "t2"]);
      s().config.clearScope("t1");
      s().config.clearScope("t1");
      expect(s().config.entriesFor("t1")).toEqual([]);
      expect(s().config.scopesWithSha(SHA_B)).toEqual(["t2"]);
      expect(s().config.globalVersion()).toBe(1);
    });
  });
}

// ─── BlobStorePort ───

export function describeBlobStoreConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] BlobStorePort`, () => {
    const { s, tamper } = useStorage(target, harness);
    const bytes = new TextEncoder().encode("hello blob");

    test("put/readVerified round-trip; ref is content-addressed", () => {
      const stored = s().blobs.put("skill", bytes);
      const sha = sha256Hex(bytes);
      expect(stored).toEqual({ name: "skill", sha256: sha, size: bytes.byteLength, ref: s().blobs.refFor("skill", sha) });
      expect(stored.ref).toBe(`skill/${sha.slice(0, 12)}`);
      expect(s().blobs.exists(stored.ref)).toBe(true);
      expect(s().blobs.sizeOf(stored.ref)).toBe(bytes.byteLength);
      expect(new TextDecoder().decode(s().blobs.readVerified(stored.ref, sha))).toBe("hello blob");
      expect(new TextDecoder().decode(s().blobs.read(stored.ref))).toBe("hello blob");
      // Idempotent re-put.
      expect(s().blobs.put("skill", bytes)).toEqual(stored);
      expect(s().blobs.listLeaves().map(l => l.ref)).toEqual([stored.ref]);
    });

    test("readVerified throws ShaMismatchError after the bytes are tampered", () => {
      const stored = s().blobs.put("skill", bytes);
      tamper(stored.ref);
      expect(() => s().blobs.readVerified(stored.ref, stored.sha256)).toThrow(ShaMismatchError);
      // A re-put of the correct bytes self-heals (HOST-M8-04).
      s().blobs.put("skill", bytes);
      expect(s().blobs.readVerified(stored.ref, stored.sha256)).toEqual(bytes);
    });

    test("a missing ref throws MissingBlobError; delete is idempotent", () => {
      expect(() => s().blobs.readVerified("skill/000000000000", "0".repeat(64))).toThrow(MissingBlobError);
      expect(() => s().blobs.read("skill/000000000000")).toThrow(MissingBlobError);
      expect(s().blobs.exists("skill/000000000000")).toBe(false);
      expect(s().blobs.sizeOf("skill/000000000000")).toBe(0);
      const stored = s().blobs.put("skill", bytes);
      s().blobs.delete(stored.ref);
      s().blobs.delete(stored.ref);
      expect(s().blobs.exists(stored.ref)).toBe(false);
      expect(s().blobs.listLeaves()).toEqual([]);
    });

    test("refFor rejects an unsafe name", () => {
      expect(() => s().blobs.refFor("../etc", "a".repeat(64))).toThrow();
      expect(() => s().blobs.put("a/b", bytes)).toThrow();
    });
  });
}

// ─── ChannelRepo ───

export function describeChannelRepoConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] ChannelRepo`, () => {
    const { s } = useStorage(target, harness);
    const channels = () => {
      const repo = s().channels;
      if (!repo) {
        throw new Error("target has no ChannelRepo");
      }
      return repo;
    };
    const channel = (id: string, extra: Record<string, unknown> = {}) => ({ id, name: `name-${id}`, type: "webhook", ownerId: "o1", ...extra });

    test("insert/get/getByName/list/setConfig", () => {
      channels().insert(channel("c1", { createdAt: 100 }));
      channels().insert(channel("c2", { createdAt: 200, name: "dup" }));
      channels().insert(channel("c3", { createdAt: 300, name: "dup" }));
      expect(channels().get("c1")).toMatchObject({ id: "c1", config: null, msgFilter: null });
      expect(channels().getByName("dup")!.id).toBe("c2");
      expect(channels().list().map(c => c.id)).toEqual(["c3", "c2", "c1"]);
      expect(channels().setConfig("c1", { webhooks: [] })).toMatchObject({ config: { webhooks: [] } });
      expect(channels().setConfig("missing", null)).toBeUndefined();
      expect(() => channels().insert(channel("c1"))).toThrow(/UNIQUE/);
    });

    test("bindIfAbsent is unique on (channelId, conversationRef)", () => {
      expect(channels().bindIfAbsent({ channelId: "c1", conversationRef: "conv", taskId: "t1" })).toBe(true);
      expect(channels().bindIfAbsent({ channelId: "c1", conversationRef: "conv", taskId: "t2" })).toBe(false);
      expect(channels().bindIfAbsent({ channelId: "c2", conversationRef: "conv", taskId: "t2" })).toBe(true);
      expect(channels().getBinding("c1", "conv")!.taskId).toBe("t1");
      expect(channels().getBinding("c1", "other")).toBeUndefined();
      expect(channels().isTaskBound("c1", "t1")).toBe(true);
      expect(channels().isTaskBound("c1", "t2")).toBe(false);
      expect(channels().listBindings("c1").map(b => b.conversationRef)).toEqual(["conv"]);
    });

    test("cursor advance is monotonic — never moves backwards", () => {
      expect(channels().readCursor("c1", "t1")).toBe(0);
      expect(channels().advanceCursor("c1", "t1", 0, 1_000)).toBe(false);
      expect(channels().advanceCursor("c1", "t1", 5, 1_000)).toBe(true);
      expect(channels().readCursor("c1", "t1")).toBe(5);
      expect(channels().advanceCursor("c1", "t1", 3, 2_000)).toBe(false);
      expect(channels().advanceCursor("c1", "t1", 5, 2_000)).toBe(false);
      expect(channels().readCursor("c1", "t1")).toBe(5);
      expect(channels().advanceCursor("c1", "t1", 9, 3_000)).toBe(true);
      expect(channels().readCursor("c1", "t1")).toBe(9);
      expect(channels().readCursor("c1", "t2")).toBe(0);
    });
  });
}

// ─── Storage ───

export function describeStorageConformance(target: ConformanceTarget, harness: ConformanceHarness): void {
  const { describe, test, expect } = harness;
  describe(`[${target.name}] Storage`, () => {
    const { s } = useStorage(target, harness);

    test("ping succeeds on an open storage", () => {
      expect(() => s().ping()).not.toThrow();
    });
  });
  describeAgentRepoConformance(target, harness);
  describeTaskRepoConformance(target, harness);
  describeOutboxRepoConformance(target, harness);
  describeTranscriptRepoConformance(target, harness);
  describeThreadRunRepoConformance(target, harness);
  describeCronRepoConformance(target, harness);
  describeAttachmentRepoConformance(target, harness);
  describeConfigRepoConformance(target, harness);
  describeBlobStoreConformance(target, harness);
  describeChannelRepoConformance(target, harness);
}
