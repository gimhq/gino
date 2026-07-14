import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "bun:test";
import {
  AgentFrame,
  AnyFrame,
  Envelope,
  HostFrame,
} from "@/protocol/frames";
import { buildSchema, MARKER, renderSection } from "../../scripts/gen-frames-schema";

// One representative VALID frame per `type`, taken from the protocol §5
// catalog (incl. the §4.1 envelope example). The direction tells the test which
// discriminated union must also accept it.
const VALID: { name: string; dir: "agent" | "host"; frame: unknown }[] = [
  // ── agent → host ──────────────────────────────────────────────────────────
  {
    name: "thread_result",
    dir: "agent",
    frame: {
      type: "thread_result",
      outboxId: 12,
      taskId: "t-1",
      threadId: "c-9@1718",
      data: {
        status: "succeeded",
        result: "done",
        engine_session_id: "sess-abc",
        duration_ms: 4200,
        final_seq: 7,
        attachments: [
          { id: "a1", name: "out.txt", content_type: "text/plain", size: 12, sha256: "deadbeef", ref: "blob://a1", direction: "outbound" },
        ],
      },
    },
  },
  {
    // The literal §4.1 envelope example.
    name: "thread_message (§4.1 example)",
    dir: "agent",
    frame: {
      type: "thread_message",
      outboxId: 43,
      taskId: "t-1",
      threadId: "c-9@1718",
      data: { seq: 3, kind: "assistant_text", content: "…" },
    },
  },
  {
    name: "thread_message (tool_use)",
    dir: "agent",
    frame: {
      type: "thread_message",
      taskId: "t-1",
      threadId: "c-9@1718",
      data: { seq: 4, kind: "tool_use", tool: "bash", input: { cmd: "ls" } },
    },
  },
  {
    name: "cron_state",
    dir: "agent",
    frame: {
      type: "cron_state",
      taskId: "t-1",
      cronId: "cr-1",
      data: { kind: "cron", schedule_type: "interval", schedule_value: "5m", continuity: "isolated", status: "active", round_count: 3, skipped_count: 1, last_status: "succeeded" },
    },
  },
  { name: "heartbeat", dir: "agent", frame: { type: "heartbeat", data: { activeThreads: 2 } } },
  { name: "heartbeat (no data)", dir: "agent", frame: { type: "heartbeat" } },
  { name: "msg_ack", dir: "agent", frame: { type: "msg_ack", msgId: 99 } },
  { name: "ack (agent)", dir: "agent", frame: { type: "ack", id: 43 } },

  // ── host → agent ──────────────────────────────────────────────────────────
  { name: "task_upsert", dir: "host", frame: { type: "task_upsert", taskId: "t-1", data: { name: "Build", kind: "conversation", status: "active" } } },
  {
    name: "task_upsert (M4 superset)",
    dir: "host",
    frame: {
      type: "task_upsert",
      taskId: "t-1",
      data: {
        name: "Build",
        engine: "claude",
        model: "claude-opus-4-8",
        workdir: "/srv/work",
        meta: { owner: "roy" },
        entries: [{ name: "CLAUDE.md", sha256: "abc123", target: "CLAUDE.md", kind: "skill" }],
      },
    },
  },
  { name: "task_delete", dir: "host", frame: { type: "task_delete", taskId: "t-1" } },
  { name: "cron_upsert", dir: "host", frame: { type: "cron_upsert", taskId: "t-1", cronId: "cr-1", data: { kind: "cron", prompt: "run the daily digest", schedule_type: "interval", schedule_value: "5m", continuity: "isolated" } } },
  { name: "cron_delete", dir: "host", frame: { type: "cron_delete", taskId: "t-1", cronId: "cr-1" } },
  { name: "message", dir: "host", frame: { type: "message", taskId: "t-1", threadId: "c-9@1718", msgId: 5, data: { text: "hello" } } },
  { name: "message (no thread)", dir: "host", frame: { type: "message", taskId: "t-1", data: { text: "kick off" } } },
  { name: "interrupt", dir: "host", frame: { type: "interrupt" } },
  { name: "close", dir: "host", frame: { type: "close" } },
  { name: "config_update", dir: "host", frame: { type: "config_update", data: { heartbeatIntervalMs: 15000 } } },
  { name: "ack (host)", dir: "host", frame: { type: "ack", id: 7 } },
];

describe("protocol v2 frame contract", () => {
  describe("valid frames parse", () => {
    for (const { name, dir, frame } of VALID) {
      it(`accepts ${name}`, () => {
        const any = AnyFrame.safeParse(frame);
        expect(any.success).toBe(true);
        // …and the direction-specific union accepts it too.
        const directional = (dir === "agent" ? AgentFrame : HostFrame).safeParse(frame);
        expect(directional.success).toBe(true);
      });
    }

    it("covers every type in the catalog (15 frame types)", () => {
      const types = new Set(VALID.map(v => (v.frame as { type: string }).type));
      expect(types).toEqual(new Set([
        "thread_result",
        "thread_message",
        "cron_state",
        "heartbeat",
        "msg_ack",
        "ack",
        "task_upsert",
        "task_delete",
        "cron_upsert",
        "cron_delete",
        "message",
        "interrupt",
        "close",
        "config_update",
      ]));
    });
  });

  describe("additive-compatibility (§1 / §4.2)", () => {
    it("accepts unknown DATA fields", () => {
      const r = AnyFrame.safeParse({
        type: "thread_message",
        taskId: "t-1",
        threadId: "c-9",
        data: { seq: 1, kind: "status", brand_new_field: "v2.1", nested: { also: true } },
      });
      expect(r.success).toBe(true);
    });

    it("accepts unknown ENVELOPE fields", () => {
      const r = AnyFrame.safeParse({
        type: "heartbeat",
        futureEnvelopeId: "x",
        data: { activeThreads: 1 },
      });
      expect(r.success).toBe(true);
    });

    it("rejects an unknown `type` (no member matches)", () => {
      const r = AnyFrame.safeParse({ type: "totally_new_frame", taskId: "t-1" });
      expect(r.success).toBe(false);
    });
  });

  describe("invalid frames fail", () => {
    it("missing a required envelope id (thread_result without threadId)", () => {
      const r = AnyFrame.safeParse({ type: "thread_result", taskId: "t-1", data: { status: "succeeded", duration_ms: 1, final_seq: 1 } });
      expect(r.success).toBe(false);
    });

    it("missing a required envelope id (cron_upsert without cronId)", () => {
      const r = HostFrame.safeParse({ type: "cron_upsert", taskId: "t-1", data: { kind: "cron", prompt: "p", continuity: "isolated" } });
      expect(r.success).toBe(false);
    });

    // HOST-M4-10: the agent's CronUpsertData requires `prompt`; the host
    // schema must reject a promptless command instead of emitting one the
    // agent cannot parse.
    it("missing required data field (cron_upsert.prompt)", () => {
      const r = HostFrame.safeParse({ type: "cron_upsert", taskId: "t", cronId: "cr", data: { kind: "cron", continuity: "isolated" } });
      expect(r.success).toBe(false);
    });

    it("bad enum (thread_result.status)", () => {
      const r = AnyFrame.safeParse({ type: "thread_result", taskId: "t", threadId: "c", data: { status: "exploded", duration_ms: 1, final_seq: 1 } });
      expect(r.success).toBe(false);
    });

    // `kind` is an OPEN string (HOST-M1M2-04): an unrecognized value parses —
    // the writer stores it opaquely and flags it — so version drift on this
    // additive field can never poison a frame.
    it("unrecognized thread_message.kind still parses (open string, HOST-M1M2-04)", () => {
      const r = AnyFrame.safeParse({ type: "thread_message", taskId: "t", threadId: "c", data: { seq: 1, kind: "screaming" } });
      expect(r.success).toBe(true);
    });

    it("wrong kind type (thread_message.kind as number)", () => {
      const r = AnyFrame.safeParse({ type: "thread_message", taskId: "t", threadId: "c", data: { seq: 1, kind: 7 } });
      expect(r.success).toBe(false);
    });

    it("missing required data field (cron_upsert.continuity)", () => {
      const r = HostFrame.safeParse({ type: "cron_upsert", taskId: "t", cronId: "cr", data: { kind: "cron" } });
      expect(r.success).toBe(false);
    });

    it("wrong delivery-id type (msg_ack.msgId as string)", () => {
      const r = AgentFrame.safeParse({ type: "msg_ack", msgId: "99" });
      expect(r.success).toBe(false);
    });
  });

  describe("envelope routing surface (§4.1)", () => {
    it("the generic Envelope extracts ids without touching data", () => {
      const e = Envelope.parse({ type: "thread_message", outboxId: 43, taskId: "t-1", threadId: "c-9@1718", data: { seq: 3, kind: "assistant_text" } });
      expect(e.taskId).toBe("t-1");
      expect(e.threadId).toBe("c-9@1718");
      expect(e.outboxId).toBe(43);
    });
  });

  describe("JSON Schema drift guard (protocol.md §8)", () => {
    const protocolMd = readFileSync(resolve(import.meta.dir, "../../docs/protocol.md"), "utf8");

    it("protocol.md contains the generated §8 section", () => {
      expect(protocolMd).toContain(MARKER);
    });

    it("the embedded JSON Schema matches a fresh generation (no drift)", () => {
      const markerIdx = protocolMd.indexOf(`\n${MARKER}`);
      const embedded = protocolMd.slice(markerIdx + 1).replace(/\s*$/, "");
      expect(embedded).toBe(renderSection());
    });

    it("the embedded JSON block is a valid, non-trivial JSON Schema", () => {
      const block = protocolMd.slice(protocolMd.indexOf(`\n${MARKER}`));
      const json = block.slice(block.indexOf("```json") + "```json".length, block.lastIndexOf("```")).trim();
      const parsed = JSON.parse(json) as Record<string, unknown>;
      expect(parsed.$schema).toBeDefined();
      // AnyFrame = anyOf[AgentFrame, HostFrame]; both also exposed under $defs.
      expect(Array.isArray(parsed.anyOf)).toBe(true);
      const defs = parsed.$defs as Record<string, unknown>;
      expect(defs.AgentFrame).toBeDefined();
      expect(defs.HostFrame).toBeDefined();
      // The freshly-built schema must serialise identically to the embedded one.
      expect(parsed).toEqual(JSON.parse(JSON.stringify(buildSchema())));
    });
  });
});
