// M2 — PROTOCOL v2 FRAME CATALOG (the single source of truth for the wire).
//
// This module mirrors `docs/protocol.md` §4 (envelope) + §5 (frame catalog) as
// executable zod. The host and the Rust runner share **no code**; both treat
// `docs/protocol.md` as the contract, so this file MUST stay in lock-step with
// it. `scripts/gen-frames-schema.ts` exports these schemas to JSON Schema and
// embeds the result in protocol.md §8 (the machine-checkable mirror).
//
// Design rules (protocol §1 + §4):
//   • Envelope vs payload (§4.1): correlation/delivery ids live at the TOP
//     LEVEL of every frame; `data` carries business fields only and NEVER
//     repeats the correlation ids.
//   • Additive-compatibility (§1 + §4.2): receivers MUST ignore unknown fields
//     and unknown `type`s. Every frame and every `data` object is therefore a
//     `looseObject` — additive fields parse, never fail. This contract is a
//     forward-compatible SUPERSET of §5, never a stricter subset.
//   • Dependency-light + side-effect-free: imported by the M2 codec/inbound/
//     outbox later; no runtime deps beyond zod v4.
import { z } from "zod";

// Each frame/payload is exported as a zod schema (the value) AND its inferred
// TS type under the SAME canonical name — the idiomatic zod value+type merge,
// so M2 consumers write `AnyFrame.parse(x)` and `(f: AnyFrame)` with one import.
// That const+type pairing is legal TS but trips `ts/no-redeclare`; disable it
// for this pure schema module.
/* eslint-disable ts/no-redeclare */

// ── Primitives ──────────────────────────────────────────────────────────────

/** Delivery / ack ids — `outboxId`, `msgId`, and the ack reference `id`. */
const DeliveryId = z.number().int();
/** Correlation ids — `taskId`, `threadId`, `cronId` (opaque strings). */
const CorrelationId = z.string();

/**
 * Attachment descriptor (protocol §5 / agent-architecture §9). Bytes never
 * travel in frames — only this descriptor does. Loose so additive descriptor
 * fields never fail validation.
 */
export const AttachmentDescriptor = z.looseObject({
  id: z.string(),
  name: z.string(),
  content_type: z.string(),
  size: z.number().int(),
  sha256: z.string(),
  ref: z.string(),
  direction: z.enum(["inbound", "outbound"]).optional(),
});
export type AttachmentDescriptor = z.infer<typeof AttachmentDescriptor>;

/**
 * The generic envelope (protocol §4.1). Every concrete frame below is a
 * superset of this shape: `type` + the delivery/correlation ids at the top
 * level. Loose — receivers ignore unknown envelope fields (§4.2). Use the
 * discriminated unions (`AgentFrame` / `HostFrame` / `AnyFrame`) for actual
 * parsing; this schema documents the routing surface the codec reads without
 * touching `data`.
 */
export const Envelope = z.looseObject({
  type: z.string(),
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
  id: DeliveryId.optional(),
  taskId: CorrelationId.optional(),
  threadId: CorrelationId.optional(),
  cronId: CorrelationId.optional(),
});
export type Envelope = z.infer<typeof Envelope>;

// ── `data` payloads — agent → host ──────────────────────────────────────────

/** `thread_result.data` — terminal outcome of one execution (→ `thread_runs`). */
export const ThreadResultData = z.looseObject({
  status: z.enum(["succeeded", "failed", "cancelled"]),
  result: z.string().optional(),
  error: z.string().optional(),
  failure_reason: z.string().optional(),
  // `engine_session_id` is a VALUE of the thread (the engine `--resume` handle),
  // so it lives inside `data` (stored, not routed-by) — protocol §4.1.
  engine_session_id: z.string().optional(),
  duration_ms: z.number().int(),
  final_seq: z.number().int(),
  attachments: z.array(AttachmentDescriptor).optional(),
});
export type ThreadResultData = z.infer<typeof ThreadResultData>;

/** `thread_message.data` — one ordered transcript event (→ `messages`). */
export const ThreadMessageData = z.looseObject({
  seq: z.number().int(),
  kind: z.enum(["user_input", "assistant_text", "thinking", "tool_use", "tool_result", "status", "error"]),
  role: z.string().optional(),
  content: z.string().optional(),
  tool: z.string().optional(),
  input: z.unknown().optional(),
  output: z.unknown().optional(),
});
export type ThreadMessageData = z.infer<typeof ThreadMessageData>;

/** `cron_state.data` — local scheduling-ledger summary (→ `crons`). */
export const CronStateData = z.looseObject({
  kind: z.string(),
  schedule_type: z.string().optional(),
  schedule_value: z.string().optional(),
  continuity: z.string().optional(),
  next_run: z.string().optional(),
  status: z.string().optional(),
  round_count: z.number().int(),
  skipped_count: z.number().int(),
  last_status: z.string().optional(),
});
export type CronStateData = z.infer<typeof CronStateData>;

/** `heartbeat.data` — liveness. `activeThreads` is the one camelCase exception. */
export const HeartbeatData = z.looseObject({
  activeThreads: z.number().int().optional(),
});
export type HeartbeatData = z.infer<typeof HeartbeatData>;

// ── `data` payloads — host → agent ──────────────────────────────────────────

/** A desired config/skill entry (M8 / PLAN-007) — engine-resolved `target`. */
export const ConfigEntry = z.looseObject({
  name: z.string(),
  sha256: z.string(),
  target: z.string(),
  kind: z.string(),
});
export type ConfigEntry = z.infer<typeof ConfigEntry>;

/**
 * `task_upsert.data` — seed/modify a task. §5 lists `{name?, kind?, status?}`;
 * this is the forward-compatible SUPERSET that also carries the M4 orchestrator
 * extras (docs/host-v2-design.md M4: `engine`/`model`/`workdir`/`meta` + config
 * `entries`). All extras optional.
 */
export const TaskUpsertData = z.looseObject({
  name: z.string().optional(),
  kind: z.string().optional(),
  status: z.string().optional(),
  engine: z.string().optional(),
  model: z.string().optional(),
  workdir: z.string().optional(),
  meta: z.looseObject({}).optional(),
  entries: z.array(ConfigEntry).optional(),
});
export type TaskUpsertData = z.infer<typeof TaskUpsertData>;

/** `cron_upsert.data` — a scheduling COMMAND (`continuity` required here). */
export const CronUpsertData = z.looseObject({
  kind: z.string(),
  schedule_type: z.string().optional(),
  schedule_value: z.string().optional(),
  continuity: z.string(),
  next_run: z.string().optional(),
  status: z.string().optional(),
});
export type CronUpsertData = z.infer<typeof CronUpsertData>;

/** `message.data` — user/system input for a task (optionally a live thread). */
export const MessageData = z.looseObject({
  text: z.string(),
  attachments: z.array(AttachmentDescriptor).optional(),
});
export type MessageData = z.infer<typeof MessageData>;

/** `config_update.data` — opaque live-config overlay (any keys). */
export const ConfigUpdateData = z.looseObject({});
export type ConfigUpdateData = z.infer<typeof ConfigUpdateData>;

// ── Frames — agent → host ───────────────────────────────────────────────────

export const ThreadResultFrame = z.looseObject({
  type: z.literal("thread_result"),
  taskId: CorrelationId,
  threadId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: ThreadResultData,
});

export const ThreadMessageFrame = z.looseObject({
  type: z.literal("thread_message"),
  taskId: CorrelationId,
  threadId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: ThreadMessageData,
});

export const CronStateFrame = z.looseObject({
  type: z.literal("cron_state"),
  taskId: CorrelationId,
  cronId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: CronStateData,
});

export const HeartbeatFrame = z.looseObject({
  type: z.literal("heartbeat"),
  outboxId: DeliveryId.optional(),
  data: HeartbeatData.optional(),
});

/** `msg_ack` — durably stored an inbound `message`; references its `msgId`. */
export const MsgAckFrame = z.looseObject({
  type: z.literal("msg_ack"),
  msgId: DeliveryId,
});

/** `ack` — acked a peer outbound frame; references its `outboxId` via `id`. */
export const AckFrame = z.looseObject({
  type: z.literal("ack"),
  id: DeliveryId,
});

// ── Frames — host → agent ───────────────────────────────────────────────────

export const TaskUpsertFrame = z.looseObject({
  type: z.literal("task_upsert"),
  taskId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: TaskUpsertData,
});

export const TaskDeleteFrame = z.looseObject({
  type: z.literal("task_delete"),
  taskId: CorrelationId,
  outboxId: DeliveryId.optional(),
});

export const CronUpsertFrame = z.looseObject({
  type: z.literal("cron_upsert"),
  taskId: CorrelationId,
  cronId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: CronUpsertData,
});

export const CronDeleteFrame = z.looseObject({
  type: z.literal("cron_delete"),
  taskId: CorrelationId,
  cronId: CorrelationId,
  outboxId: DeliveryId.optional(),
});

export const MessageFrame = z.looseObject({
  type: z.literal("message"),
  taskId: CorrelationId,
  threadId: CorrelationId.optional(),
  // `message` delivery advances the host cursor via `msgId` + `msg_ack` (§4.2).
  msgId: DeliveryId.optional(),
  data: MessageData,
});

export const InterruptFrame = z.looseObject({
  type: z.literal("interrupt"),
  outboxId: DeliveryId.optional(),
});

export const CloseFrame = z.looseObject({
  type: z.literal("close"),
  outboxId: DeliveryId.optional(),
});

export const ConfigUpdateFrame = z.looseObject({
  type: z.literal("config_update"),
  outboxId: DeliveryId.optional(),
  data: ConfigUpdateData.optional(),
});

// ── Unions + inferred types ─────────────────────────────────────────────────

/** Everything the agent sends to the host (discriminated on `type`). */
export const AgentFrame = z.discriminatedUnion("type", [
  ThreadResultFrame,
  ThreadMessageFrame,
  CronStateFrame,
  HeartbeatFrame,
  MsgAckFrame,
  AckFrame,
]);
export type AgentFrame = z.infer<typeof AgentFrame>;

/** Everything the host sends to the agent (discriminated on `type`). */
export const HostFrame = z.discriminatedUnion("type", [
  TaskUpsertFrame,
  TaskDeleteFrame,
  CronUpsertFrame,
  CronDeleteFrame,
  MessageFrame,
  InterruptFrame,
  CloseFrame,
  ConfigUpdateFrame,
  AckFrame,
]);
export type HostFrame = z.infer<typeof HostFrame>;

/**
 * Any valid v2 frame, either direction. A plain union of the two
 * direction-specific discriminated unions (both carry `ack`, so a single
 * discriminated union over all 15 frames would collide on the `ack` tag).
 * An unknown `type` matches no member and is REJECTED at parse time — the M2
 * inbound handler turns that rejection into the §4.2 "ignore unknown type"
 * policy.
 */
export const AnyFrame = z.union([AgentFrame, HostFrame]);
export type AnyFrame = z.infer<typeof AnyFrame>;
