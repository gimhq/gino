import { Buffer } from "node:buffer";
// M2 — PROTOCOL v2 FRAME CATALOG (the single source of truth for the wire).
//
// This module mirrors `docs/protocol.md` §4 (envelope) + §5 (frame catalog) as
// executable zod. The host and the Rust runner share **no code**; both treat
// `docs/protocol.md` as the contract, so this file MUST stay in lock-step with
// it. `scripts/gen-frames-schema.ts` (in this package) exports these schemas to
// JSON Schema and embeds the result in protocol.md §8 (the machine-checkable
// mirror).
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
  reqId: CorrelationId.optional(),
  ptyId: CorrelationId.optional(),
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

/**
 * The `kind` values this host build knows about. `kind` is deliberately NOT a
 * closed `z.enum`: the protocol is additive-by-design (§1), so
 * an agent build that emits a new transcript kind must not produce a frame the
 * host refuses forever. The inbound handler stores any string `kind` and
 * warn-logs values outside this set; readers treat unknown kinds as opaque.
 */
export const RECOGNIZED_MESSAGE_KINDS: ReadonlySet<string> = new Set([
  "user_input",
  "assistant_text",
  "thinking",
  "tool_use",
  "tool_result",
  "status",
  "error",
]);

/**
 * `thread_message.data` — one ordered transcript event (→ `messages`).
 *
 * Attribution: `parent_tool_use_id` is present on every event produced INSIDE a
 * sub-agent (a `Task` tool call) and absent for the main agent's own events, so
 * absence means "the main agent". `tool_use_id` is the engine's id for a
 * `tool_use` block, echoed on the matching `tool_result`. Neither introduces
 * hierarchy: `seq` stays a single per-thread monotonic stream.
 */
export const ThreadMessageData = z.looseObject({
  seq: z.number().int(),
  kind: z.string(),
  role: z.string().optional(),
  content: z.string().optional(),
  tool: z.string().optional(),
  input: z.unknown().optional(),
  output: z.unknown().optional(),
  tool_use_id: z.string().optional(),
  parent_tool_use_id: z.string().optional(),
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

/** One directory entry or `stat` result. */
export const FsStat = z.looseObject({
  type: z.string(),
  size: z.number().int(),
  mtime_ms: z.number().int().optional(),
});
export type FsStat = z.infer<typeof FsStat>;

/**
 * `fs_response.data` — the outcome of one file operation. `ok: false` carries
 * `error`; otherwise `entries` (list), `stat` (stat), or `sha256` + `size` of
 * the uploaded bytes (get). `error.code` is an open string: `disabled`,
 * `not_found`, `exists`, `too_large`, `invalid`, `denied`, `io`.
 */
export const FsResponseData = z.looseObject({
  ok: z.boolean(),
  entries: z.array(FsStat.extend({ name: z.string() })).optional(),
  stat: FsStat.optional(),
  sha256: z.string().optional(),
  size: z.number().int().optional(),
  error: z.looseObject({ code: z.string(), message: z.string() }).optional(),
});
export type FsResponseData = z.infer<typeof FsResponseData>;

/** Raw terminal bytes, capped at 16 KiB after base64 decoding. */
export const PtyBytesData = z.looseObject({
  // JSON Schema patterns cannot carry JavaScript regex flags.
  // eslint-disable-next-line regexp/use-ignore-case
  data: z.string().max(21848).regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/).refine(value => Buffer.from(value, "base64").byteLength <= 16384),
});
export type PtyBytesData = z.infer<typeof PtyBytesData>;

/** Terminal window dimensions. */
export const PtyResizeData = z.looseObject({ cols: z.number().int().min(1).max(1000), rows: z.number().int().min(1).max(1000) });
export type PtyResizeData = z.infer<typeof PtyResizeData>;

/** Options for a new terminal; relative cwd resolves against the agent workspace. */
export const PtyOpenData = z.looseObject({
  argv: z.array(z.string().max(4096).refine(value => !value.includes("\0"))).min(1).max(64).optional(),
  cwd: z.string().max(4096).refine(value => !value.includes("\0")).optional(),
  env: z.record(z.string().min(1).max(256).regex(/^[^=\0]+$/), z.string().max(4096).refine(value => !value.includes("\0")))
    .refine(value => Object.keys(value).length <= 64)
    .optional(),
  cols: PtyResizeData.shape.cols.optional(),
  rows: PtyResizeData.shape.rows.optional(),
}).refine(value => Buffer.byteLength(JSON.stringify(value)) <= 65536);
export type PtyOpenData = z.infer<typeof PtyOpenData>;

/** Terminal completion, including a failed open or forced close. */
export const PtyExitData = z.looseObject({
  exit_code: z.number().int().optional(),
  signal: z.number().int().optional(),
  error: z.looseObject({ code: z.string(), message: z.string() }).optional(),
});
export type PtyExitData = z.infer<typeof PtyExitData>;

/** Terminal frames are live-only and correlated by ptyId. */
export const PtyOpenFrame = z.looseObject({ type: z.literal("pty_open"), ptyId: CorrelationId, data: PtyOpenData });
export const PtyInputFrame = z.looseObject({ type: z.literal("pty_input"), ptyId: CorrelationId, data: PtyBytesData });
export const PtyResizeFrame = z.looseObject({ type: z.literal("pty_resize"), ptyId: CorrelationId, data: PtyResizeData });
export const PtyCloseFrame = z.looseObject({ type: z.literal("pty_close"), ptyId: CorrelationId });
export const PtyReadyFrame = z.looseObject({ type: z.literal("pty_ready"), ptyId: CorrelationId, data: z.looseObject({}) });
export const PtyOutputFrame = z.looseObject({ type: z.literal("pty_output"), ptyId: CorrelationId, data: PtyBytesData });
export const PtyExitFrame = z.looseObject({ type: z.literal("pty_exit"), ptyId: CorrelationId, data: PtyExitData });

// ── `data` payloads — host → agent ──────────────────────────────────────────

/** A desired config/skill entry (M8) — engine-resolved `target`. */
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

/**
 * `cron_upsert.data` — a scheduling COMMAND (`continuity` required here).
 * `prompt` is the recurring instruction the cron runs each fire — REQUIRED:
 * the agent's `CronUpsertData` rejects a promptless command, so the host
 * schema matches it.
 */
export const CronUpsertData = z.looseObject({
  kind: z.string(),
  prompt: z.string(),
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

/**
 * A metadata operation a caller may ask for directly. A relative `path`
 * resolves against the agent's workspace directory; an absolute one is used
 * as given.
 */
export const FsOpData = z.looseObject({
  op: z.enum(["list", "stat", "mkdir", "remove", "rename"]),
  path: z.string().min(1),
  /** `rename` destination. */
  to: z.string().min(1).optional(),
  /** `mkdir` parents / `remove` a non-empty directory. */
  recursive: z.boolean().optional(),
});
export type FsOpData = z.infer<typeof FsOpData>;

/**
 * `fs_request.data` — one file operation. Adds the two transfers to
 * `FsOpData`; file bytes never ride in a frame, they move over the attachment
 * byte channel. `put` names the blob to download (`ref`, `sha256`, `size`);
 * `get` names the upload the agent must make (`upload`). Only the host mints
 * these, so they are not part of `FsOpData`.
 */
export const FsRequestData = FsOpData.extend({
  op: z.enum(["list", "stat", "mkdir", "remove", "rename", "put", "get"]),
  ref: z.string().optional(),
  sha256: z.string().optional(),
  size: z.number().int().nonnegative().optional(),
  upload: z.string().optional(),
});
export type FsRequestData = z.infer<typeof FsRequestData>;

// ── Frames — agent → host ───────────────────────────────────────────────────

/** `thread_result` — agent→host, one thread reached its terminal state. */
export const ThreadResultFrame = z.looseObject({
  type: z.literal("thread_result"),
  taskId: CorrelationId,
  threadId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: ThreadResultData,
});

/** `thread_message` — agent→host, one ordered transcript event. Deduped on `(threadId, data.seq)`. */
export const ThreadMessageFrame = z.looseObject({
  type: z.literal("thread_message"),
  taskId: CorrelationId,
  threadId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: ThreadMessageData,
});

/** `cron_state` — agent→host, the scheduling ledger for one cron. Never carries the prompt. */
export const CronStateFrame = z.looseObject({
  type: z.literal("cron_state"),
  taskId: CorrelationId,
  cronId: CorrelationId,
  outboxId: DeliveryId.optional(),
  data: CronStateData,
});

/** `heartbeat` — agent→host liveness at the advertised interval, optionally reporting in-flight threads. */
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

/** `fs_response` — agent→host, answers the `fs_request` with the same `reqId`. Live-only: never queued or acked. */
export const FsResponseFrame = z.looseObject({
  type: z.literal("fs_response"),
  reqId: CorrelationId,
  data: FsResponseData,
});

// ── Frames — host → agent ───────────────────────────────────────────────────

// Every durable host→agent frame carries `msgId` (its outbox row id): the
// runner dedups inbound frames and acknowledges durable storage by `msgId` +
// `msg_ack` alone (§4.2) — `outboxId` rides along for compatibility but is
// never acked by the runner.
/** `task_upsert` — host→agent, seed or modify a task. Sent once per placement before any work. */
export const TaskUpsertFrame = z.looseObject({
  type: z.literal("task_upsert"),
  taskId: CorrelationId,
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
  data: TaskUpsertData,
});

/** `task_delete` — host→agent, drop a task and everything scheduled under it. */
export const TaskDeleteFrame = z.looseObject({
  type: z.literal("task_delete"),
  taskId: CorrelationId,
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
});

/** `cron_upsert` — host→agent, create or update a trigger the AGENT then owns and fires. */
export const CronUpsertFrame = z.looseObject({
  type: z.literal("cron_upsert"),
  taskId: CorrelationId,
  cronId: CorrelationId,
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
  data: CronUpsertData,
});

/** `cron_delete` — host→agent, remove a trigger. */
export const CronDeleteFrame = z.looseObject({
  type: z.literal("cron_delete"),
  taskId: CorrelationId,
  cronId: CorrelationId,
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
});

/** `message` — host→agent, user/system input for a task. Acked by `msg_ack` once durably stored. */
export const MessageFrame = z.looseObject({
  type: z.literal("message"),
  taskId: CorrelationId,
  threadId: CorrelationId.optional(),
  // `message` delivery advances the host cursor via `msgId` + `msg_ack` (§4.2).
  msgId: DeliveryId.optional(),
  data: MessageData,
});

/** `interrupt` — host→agent, cancel the current turn. Live-only: never queued, never replayed. */
export const InterruptFrame = z.looseObject({
  type: z.literal("interrupt"),
  /**
   * Present → the interrupt is scoped to that task (the agent cancels the
   * current turn only if that task owns it); absent → global (cancel the
   * current turn regardless).
   */
  taskId: CorrelationId.optional(),
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
});

/** `close` — host→agent, wind the runner down. Not the per-task close; that is an `interrupt` plus a `task_upsert`. */
export const CloseFrame = z.looseObject({
  type: z.literal("close"),
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
});

/** `config_update` — host→agent, an opaque live-config overlay. */
export const ConfigUpdateFrame = z.looseObject({
  type: z.literal("config_update"),
  outboxId: DeliveryId.optional(),
  msgId: DeliveryId.optional(),
  data: ConfigUpdateData.optional(),
});

/** `fs_request` — host→agent, one engine-free file operation. Live-only: never queued, never replayed. */
export const FsRequestFrame = z.looseObject({
  type: z.literal("fs_request"),
  reqId: CorrelationId,
  data: FsRequestData,
});

// ── Unions + inferred types ─────────────────────────────────────────────────

/** Everything the agent sends to the host (discriminated on `type`). */
export const AgentFrame = z.discriminatedUnion("type", [
  ThreadResultFrame,
  ThreadMessageFrame,
  CronStateFrame,
  HeartbeatFrame,
  FsResponseFrame,
  PtyReadyFrame,
  PtyOutputFrame,
  PtyExitFrame,
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
  FsRequestFrame,
  PtyOpenFrame,
  PtyInputFrame,
  PtyResizeFrame,
  PtyCloseFrame,
  AckFrame,
]);
export type HostFrame = z.infer<typeof HostFrame>;

/**
 * Any valid v2 frame, either direction. A plain union of the two
 * direction-specific discriminated unions (both carry `ack`, so a single
 * discriminated union over both directions would collide on the `ack` tag).
 * An unknown `type` matches no member and is REJECTED at parse time — the M2
 * inbound handler turns that rejection into the §4.2 "ignore unknown type"
 * policy.
 */
export const AnyFrame = z.union([AgentFrame, HostFrame]);
export type AnyFrame = z.infer<typeof AnyFrame>;
