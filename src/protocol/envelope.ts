// M2 — ENVELOPE CODEC (the routing layer of the only wire-aware module).
//
// One JSON object per frame (protocol §4.1): the ENVELOPE carries `type` + the
// delivery ids (`outboxId` / `msgId` / `id`) + the correlation ids (`taskId` /
// `threadId` / `cronId`) at the TOP LEVEL; `data` carries business fields only
// and never repeats the ids. This module reads/writes that envelope and nothing
// else:
//   • decode — raw JSON → a `DecodedFrame` (typed routing ids + an OPAQUE
//     `data`). It validates the ENVELOPE shape via `frames.ts` (`Envelope`) but
//     does NOT parse `data` — business validation is the inbound dispatch's job
//     (still M2), keeping "decode" a pure routing concern (protocol §4.1: route
//     by envelope ids without parsing `data`).
//   • encode — an `outbox` row → the wire frame, rebuilding the envelope from
//     the row's columns (the durable host→agent path; protocol §4.2).
import type { OutboxRow } from "@/host/ports/storage";
import { Envelope } from "@/protocol/frames";

/**
 * A frame reduced to its routing surface. `data` is intentionally `unknown`:
 * the codec never interprets it — the per-type inbound handler validates it
 * against the matching `*Data` schema right before it hands it to an M5 writer.
 */
export interface DecodedFrame {
  type: string;
  outboxId?: number;
  msgId?: number;
  id?: number;
  taskId?: string;
  threadId?: string;
  cronId?: string;
  data?: unknown;
}

/** Normalise a WS payload (text frame or binary frame) to a JSON string. */
function toText(raw: string | ArrayBuffer | Uint8Array): string {
  if (typeof raw === "string") {
    return raw;
  }
  return new TextDecoder().decode(raw);
}

/**
 * Decode one inbound wire frame to its routing envelope. Returns `null` for a
 * non-JSON payload or a frame whose envelope is structurally invalid (e.g.
 * `type` missing) — the inbound handler turns `null` into the protocol §4.2
 * "ignore unknown / malformed" policy. Unknown envelope fields are preserved by
 * the loose `Envelope` schema and simply ignored downstream; `data` rides along
 * untouched.
 */
export function decodeFrame(raw: string | ArrayBuffer | Uint8Array): DecodedFrame | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(toText(raw));
  }
  catch {
    return null;
  }

  const envelope = Envelope.safeParse(parsed);
  if (!envelope.success) {
    return null;
  }

  const e = envelope.data;
  const data = (parsed as { data?: unknown }).data;
  return {
    type: e.type,
    ...(e.outboxId != null ? { outboxId: e.outboxId } : {}),
    ...(e.msgId != null ? { msgId: e.msgId } : {}),
    ...(e.id != null ? { id: e.id } : {}),
    ...(e.taskId != null ? { taskId: e.taskId } : {}),
    ...(e.threadId != null ? { threadId: e.threadId } : {}),
    ...(e.cronId != null ? { cronId: e.cronId } : {}),
    ...(data !== undefined ? { data } : {}),
  };
}

/**
 * Encode a durable `outbox` row into the host→agent wire frame, rebuilding the
 * envelope from the row's columns (protocol §4.2 — the outbox is the sole
 * host→agent path). EVERY durable frame's delivery id is `msgId` (acked by
 * `msg_ack` after the runner's durable store) and is DERIVED from the row id,
 * so even a row written by a host that crashed mid-enqueue carries an ackable
 * id. The runner keys its inbound dedup and its `msg_ack` on `msgId` alone —
 * it never acknowledges an `outboxId` — so `msgId` is the only id that stops
 * replay. Non-`message` frames additionally carry `outboxId` (same row id) for
 * §4.2 compatibility with receivers that ack by `ack{id}`. Correlation ids and
 * `data` are emitted only when present, so a delete/control frame (which
 * carries no `data`) stays envelope-only.
 */
export function encodeOutbox(row: OutboxRow): Record<string, unknown> {
  const frame: Record<string, unknown> = { type: row.type };

  frame.msgId = row.id;
  if (row.type !== "message") {
    frame.outboxId = row.id;
  }

  if (row.taskId != null) {
    frame.taskId = row.taskId;
  }
  if (row.threadId != null) {
    frame.threadId = row.threadId;
  }
  if (row.cronId != null) {
    frame.cronId = row.cronId;
  }

  const data = row.dataJson;
  if (data != null && typeof data === "object" && Object.keys(data).length > 0) {
    frame.data = data;
  }

  return frame;
}
