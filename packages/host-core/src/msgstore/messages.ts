// M5 — `thread_message` → `messages` (the durable transcript) + transcript reads.
//
// The transcript is the centerpiece of the store and the source M6 distributes
// from. Writes come ONLY from inbound `thread_message` frames; the writer is
// idempotent on the `(thread_id, seq)` unique key, so an at-least-once replay
// of a frame never duplicates a row and never errors. Order is by `seq`
// (per-thread monotonic) — NEVER by arrival or `outboxId` (protocol §6).
//
// Retention: transcript rows are append-only here — there is
// deliberately NO delete path in the store module. Reclamation belongs to the
// unified store GC alongside attachments/config artifacts (a documented no-op
// today); what this module DOES enforce is that every single-query read is
// bounded (`TRANSCRIPT_READ_LIMIT` below, batch caps in `channels/egress.ts`).
import type { MessageRow, Storage } from "../host/ports/storage";
import type { ThreadMessageData } from "../protocol/frames";

export interface ThreadMessageWrite {
  taskId: string;
  threadId: string;
  data: ThreadMessageData;
}

/**
 * Persist one transcript event into `messages`. Idempotent on
 * `(thread_id, seq)`: a replayed frame is silently ignored (first writer
 * wins), so the FIRST write of a given `(thread_id, seq)` wins and a later
 * duplicate can neither overwrite nor raise. Nothing is synthesized — only the
 * fields the frame carries are stored.
 */
export function writeThreadMessage(storage: Storage, { taskId, threadId, data }: ThreadMessageWrite): void {
  storage.transcript.insertIfAbsent({
    taskId,
    threadId,
    seq: data.seq,
    kind: data.kind,
    role: data.role ?? null,
    content: data.content ?? null,
    tool: data.tool ?? null,
    input: data.input ?? null,
    output: data.output ?? null,
    toolUseId: data.tool_use_id ?? null,
    parentToolUseId: data.parent_tool_use_id ?? null,
  });
}

/** Per-query row cap on transcript reads — page with `afterSeq` for longer threads. */
export const TRANSCRIPT_READ_LIMIT = 1000;

export interface TranscriptReadOptions {
  /** Read messages with `seq > afterSeq` (pagination cursor). */
  afterSeq?: number;
  /** Row cap for this read (defaults to and is capped at `TRANSCRIPT_READ_LIMIT`). */
  limit?: number;
}

/** Transcript for a thread, `ORDER BY seq` (protocol §6 — never arrival), bounded to at most `TRANSCRIPT_READ_LIMIT` rows. */
export function getTranscript(storage: Storage, threadId: string, opts: TranscriptReadOptions = {}): MessageRow[] {
  const limit = Math.min(Math.max(opts.limit ?? TRANSCRIPT_READ_LIMIT, 1), TRANSCRIPT_READ_LIMIT);
  return storage.transcript.read(threadId, {
    ...(opts.afterSeq != null ? { afterSeq: opts.afterSeq } : {}),
    limit,
  });
}

export interface TranscriptStats {
  messageCount: number;
  /** Highest stored `seq`, or null when the thread has no transcript rows. */
  lastSeq: number | null;
}

/** Per-thread transcript summary (count + last seq) for bounded list views. */
export function getTranscriptStats(storage: Storage, threadId: string): TranscriptStats {
  const stats = storage.transcript.stats(threadId);
  return { messageCount: stats.messageCount, lastSeq: stats.lastSeq };
}
