// M5 — `thread_result` → `thread_runs` (one row per execution) + run reads.
//
// The terminal outcome of one thread execution, keyed by `thread_id`. There is
// no "thread started" inbound frame, so the row is created here on the terminal
// result and UPSERTed: re-applying the same result overwrites the same row
// (never a duplicate). Only the terminal mirror fields are touched on conflict,
// so an identical replay is a true no-op and the original receipt timestamps
// (`created_at` / `finished_at`) are preserved.
//
// `source` and `cronId` are thread *context* the inbound handler (M2) carries
// from the dispatch that opened the thread — the `thread_result` frame does not
// repeat them (protocol §4.1). `result`/`error` from the frame are NOT stored:
// the schema has no column for them; the assistant's text already lives in the
// transcript (`messages`), and `thread_runs` is the terminal summary only.
import type { Storage, ThreadRunRow } from "../host/ports/storage";
import type { ThreadResultData } from "../protocol/frames";

export interface ThreadResultWrite {
  taskId: string;
  threadId: string;
  data: ThreadResultData;
  /** Thread origin (`chat` | `cron` | …) — carried by M2 from dispatch context. */
  source?: string;
  /** Owning cron, when the thread was opened by a cron dispatch. */
  cronId?: string;
}

/**
 * Persist the terminal outcome of a thread. UPSERT keyed by `thread_id`:
 * INSERT the run on first sight, and on a replay UPDATE only the terminal
 * mirror fields (`status` / `failure_reason` / `engine_session_id` /
 * `duration_ms` / `final_seq`). Idempotent — an identical replay leaves every
 * column unchanged; `source`/`cron_id`/timestamps are set once at creation.
 */
export function writeThreadResult(
  storage: Storage,
  { taskId, threadId, data, source = "chat", cronId }: ThreadResultWrite,
): void {
  storage.threadRuns.upsert({
    threadId,
    taskId,
    cronId: cronId ?? null,
    source,
    status: data.status,
    failureReason: data.failure_reason ?? null,
    engineSessionId: data.engine_session_id ?? null,
    durationMs: data.duration_ms,
    finalSeq: data.final_seq,
    finishedAt: Date.now(),
  });
}

export interface TranscriptCompleteness {
  /** True iff the stored transcript is exactly `1..=final_seq`. */
  complete: boolean;
  messageCount: number;
  maxSeq: number | null;
  finalSeq: number;
}

/**
 * Compare the stored transcript against `thread_result.final_seq` (protocol §6:
 * the host verifies it received `1..=final_seq`) — HOST-M1M2-13. `(thread_id,
 * seq)` is unique, so `COUNT(*) == final_seq && MAX(seq) == final_seq` holds
 * exactly when every seq in `1..=final_seq` is present (seq ≥ 1 per §6).
 * Diagnostic only — the caller logs/flags a gap, it never blocks the ack.
 */
export function checkTranscriptCompleteness(storage: Storage, threadId: string, finalSeq: number): TranscriptCompleteness {
  const { messageCount, lastSeq: maxSeq } = storage.transcript.stats(threadId);
  return {
    complete: messageCount === finalSeq && (maxSeq ?? 0) === finalSeq,
    messageCount,
    maxSeq,
    finalSeq,
  };
}

/** The run record for a thread (terminal summary), or `undefined` if none yet. */
export function getThreadRun(storage: Storage, threadId: string): ThreadRunRow | undefined {
  return storage.threadRuns.get(threadId);
}
