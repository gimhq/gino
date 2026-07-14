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
import type { AppDatabase } from "@/db";
import type { ThreadRun } from "@/db/schema";
import type { ThreadResultData } from "@/protocol/frames";
import { count, eq, max } from "drizzle-orm";
import { messages, threadRuns } from "@/db/schema";

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
  db: AppDatabase,
  { taskId, threadId, data, source = "chat", cronId }: ThreadResultWrite,
): void {
  db.insert(threadRuns)
    .values({
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
    })
    .onConflictDoUpdate({
      target: threadRuns.threadId,
      set: {
        status: data.status,
        failureReason: data.failure_reason ?? null,
        engineSessionId: data.engine_session_id ?? null,
        durationMs: data.duration_ms,
        finalSeq: data.final_seq,
      },
    })
    .run();
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
export function checkTranscriptCompleteness(db: AppDatabase, threadId: string, finalSeq: number): TranscriptCompleteness {
  const row = db
    .select({ messageCount: count(), maxSeq: max(messages.seq) })
    .from(messages)
    .where(eq(messages.threadId, threadId))
    .get();
  const messageCount = row?.messageCount ?? 0;
  const maxSeq = row?.maxSeq ?? null;
  return {
    complete: messageCount === finalSeq && (maxSeq ?? 0) === finalSeq,
    messageCount,
    maxSeq,
    finalSeq,
  };
}

/** The run record for a thread (terminal summary), or `undefined` if none yet. */
export function getThreadRun(db: AppDatabase, threadId: string): ThreadRun | undefined {
  return db
    .select()
    .from(threadRuns)
    .where(eq(threadRuns.threadId, threadId))
    .get();
}
