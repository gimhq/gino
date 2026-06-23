// M5 — `cron_state` → `crons` (READ-ONLY MIRROR) + cron reads.
//
// Crons LIVE ON THE AGENT (OD14); the host keeps a mirror written ONLY from
// inbound `cron_state` frames — the agent is authoritative for scheduling. The
// host derives *dispatched* counts from `thread_runs` by `cron_id`; the
// runner-local `round_count` / `skipped_count` summary is mirrored verbatim.
//
// The `crons` row carries two columns the `cron_state` frame does not: `prompt`
// (the work command, owned by the dispatch side) and `continuity` is optional
// on the wire. So the UPSERT preserves `prompt` across mirror updates (never
// clobbers it) and only overwrites `continuity` when the frame actually carries
// it. `next_run` arrives as a string on the wire but the column is epoch-ms, so
// it is coerced.
import type { AppDatabase } from "@/db";
import type { Cron } from "@/db/schema";
import type { CronStateData } from "@/protocol/frames";
import { eq } from "drizzle-orm";
import { crons } from "@/db/schema";

export interface CronStateWrite {
  taskId: string;
  cronId: string;
  data: CronStateData;
}

/** Coerce the wire `next_run` (string: epoch-ms or ISO) to epoch-ms, or null. */
function toEpochMs(value: string | undefined): number | null {
  if (value == null) {
    return null;
  }
  if (/^\d+$/.test(value)) {
    return Number(value);
  }
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

/**
 * Mirror one `cron_state` into `crons`. UPSERT keyed by the cron `id`: INSERT
 * on first sight, and on a later frame overwrite the mirror fields
 * (`kind` / `schedule_*` / `next_run` / `status` / `last_status` /
 * `round_count` / `skipped_count`). `prompt` and `task_id` are set once and
 * preserved; `continuity` is updated only when the frame carries it. Idempotent
 * on the cron id — no duplicate rows, ever.
 */
export function writeCronState(db: AppDatabase, { taskId, cronId, data }: CronStateWrite): void {
  const mirror = {
    kind: data.kind,
    scheduleType: data.schedule_type ?? null,
    scheduleValue: data.schedule_value ?? null,
    nextRun: toEpochMs(data.next_run),
    status: data.status ?? null,
    lastStatus: data.last_status ?? null,
    roundCount: data.round_count,
    skippedCount: data.skipped_count,
    updatedAt: Date.now(),
  };

  db.insert(crons)
    .values({
      id: cronId,
      taskId,
      // `prompt` is the dispatch-side work command — not carried by `cron_state`.
      // Seed empty on first mirror; preserved (never in the conflict set) after.
      prompt: "",
      continuity: data.continuity ?? "",
      ...mirror,
    })
    .onConflictDoUpdate({
      target: crons.id,
      set: {
        ...mirror,
        ...(data.continuity !== undefined ? { continuity: data.continuity } : {}),
      },
    })
    .run();
}

/** The mirrored cron row, or `undefined` if no `cron_state` has been seen. */
export function getCron(db: AppDatabase, cronId: string): Cron | undefined {
  return db
    .select()
    .from(crons)
    .where(eq(crons.id, cronId))
    .get();
}
