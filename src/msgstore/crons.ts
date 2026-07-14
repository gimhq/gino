// M5 — `cron_state` → `crons` (READ-ONLY MIRROR) + cron reads.
//
// Crons LIVE ON THE AGENT (OD14); the host keeps a mirror written ONLY from
// inbound `cron_state` frames — the agent is authoritative for scheduling. The
// host derives *dispatched* counts from `thread_runs` by `cron_id`; the
// runner-local `round_count` / `skipped_count` summary is mirrored verbatim.
//
// The `crons` row carries two columns the `cron_state` frame does not: `prompt`
// (the work command, owned by the dispatch side — persisted at `cron_upsert`
// emit time via `writeCronPrompt`, the one dispatch-side write into this
// mirror) and `continuity` is optional on the wire. So the state UPSERT
// preserves `prompt` across mirror updates (never clobbers it) and only
// overwrites `continuity` when the frame actually carries it. `next_run`
// arrives as a string on the wire but the column is epoch-ms, so it is coerced.
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

/** ISO date-time with a time part but NO timezone designator (`Z`/offset). */
const TZ_LESS_ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;

/**
 * Coerce the wire `next_run` (string: epoch-ms or ISO) to epoch-ms, or null.
 * ECMAScript reads a timezone-less ISO date-time in the host's LOCAL zone, so
 * the stored value would shift with the host `TZ` (HOST-XC-01) — the wire value
 * is UTC, so pin a TZ-less string with an explicit `Z` before parsing.
 */
function toEpochMs(value: string | undefined): number | null {
  if (value == null) {
    return null;
  }
  if (/^\d+$/.test(value)) {
    return Number(value);
  }
  const parsed = Date.parse(TZ_LESS_ISO.test(value) ? `${value}Z` : value);
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

export interface CronPromptWrite {
  taskId: string;
  cronId: string;
  prompt: string;
}

/**
 * Persist the dispatch-owned `prompt` when a `cron_upsert` COMMAND is emitted —
 * `cron_state` never carries it, so without this write the mirror column stays
 * empty forever (HOST-M5M6-09). INSERT a stub row when the command precedes the
 * first `cron_state` (the usual order — the host mints the id); on conflict
 * update ONLY `prompt`, leaving every mirror field to `writeCronState`.
 */
export function writeCronPrompt(db: AppDatabase, { taskId, cronId, prompt }: CronPromptWrite): void {
  db.insert(crons)
    .values({ id: cronId, taskId, kind: "", prompt, continuity: "" })
    .onConflictDoUpdate({ target: crons.id, set: { prompt, updatedAt: Date.now() } })
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
