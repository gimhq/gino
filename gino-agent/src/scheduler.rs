//! Engine-agnostic local scheduler — v2 scheduling plane (agent-architecture
//! §4/§5/§7, PLAN-006 §3 Lane C).
//!
//! The runner owns its triggers. A single tokio task loads the active
//! [`crons`](crate::db) on each pass, sleeps until the earliest `next_run` is due
//! (re-checking promptly on a [`Notify`] nudge or a bounded poll), and **fires**
//! each due trigger. A fire is a pure scheduling-plane act: it appends a
//! [`cron_logs`](crate::db::CronLogRow) ledger line, materializes (isolated) or
//! reuses (shared) the [`threads`](crate::db::ThreadRow) row that the fire drives,
//! and advances the cron. Execution itself is **not** here — the executor (Lane D)
//! drains `threads WHERE status='pending'`; the scheduler never starts an engine.
//!
//! ## Trigger model (`crons`, agent-architecture §4)
//!
//! * **`kind`** ∈ `schedule | webhook | api`; only `schedule` is wired. Non-schedule
//!   triggers carry no `next_run`, so the timing loop never selects them.
//! * **`schedule_type`** ∈ `cron | interval | once`. A `once` fires once, then
//!   `crons.status='completed'`. All timing rides `crons.next_run` → one loop.
//! * **`continuity`** ∈ `isolated` (each fire → a NEW thread, deterministic
//!   `thread_id = "<cron_id>@<scheduled_epoch>"`) | `shared` (one ongoing thread,
//!   `thread_id = "<cron_id>@shared"`, reused across turns — reusing its
//!   `engine_session_id`).
//!
//! ## Durable dedup (replaces the in-memory `HashSet`)
//!
//! Firing idempotency is the schema-enforced `cron_logs UNIQUE(cron_id,
//! scheduled_at)`. Each fire is logged through [`Db::try_insert_cron_log`]
//! (`INSERT OR IGNORE`): a fresh slot returns `Some(id)` and dispatches; a replay
//! after a crash/restart returns `None` and is skipped — no double-fire, no
//! double-thread. The old process-local running-set is gone.
//!
//! ## Ledger & catch-up (agent-architecture §7)
//!
//! `cron_logs` records **every** fire, including ones that produce no thread
//! (`skipped`/`missed`). Round-count truth is `cron_logs MAX(round_seq)` (D4);
//! `crons.run_count` is a cache reconciled from it at startup. Catch-up: `interval`
//! collapses missed periods (anchored advance past now), `cron` recomputes from
//! wall-clock, `once` dispatches if within a grace window else logs `missed`.
//!
//! ## v1 strangler removal (PLAN-006 §4)
//!
//! The v2 path is the only live one: [`Scheduler::new`] takes `(db, cancel)`, fires
//! triggers into `cron_logs` + pending `threads`, and signals
//! [`Scheduler::drain_signal`] so the runner's executor drains them. The v1
//! execution shims (`TaskDispatcher`, `output_of`, `mode_of`, the v1 task-upsert
//! handlers) and the host-mirror task-snapshot merge (`reconcile_snapshot`) are
//! gone, as is the v1 `compute_next_run`/`validate_schedule` pair that only the
//! cancelled in-runner tool server drove (PLAN-007). Consumers:
//!
//! * `runner.rs` → [`Cancel`], [`Scheduler`], [`apply_cron_upsert`] /
//!   [`apply_cron_delete`] (v2 cron routes).
//! * The v2 cron path → [`next_run_at_creation`] (seeds a new trigger's `next_run`),
//!   [`now`], [`to_iso`], and the private [`next_run_after_fire`] core (catch-up
//!   recompute on each fire).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use croner::Cron;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::db::{CronLogRow, CronRow, Db, DbError, ThreadRow};
use crate::protocol::v2::{CronStateData, CronUpsertData};

/// Upper bound on how long the loop sleeps before re-scanning the DB even with no
/// nudge — the bounded re-scan safety net for a trigger change that lands without
/// (or races) an in-memory nudge.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Fallback `next_run` for an interval whose value cannot be parsed, matching the
/// host (`cron.ts`: `now + 60_000`).
const INVALID_INTERVAL_FALLBACK_MS: i64 = 60_000;

/// Engine a cron-spawned thread defaults to. The only engine today; Lane G/D may
/// source this from the task/config when more engines land.
const DEFAULT_ENGINE: &str = "claude";

/// How late a `once` trigger may fire and still dispatch. Past this, the fire is
/// logged `missed` rather than run stale (an offline catch-up we don't replay).
/// Lane G may promote this to [`SchedulerConfig`].
const ONCE_CATCHUP_GRACE_SECS: i64 = 300;

/// `cron_logs.skip_reason` when a shared cron fires while its thread is still in
/// flight (a turn pending/dispatched/running) — appending a turn would overlap.
const SKIP_SHARED_BUSY: &str = "shared_thread_busy";

/// `cron_logs.skip_reason` for a `once` whose slot lapsed during downtime.
const MISSED_ONCE_STALE: &str = "once_catchup_window_exceeded";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the scheduler's pure next-run helpers.
#[derive(Debug, Error)]
pub(crate) enum SchedulerError {
    /// A cron expression could not be parsed by [`croner`].
    #[error("invalid cron expression {expr:?}: {source}")]
    Cron {
        /// The offending expression.
        expr: String,
        /// The underlying parser error.
        source: croner::errors::CronError,
    },
    /// An interval value was not a positive integer number of milliseconds.
    #[error("invalid interval value {0:?} (expected positive milliseconds)")]
    Interval(String),
    /// A `once` timestamp was not a valid RFC3339 datetime.
    #[error("invalid timestamp {0:?} (expected RFC3339)")]
    Timestamp(String),
    /// An unknown `schedule_type` was supplied.
    #[error("unknown schedule_type {0:?} (expected cron|interval|once)")]
    UnknownType(String),
}

// ---------------------------------------------------------------------------
// Time helpers (pure)
// ---------------------------------------------------------------------------

/// The current instant as a UTC timestamp. Centralised so tests can reason about a
/// fixed `now` by passing it explicitly to the pure helpers below.
pub(crate) fn now() -> DateTime<Utc> {
    Utc::now()
}

/// Render a UTC timestamp the way the host does (`Date.toISOString()`):
/// millisecond precision, `Z` suffix — so the host mirror lines up byte-for-byte.
pub(crate) fn to_iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Parse an RFC3339 timestamp (accepts both `…Z` and `…+00:00`, with or without
/// fractional seconds) into UTC, or `None` if it is malformed.
pub(crate) fn parse_iso(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

// ---------------------------------------------------------------------------
// Next-run computation (pure; port of host cron.ts / operations.ts)
// ---------------------------------------------------------------------------

/// The initial `next_run` for a freshly created trigger (port of `operations.ts`):
/// `cron` → next occurrence after `now`; `interval` → `now + ms`; `once` → its
/// absolute timestamp.
pub(crate) fn next_run_at_creation(
    schedule_type: &str,
    schedule_value: &str,
    now: DateTime<Utc>,
) -> Result<Option<String>, SchedulerError> {
    match schedule_type {
        "cron" => Ok(Some(to_iso(next_cron_after(schedule_value, now)?))),
        "interval" => {
            let ms = parse_interval_ms(schedule_value)?;
            Ok(Some(to_iso(now + TimeDelta::milliseconds(ms))))
        }
        "once" => {
            let at = parse_iso(schedule_value)
                .ok_or_else(|| SchedulerError::Timestamp(schedule_value.to_owned()))?;
            Ok(Some(to_iso(at)))
        }
        other => Err(SchedulerError::UnknownType(other.to_owned())),
    }
}

/// Shared next-run core for the v2 cron path ([`Scheduler::advance_cron`]):
/// `once` → `None`; `cron` → next
/// occurrence after `now`; `interval` → anchored to the current `next_run` (else
/// `now`), advanced past `now`. A malformed interval falls back to `now + 60s`
/// (host parity) so a bad row never wedges the loop; a malformed cron pauses
/// recurrence (`None`).
fn next_run_after_fire(
    id: &str,
    schedule_type: &str,
    schedule_value: &str,
    anchor: Option<&str>,
    now: DateTime<Utc>,
) -> Option<String> {
    match schedule_type {
        "once" => None,
        "cron" => match next_cron_after(schedule_value, now) {
            Ok(next) => Some(to_iso(next)),
            Err(err) => {
                warn!(trigger = %id, error = %err, "cron recompute failed; pausing recurrence");
                None
            }
        },
        "interval" => {
            let ms = parse_interval_ms(schedule_value).unwrap_or_else(|_| {
                warn!(trigger = %id, value = %schedule_value, "invalid interval; using 60s fallback");
                INVALID_INTERVAL_FALLBACK_MS
            });
            let anchor = anchor.and_then(parse_iso).unwrap_or(now);
            let step = TimeDelta::milliseconds(ms);
            let mut next = anchor + step;
            while next <= now {
                next += step;
            }
            Some(to_iso(next))
        }
        other => {
            warn!(trigger = %id, schedule_type = other, "unknown schedule_type; not rescheduling");
            None
        }
    }
}

/// Parse a 5-field cron expression with [`croner`]. The default (no
/// `with_seconds_*`) is exactly 5 fields, mapping the host's `0 <expr>` cronbake
/// form (seconds pinned to `0`) to the same instants.
fn parse_cron(expr: &str) -> Result<Cron, SchedulerError> {
    Cron::new(expr.trim())
        .parse()
        .map_err(|source| SchedulerError::Cron {
            expr: expr.to_owned(),
            source,
        })
}

/// The next cron occurrence strictly after `now`.
fn next_cron_after(expr: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>, SchedulerError> {
    parse_cron(expr)?
        .find_next_occurrence(&now, false)
        .map_err(|source| SchedulerError::Cron {
            expr: expr.to_owned(),
            source,
        })
}

/// Parse an interval value as a strictly-positive count of milliseconds.
fn parse_interval_ms(value: &str) -> Result<i64, SchedulerError> {
    value
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|&ms| ms > 0)
        .ok_or_else(|| SchedulerError::Interval(value.to_owned()))
}

// ---------------------------------------------------------------------------
// Inbound trigger definition handling (v2 `crons`; runner.rs route)
// ---------------------------------------------------------------------------

/// Apply a host `cron_upsert` to the local `crons` table: insert when new, update
/// in place when known. A missing `next_run` for a `schedule` trigger is seeded
/// from `schedule_type`/`schedule_value`. A new row starts `run_count = 0`; an
/// update preserves the existing `run_count` cache, `created_at`, and
/// `last_fired_at`. Returns `true` when a row was written (the caller nudges the
/// scheduler so the new timing is picked up promptly).
pub(crate) fn apply_cron_upsert(
    db: &Mutex<Db>,
    task_id: &str,
    cron_id: &str,
    data: &CronUpsertData,
) -> Result<bool, DbError> {
    let guard = lock_db(db);
    let existing = guard.get_cron(cron_id)?;
    let now_iso = to_iso(now());

    // Seed next_run for a schedule trigger when the host did not supply one.
    let next_run = data.next_run.clone().or_else(|| {
        match (
            data.schedule_type.as_deref(),
            data.schedule_value.as_deref(),
        ) {
            (Some(ty), Some(val)) => next_run_at_creation(ty, val, now()).ok().flatten(),
            _ => None,
        }
    });

    let row = CronRow {
        id: cron_id.to_owned(),
        task_id: task_id.to_owned(),
        kind: data.kind.clone(),
        // The host-supplied recurring instruction → crons.prompt (D10:(a)).
        prompt: data.prompt.clone(),
        schedule_type: data.schedule_type.clone(),
        schedule_value: data.schedule_value.clone(),
        next_run,
        last_fired_at: existing.as_ref().and_then(|c| c.last_fired_at.clone()),
        continuity: data.continuity.clone(),
        run_count: existing.as_ref().map_or(0, |c| c.run_count),
        status: data.status.clone().unwrap_or_else(|| "active".to_owned()),
        dirty: true,
        created_at: existing
            .as_ref()
            .map_or_else(|| now_iso.clone(), |c| c.created_at.clone()),
        updated_at: now_iso,
    };
    if existing.is_some() {
        guard.update_cron(&row)?;
    } else {
        guard.insert_cron(&row)?;
    }
    Ok(true)
}

/// Apply a host `cron_delete`: there is no hard delete, so the trigger is marked
/// `cancelled` and its `next_run` cleared so the timing loop stops selecting it.
/// Returns `true` when a row was found and changed.
pub(crate) fn apply_cron_delete(db: &Mutex<Db>, cron_id: &str) -> Result<bool, DbError> {
    let guard = lock_db(db);
    let Some(mut cron) = guard.get_cron(cron_id)? else {
        return Ok(false);
    };
    "cancelled".clone_into(&mut cron.status);
    cron.next_run = None;
    cron.dirty = true;
    cron.updated_at = to_iso(now());
    guard.update_cron(&cron)?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

/// A one-shot broadcast cancellation flag (same race-free pattern as the
/// transport's): set the flag, wake every waiter; check the flag, register
/// interest, recheck.
#[derive(Debug)]
pub(crate) struct Cancel {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Cancel {
    /// A fresh, un-cancelled token.
    pub(crate) fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Trip the flag and wake every current waiter. Idempotent.
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether [`Self::cancel`] has been called.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Resolve once cancelled (immediately if already).
    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let mut notified = std::pin::pin!(self.notify.notified());
        notified.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

impl Default for Cancel {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Fire decision (pure)
// ---------------------------------------------------------------------------

/// What evaluating one due trigger fire resolves to — computed without I/O so it
/// can be unit-tested in isolation.
#[derive(Clone, Debug, PartialEq)]
enum FireAction {
    /// Drive `thread_id`: a brand-new deterministic thread (`isolated`) or the
    /// cron's ongoing shared thread (`!isolated`).
    Dispatch {
        /// The thread this fire drives.
        thread_id: String,
        /// `true` for `isolated` continuity (mint a new thread per fire).
        isolated: bool,
    },
    /// No thread; record a `skipped` ledger line with this reason.
    Skip {
        /// Why the fire produced no thread.
        reason: &'static str,
    },
    /// No thread; the slot lapsed during downtime (`missed`).
    Missed,
}

/// The deterministic thread id an *isolated* fire mints (agent-architecture §4):
/// `"<cron_id>@<scheduled_epoch_seconds>"` — stable across replays so a re-fire of
/// the same slot maps to the same row.
fn isolated_thread_id(cron_id: &str, scheduled_at: DateTime<Utc>) -> String {
    format!("{cron_id}@{}", scheduled_at.timestamp())
}

/// The stable thread id a *shared* cron reuses across its turns.
fn shared_thread_id(cron_id: &str) -> String {
    format!("{cron_id}@shared")
}

/// Classify a due fire. `shared_thread_status` is the current status of the cron's
/// shared thread (`None` for isolated continuity or when no shared thread exists
/// yet). Pure — the caller performs the I/O the decision implies.
fn decide_fire(
    cron: &CronRow,
    scheduled_at: DateTime<Utc>,
    now: DateTime<Utc>,
    shared_thread_status: Option<&str>,
) -> FireAction {
    // A `once` whose slot lapsed beyond the grace window is not run stale.
    if cron.schedule_type.as_deref() == Some("once")
        && now.signed_duration_since(scheduled_at) > TimeDelta::seconds(ONCE_CATCHUP_GRACE_SECS)
    {
        return FireAction::Missed;
    }

    if cron.continuity == "shared" {
        // A turn already in flight on the shared thread → appending now would
        // overlap; skip. `active` (idle between turns) and "no thread yet" dispatch.
        if matches!(
            shared_thread_status,
            Some("pending" | "dispatched" | "running")
        ) {
            return FireAction::Skip {
                reason: SKIP_SHARED_BUSY,
            };
        }
        FireAction::Dispatch {
            thread_id: shared_thread_id(&cron.id),
            isolated: false,
        }
    } else {
        FireAction::Dispatch {
            thread_id: isolated_thread_id(&cron.id, scheduled_at),
            isolated: true,
        }
    }
}

/// Process-local per-cron tally feeding the `cron_state` summary (D2). `round_count`
/// is the durable truth (`cron_logs MAX(round_seq)`); `skipped`/`last_status` are
/// process-local and reset on restart.
#[derive(Clone, Debug, Default)]
pub(crate) struct CronTally {
    /// Skips + misses observed by this process since start.
    skipped: i64,
    /// The most recent fire's scheduling status.
    last_status: Option<String>,
}

/// Build the local scheduling-ledger summary frame payload for a cron. `pub(crate)`
/// so the `gino-agent cron` CLI can enqueue a `cron_state` for AI-managed crons.
pub(crate) fn cron_state_data(
    cron: &CronRow,
    round_count: i64,
    tally: &CronTally,
) -> CronStateData {
    CronStateData {
        kind: cron.kind.clone(),
        schedule_type: cron.schedule_type.clone(),
        schedule_value: cron.schedule_value.clone(),
        continuity: Some(cron.continuity.clone()),
        next_run: cron.next_run.clone(),
        status: Some(cron.status.clone()),
        round_count,
        skipped_count: tally.skipped,
        last_status: tally.last_status.clone(),
    }
}

/// A fresh `pending` thread row for a cron fire (agent-architecture §5),
/// snapshotting the cron's `prompt` into the thread at attempt 0 (D10). The
/// retry budget comes from `config.executor.max_attempts` via the scheduler.
fn new_pending_thread(
    cron: &CronRow,
    thread_id: &str,
    now: DateTime<Utc>,
    max_attempts: i64,
) -> ThreadRow {
    ThreadRow {
        id: thread_id.to_owned(),
        task_id: cron.task_id.clone(),
        cron_id: Some(cron.id.clone()),
        source: "cron".to_owned(),
        // Snapshot the cron's recurring instruction at attempt 0 (D10). A shared
        // thread snapshots on its first fire and keeps it across later turns;
        // retries reuse it rather than re-resolving (agent-architecture §3/§8).
        prompt: cron.prompt.clone(),
        engine: DEFAULT_ENGINE.to_owned(),
        engine_session_id: None,
        status: "pending".to_owned(),
        failure_reason: None,
        attempt: 0,
        max_attempts,
        work_dir: None,
        started_at: None,
        finished_at: None,
        duration_ms: None,
        result: None,
        error: None,
        synced: false,
        created_at: to_iso(now),
    }
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// The local v2 scheduler. [`Scheduler::run`] consumes it and drives the loop;
/// clone [`Scheduler::nudge`] beforehand to poke it after an external trigger
/// change, and [`Scheduler::drain_signal`] to learn when a fire produced work the
/// executor should drain.
#[derive(Debug)]
pub(crate) struct Scheduler {
    db: Arc<Mutex<Db>>,
    cancel: Arc<Cancel>,
    /// Woken on any trigger-set change to re-scan promptly.
    nudge: Arc<Notify>,
    /// Notified after each fire so the runner's executor drains the freshly-pending
    /// thread and flushes the `cron_state` the fire enqueued.
    drain: Arc<Notify>,
    poll_interval: Duration,
    /// Per-thread retry budget stamped on cron-fired threads
    /// (`config.executor.max_attempts`).
    max_attempts: i64,
}

impl Scheduler {
    /// Build a scheduler over the shared DB. `cancel` stops the loop;
    /// `max_attempts` is the retry budget stamped on the threads its fires
    /// create (`config.executor.max_attempts`). The v2 scheduler is
    /// scheduling-plane only: it fires triggers into `cron_logs` + pending
    /// `threads` and signals [`Self::drain_signal`]; the runner's executor owns
    /// execution.
    pub(crate) fn new(db: Arc<Mutex<Db>>, cancel: Arc<Cancel>, max_attempts: i64) -> Self {
        Self {
            db,
            cancel,
            nudge: Arc::new(Notify::new()),
            drain: Arc::new(Notify::new()),
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_attempts,
        }
    }

    /// Override the maximum re-scan interval (tests use a short value).
    #[cfg(test)]
    pub(crate) fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// A handle that wakes the loop after an external trigger change. A change that
    /// lands without (or races) a nudge is caught by the bounded poll instead.
    pub(crate) fn nudge(&self) -> Arc<Notify> {
        Arc::clone(&self.nudge)
    }

    /// A handle notified after each fire — the runner's executor waits on it to
    /// drain the newly-pending thread (and flush the fire's `cron_state`) promptly.
    pub(crate) fn drain_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.drain)
    }

    /// Drive the scheduler until cancelled. Reconciles the `run_count` cache from
    /// the ledger at startup (D4), then each pass computes the earliest due time
    /// and sleeps until due / nudged / cancelled.
    pub(crate) async fn run(self) {
        info!("scheduler started (v2 crons/cron_logs)");
        if let Err(err) = self.reconcile_run_counts() {
            warn!(error = %err, "startup run_count reconcile failed");
        }
        // Process-local skip/last-status tally per cron, feeding `cron_state`.
        let mut tally: HashMap<String, CronTally> = HashMap::new();
        loop {
            // Register nudge interest before reading the DB so a change that lands
            // mid-scan still wakes us.
            let notified = self.nudge.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let wait = self.next_wait();
            tokio::select! {
                () = self.cancel.cancelled() => break,
                () = &mut notified => {}
                () = tokio::time::sleep(wait) => self.tick(now(), &mut tally),
            }
        }
        info!("scheduler stopped");
    }

    /// Reconcile each active cron's `run_count` cache to the ledger truth
    /// (`cron_logs MAX(round_seq)`, D4). A pure cache correction — it preserves the
    /// cron's `dirty`/`updated_at` so it does not provoke a spurious sync.
    fn reconcile_run_counts(&self) -> Result<(), DbError> {
        let crons = lock_db(&self.db).list_active_crons()?;
        for cron in crons {
            let truth = lock_db(&self.db).max_round_seq(&cron.id)?.unwrap_or(0);
            if cron.run_count != truth {
                let mut fixed = cron.clone();
                fixed.run_count = truth;
                lock_db(&self.db).update_cron(&fixed)?;
                debug!(cron = %cron.id, from = cron.run_count, to = truth, "reconciled run_count");
            }
        }
        Ok(())
    }

    /// How long to sleep before the next scan: until the earliest due cron, capped
    /// at [`Self::poll_interval`] and floored at zero (fire-now when overdue).
    fn next_wait(&self) -> Duration {
        let crons = match lock_db(&self.db).list_active_crons() {
            Ok(crons) => crons,
            Err(err) => {
                warn!(error = %err, "scheduler failed to load active crons");
                return self.poll_interval;
            }
        };
        let now = now();
        let mut earliest: Option<DateTime<Utc>> = None;
        for cron in &crons {
            if let Some(next) = cron.next_run.as_deref().and_then(parse_iso) {
                earliest = Some(earliest.map_or(next, |cur| cur.min(next)));
            }
        }
        match earliest {
            Some(next) => next
                .signed_duration_since(now)
                .to_std()
                .unwrap_or(Duration::ZERO)
                .min(self.poll_interval),
            None => self.poll_interval,
        }
    }

    /// Fire every active cron whose `next_run` is due (`<= now`).
    fn tick(&self, now: DateTime<Utc>, tally: &mut HashMap<String, CronTally>) {
        let crons = match lock_db(&self.db).list_active_crons() {
            Ok(crons) => crons,
            Err(err) => {
                warn!(error = %err, "scheduler tick failed to load crons");
                return;
            }
        };
        for cron in crons {
            let Some(next) = cron.next_run.as_deref().and_then(parse_iso) else {
                continue;
            };
            if next > now {
                continue;
            }
            self.fire_cron(&cron, now, tally);
        }
    }

    /// Fire one due cron: dedup via the ledger `UNIQUE(cron_id, scheduled_at)`,
    /// materialize/reuse the driven thread, advance the cron, and emit `cron_state`.
    fn fire_cron(
        &self,
        cron: &CronRow,
        now: DateTime<Utc>,
        tally: &mut HashMap<String, CronTally>,
    ) {
        let Some(scheduled_iso) = cron.next_run.clone() else {
            return;
        };
        let Some(scheduled_at) = parse_iso(&scheduled_iso) else {
            warn!(cron = %cron.id, value = %scheduled_iso, "unparseable next_run; advancing without firing");
            self.advance_cron(cron, now);
            return;
        };

        // Round-count truth = cron_logs MAX(round_seq); this fire is the next round.
        let round_seq = lock_db(&self.db)
            .max_round_seq(&cron.id)
            .ok()
            .flatten()
            .unwrap_or(0)
            + 1;

        // Shared continuity needs the open thread's state to decide overlap.
        let shared_status = (cron.continuity == "shared")
            .then(|| {
                lock_db(&self.db)
                    .get_thread(&shared_thread_id(&cron.id))
                    .ok()
                    .flatten()
                    .map(|t| t.status)
            })
            .flatten();
        let action = decide_fire(cron, scheduled_at, now, shared_status.as_deref());

        let (status, skip_reason, thread_id) = match &action {
            FireAction::Dispatch { thread_id, .. } => ("dispatched", None, Some(thread_id.clone())),
            FireAction::Skip { reason } => ("skipped", Some(*reason), None),
            FireAction::Missed => ("missed", Some(MISSED_ONCE_STALE), None),
        };

        // Materialize the driven thread first so the ledger's `thread_id` FK target
        // exists. The thread id is deterministic (isolated) / stable (shared), so
        // this is idempotent: a replay re-ensures the same row, never a duplicate.
        if let FireAction::Dispatch { thread_id, .. } = &action {
            self.ensure_thread(cron, thread_id, now);
        }

        // Durable dedup: a replay of the same (cron_id, scheduled_at) is IGNORE'd.
        let log = CronLogRow {
            id: 0,
            cron_id: cron.id.clone(),
            task_id: cron.task_id.clone(),
            round_seq,
            scheduled_at: scheduled_iso.clone(),
            fired_at: Some(to_iso(now)),
            status: status.to_owned(),
            skip_reason: skip_reason.map(str::to_owned),
            thread_id: thread_id.clone(),
            created_at: String::new(),
        };
        let inserted = match lock_db(&self.db).try_insert_cron_log(&log) {
            Ok(v) => v,
            Err(err) => {
                warn!(cron = %cron.id, error = %err, "cron_log insert failed; advancing to avoid a hot loop");
                None
            }
        };
        if inserted.is_none() {
            debug!(cron = %cron.id, scheduled_at = %scheduled_iso, "duplicate fire ignored (already logged)");
            // A crash between logging and advancing leaves next_run on the old slot;
            // advance now so the loop moves on without re-dispatching.
            self.advance_cron(cron, now);
            return;
        }

        // A fresh (deduped) shared fire drives a NEW turn on the ongoing thread:
        // re-pend it, keeping the engine session (P0-1). After the dedup so a
        // replayed slot never re-runs a turn that already executed.
        if let FireAction::Dispatch {
            thread_id,
            isolated: false,
        } = &action
        {
            self.repend_shared_thread(thread_id);
        }

        // Tally + summary (process-local skip/last-status; round_count is the truth).
        let entry = tally.entry(cron.id.clone()).or_default();
        if status != "dispatched" {
            entry.skipped += 1;
        }
        entry.last_status = Some(status.to_owned());

        let updated = self.advance_cron(cron, now);
        self.emit_cron_state(updated.as_ref().unwrap_or(cron), round_seq, entry);

        // Tell the runner's executor to drain the freshly-pending thread and flush
        // the `cron_state` this fire enqueued. Cheap when the fire was a skip/missed
        // (the drain finds nothing new but still flushes the ledger summary).
        self.drain.notify_one();
    }

    /// Ensure the thread a fire drives exists: insert the deterministic
    /// (isolated) / stable (shared) row if absent. Idempotent on replay — an
    /// existing row is reused as-is; making a resting shared thread runnable
    /// again is [`Self::repend_shared_thread`]'s job, gated on the fire passing
    /// the ledger dedup so a replayed slot never re-runs.
    fn ensure_thread(&self, cron: &CronRow, thread_id: &str, now: DateTime<Utc>) {
        let guard = lock_db(&self.db);
        match guard.get_thread(thread_id) {
            Ok(Some(_)) => {
                debug!(thread = %thread_id, "thread already present; reusing");
            }
            Ok(None) => {
                let row = new_pending_thread(cron, thread_id, now, self.max_attempts);
                if let Err(err) = guard.insert_thread(&row) {
                    warn!(thread = %thread_id, error = %err, "failed to insert pending thread");
                }
            }
            Err(err) => {
                warn!(thread = %thread_id, error = %err, "thread lookup before fire failed");
            }
        }
    }

    /// Re-set a resting shared thread `pending` so a fresh fire actually runs a
    /// new turn (P0-1): the `engine_session_id` (continuity) and the prompt
    /// snapshot are KEPT, the per-turn fields (attempt counter, failure fields,
    /// timestamps, result) are cleared, and the retry budget is refreshed from
    /// config. An in-flight (`pending`/`dispatched`/`running`) thread is left
    /// as-is — [`decide_fire`] skips those fires, so that arm only guards a
    /// replay race. Called only after the fire passed the ledger dedup.
    fn repend_shared_thread(&self, thread_id: &str) {
        let guard = lock_db(&self.db);
        match guard.get_thread(thread_id) {
            Ok(Some(existing)) => {
                if matches!(
                    existing.status.as_str(),
                    "pending" | "dispatched" | "running"
                ) {
                    debug!(thread = %thread_id, status = %existing.status, "shared thread in flight; not re-pending");
                    return;
                }
                let mut turn = existing;
                "pending".clone_into(&mut turn.status);
                turn.attempt = 0;
                turn.max_attempts = self.max_attempts;
                turn.failure_reason = None;
                turn.started_at = None;
                turn.finished_at = None;
                turn.duration_ms = None;
                turn.result = None;
                turn.error = None;
                if let Err(err) = guard.update_thread(&turn) {
                    warn!(thread = %thread_id, error = %err, "failed to re-pend shared thread");
                }
            }
            Ok(None) => {
                warn!(thread = %thread_id, "shared thread missing at re-pend");
            }
            Err(err) => {
                warn!(thread = %thread_id, error = %err, "shared thread lookup at re-pend failed");
            }
        }
    }

    /// Advance a cron after a fire: `next_run` recomputed (catch-up aware),
    /// `last_fired_at` stamped, `run_count` cache set to the ledger truth (D4), and
    /// a `once` marked `completed`. Returns the updated row (for `cron_state`).
    fn advance_cron(&self, cron: &CronRow, now: DateTime<Utc>) -> Option<CronRow> {
        let is_once = cron.schedule_type.as_deref() == Some("once");
        let next_run = next_run_after_fire(
            &cron.id,
            cron.schedule_type.as_deref().unwrap_or_default(),
            cron.schedule_value.as_deref().unwrap_or_default(),
            cron.next_run.as_deref(),
            now,
        );
        let truth = lock_db(&self.db)
            .max_round_seq(&cron.id)
            .ok()
            .flatten()
            .unwrap_or(0);

        let mut updated = cron.clone();
        updated.next_run = next_run;
        updated.last_fired_at = Some(to_iso(now));
        updated.run_count = truth;
        if is_once {
            "completed".clone_into(&mut updated.status);
        }
        updated.dirty = true;
        updated.updated_at = to_iso(now);
        match lock_db(&self.db).update_cron(&updated) {
            Ok(()) => Some(updated),
            Err(err) => {
                warn!(cron = %cron.id, error = %err, "failed to advance cron");
                None
            }
        }
    }

    /// Enqueue a `cron_state` summary to the durable outbox (D2).
    ///
    /// The host correlates `cron_state` by the **envelope `cronId`**
    /// (agent-architecture §10). The `outbox` keeps a single secondary correlation
    /// column (`thread_id`); the transport's `build_envelope` keys it back to
    /// `cronId` for a `cron_state` row (Lane E), so the cron id rides that column.
    fn emit_cron_state(&self, cron: &CronRow, round_count: i64, tally: &CronTally) {
        let data = cron_state_data(cron, round_count, tally);
        if let Err(err) = lock_db(&self.db).enqueue_outbox_correlated(
            "cron_state",
            &data,
            Some(&cron.task_id),
            Some(&cron.id),
        ) {
            warn!(cron = %cron.id, error = %err, "failed to enqueue cron_state");
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Lock the shared DB, recovering from a poisoned mutex rather than panicking
/// (Lock 6). Critical sections are short and never held across an `.await`.
fn lock_db(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use chrono::{DateTime, TimeDelta, Utc};

    use async_trait::async_trait;
    use futures_util::stream::{self, BoxStream};

    use super::{
        Cancel, CronTally, FireAction, SKIP_SHARED_BUSY, Scheduler, apply_cron_delete,
        apply_cron_upsert, cron_state_data, decide_fire, isolated_thread_id, lock_db,
        next_run_at_creation, parse_iso, shared_thread_id,
    };
    use crate::config::ExecutorConfig;
    use crate::db::{CronLogRow, CronRow, Db, TaskRow, ThreadRow};
    use crate::engine::{
        Engine, EngineError, EngineEvent, Input, Payload, RunOutcome, RunStatus, Session,
        SessionCfg,
    };
    use crate::executor::v2::{PromptSource, ThreadExecutor, WatchdogCfg};
    use crate::protocol::v2::CronUpsertData;

    fn at(value: &str) -> DateTime<Utc> {
        parse_iso(value).expect("valid timestamp")
    }

    // ===== next-run-at-creation parity (port of host operations.ts) =====

    #[test]
    fn next_run_at_creation_matches_host_for_each_schedule_type() {
        // cron → next occurrence strictly after now (host getNext parity).
        for (expr, now, expected) in [
            (
                "0 9 * * *",
                "2026-06-19T10:00:00Z",
                "2026-06-20T09:00:00.000Z",
            ),
            (
                "*/15 * * * *",
                "2026-06-19T10:07:00Z",
                "2026-06-19T10:15:00.000Z",
            ),
            (
                "0 0 * * *",
                "2026-06-19T10:00:00Z",
                "2026-06-20T00:00:00.000Z",
            ),
        ] {
            assert_eq!(
                next_run_at_creation("cron", expr, at(now))
                    .unwrap()
                    .as_deref(),
                Some(expected),
                "cron next_run_at_creation {expr} @ {now}"
            );
        }

        // interval → now + ms; once → its absolute timestamp.
        assert_eq!(
            next_run_at_creation("interval", "60000", at("2026-06-19T10:00:00Z"))
                .unwrap()
                .as_deref(),
            Some("2026-06-19T10:01:00.000Z")
        );
        assert_eq!(
            next_run_at_creation("once", "2026-12-25T00:00:00Z", at("2026-06-19T10:00:00Z"))
                .unwrap()
                .as_deref(),
            Some("2026-12-25T00:00:00.000Z")
        );

        // Malformed inputs and an unknown type are rejected.
        assert!(next_run_at_creation("cron", "not a cron", at("2026-06-19T10:00:00Z")).is_err());
        assert!(next_run_at_creation("interval", "0", at("2026-06-19T10:00:00Z")).is_err());
        assert!(next_run_at_creation("once", "nope", at("2026-06-19T10:00:00Z")).is_err());
        assert!(next_run_at_creation("weird", "x", at("2026-06-19T10:00:00Z")).is_err());
    }

    // ===== v2 scheduling plane (crons / threads / cron_logs) =====

    fn cron(
        id: &str,
        schedule_type: &str,
        schedule_value: &str,
        continuity: &str,
        next_run: Option<&str>,
    ) -> CronRow {
        CronRow {
            id: id.to_owned(),
            task_id: "task-1".to_owned(),
            kind: "schedule".to_owned(),
            prompt: "run the recurring job".to_owned(),
            schedule_type: Some(schedule_type.to_owned()),
            schedule_value: Some(schedule_value.to_owned()),
            next_run: next_run.map(str::to_owned),
            last_fired_at: None,
            continuity: continuity.to_owned(),
            run_count: 0,
            status: "active".to_owned(),
            dirty: false,
            created_at: "2026-06-22T09:00:00.000Z".to_owned(),
            updated_at: "2026-06-22T09:00:00.000Z".to_owned(),
        }
    }

    fn seed_task(db: &Mutex<Db>) {
        lock_db(db)
            .insert_task_row(&TaskRow {
                id: "task-1".to_owned(),
                name: None,
                kind: None,
                status: "active".to_owned(),
                created_at: "2026-06-22T09:00:00.000Z".to_owned(),
                updated_at: "2026-06-22T09:00:00.000Z".to_owned(),
            })
            .unwrap();
    }

    fn make_scheduler(db: Arc<Mutex<Db>>) -> Scheduler {
        // The retry budget cron threads are stamped with comes from
        // `config.executor.max_attempts`; tests use the shipped default.
        Scheduler::new(
            db,
            Arc::new(Cancel::new()),
            ExecutorConfig::default().max_attempts,
        )
    }

    fn cron_upsert_data(
        kind: &str,
        schedule_type: Option<&str>,
        schedule_value: Option<&str>,
        continuity: &str,
    ) -> CronUpsertData {
        CronUpsertData {
            kind: kind.to_owned(),
            prompt: "run the recurring job".to_owned(),
            schedule_type: schedule_type.map(str::to_owned),
            schedule_value: schedule_value.map(str::to_owned),
            continuity: continuity.to_owned(),
            next_run: None,
            status: None,
        }
    }

    #[test]
    fn cron_upsert_inserts_then_updates_and_seeds_next_run() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        seed_task(&db);
        let data = cron_upsert_data("schedule", Some("interval"), Some("60000"), "isolated");
        assert!(apply_cron_upsert(&db, "task-1", "cr-1", &data).unwrap());
        let inserted = lock_db(&db).get_cron("cr-1").unwrap().unwrap();
        assert_eq!(inserted.task_id, "task-1");
        assert_eq!(inserted.continuity, "isolated");
        assert!(
            inserted.next_run.is_some(),
            "schedule trigger seeds next_run"
        );
        assert_eq!(inserted.run_count, 0);

        // An update preserves run_count/created_at and applies the new fields.
        let mut bumped = lock_db(&db).get_cron("cr-1").unwrap().unwrap();
        bumped.run_count = 4;
        lock_db(&db).update_cron(&bumped).unwrap();
        let edited = cron_upsert_data("schedule", Some("interval"), Some("60000"), "shared");
        assert!(apply_cron_upsert(&db, "task-1", "cr-1", &edited).unwrap());
        let reloaded = lock_db(&db).get_cron("cr-1").unwrap().unwrap();
        assert_eq!(reloaded.continuity, "shared");
        assert_eq!(
            reloaded.run_count, 4,
            "run_count cache preserved across upsert"
        );
        assert_eq!(reloaded.created_at, inserted.created_at);
    }

    #[test]
    fn cron_upsert_stores_prompt_then_fire_snapshots_it_into_the_thread() {
        // The full D10 cron path: cron_upsert.data.prompt → crons.prompt, then a
        // fire snapshots crons.prompt into the driven thread's threads.prompt.
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        let mut data = cron_upsert_data("schedule", Some("interval"), Some("60000"), "isolated");
        "summarize the overnight alerts".clone_into(&mut data.prompt);
        data.next_run = Some(scheduled.to_owned());
        assert!(apply_cron_upsert(&db, "task-1", "cr-1", &data).unwrap());
        assert_eq!(
            lock_db(&db).get_cron("cr-1").unwrap().unwrap().prompt,
            "summarize the overnight alerts",
            "cron_upsert.data.prompt is stored into crons.prompt"
        );

        let sched = make_scheduler(Arc::clone(&db));
        sched.tick(at("2026-06-22T12:00:05Z"), &mut HashMap::new());

        let tid = isolated_thread_id("cr-1", at(scheduled));
        let thread = lock_db(&db)
            .get_thread(&tid)
            .unwrap()
            .expect("thread created");
        assert_eq!(
            thread.prompt, "summarize the overnight alerts",
            "the fire snapshots crons.prompt into threads.prompt"
        );
    }

    #[test]
    fn cron_delete_marks_cancelled_and_clears_next_run() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        seed_task(&db);
        let data = cron_upsert_data("schedule", Some("interval"), Some("60000"), "isolated");
        apply_cron_upsert(&db, "task-1", "cr-1", &data).unwrap();
        assert!(apply_cron_delete(&db, "cr-1").unwrap());
        let reloaded = lock_db(&db).get_cron("cr-1").unwrap().unwrap();
        assert_eq!(reloaded.status, "cancelled");
        assert!(reloaded.next_run.is_none());
        assert!(!apply_cron_delete(&db, "ghost").unwrap());
    }

    #[test]
    fn poll_interval_override_is_applied() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let scheduler = make_scheduler(db).with_poll_interval(Duration::from_millis(25));
        assert_eq!(scheduler.poll_interval, Duration::from_millis(25));
    }

    #[test]
    fn decide_fire_classifies_each_outcome() {
        let now = at("2026-06-22T12:00:00Z");
        let sched_at = at("2026-06-22T11:59:59Z");

        let iso = cron("c", "interval", "60000", "isolated", None);
        assert!(matches!(
            decide_fire(&iso, sched_at, now, None),
            FireAction::Dispatch { isolated: true, .. }
        ));

        let sh = cron("c", "interval", "60000", "shared", None);
        assert_eq!(
            decide_fire(&sh, sched_at, now, Some("running")),
            FireAction::Skip {
                reason: SKIP_SHARED_BUSY
            }
        );
        assert!(matches!(
            decide_fire(&sh, sched_at, now, Some("active")),
            FireAction::Dispatch {
                isolated: false,
                ..
            }
        ));
        assert!(matches!(
            decide_fire(&sh, sched_at, now, None),
            FireAction::Dispatch {
                isolated: false,
                ..
            }
        ));

        let once = cron("c", "once", "2026-06-22T11:00:00Z", "isolated", None);
        assert_eq!(
            decide_fire(&once, at("2026-06-22T11:00:00Z"), now, None),
            FireAction::Missed,
            "an hour overdue exceeds the grace window"
        );
        assert!(
            matches!(
                decide_fire(&once, at("2026-06-22T11:59:30Z"), now, None),
                FireAction::Dispatch { .. }
            ),
            "within grace dispatches"
        );
    }

    #[test]
    fn isolated_fire_creates_deterministic_thread_and_logs_dispatched() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron(
                "c-1",
                "interval",
                "60000",
                "isolated",
                Some(scheduled),
            ))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        let now = at("2026-06-22T12:00:05Z");
        sched.tick(now, &mut HashMap::new());

        let tid = format!("c-1@{}", at(scheduled).timestamp());
        assert_eq!(tid, isolated_thread_id("c-1", at(scheduled)));
        let thread = lock_db(&db)
            .get_thread(&tid)
            .unwrap()
            .expect("thread created");
        assert_eq!(thread.status, "pending");
        assert_eq!(thread.cron_id.as_deref(), Some("c-1"));
        assert_eq!(thread.source, "cron");
        // The fire snapshots crons.prompt into the thread at attempt 0 (D10).
        assert_eq!(thread.prompt, "run the recurring job");
        assert_eq!(thread.attempt, 0);

        assert_eq!(lock_db(&db).max_round_seq("c-1").unwrap(), Some(1));
        let log = lock_db(&db).get_cron_log(1).unwrap().unwrap();
        assert_eq!(log.status, "dispatched");
        assert_eq!(log.thread_id.as_deref(), Some(tid.as_str()));

        let advanced = lock_db(&db).get_cron("c-1").unwrap().unwrap();
        assert_eq!(advanced.run_count, 1);
        assert!(parse_iso(advanced.next_run.as_deref().unwrap()).unwrap() > now);
        assert!(advanced.last_fired_at.is_some());
    }

    #[test]
    fn durable_dedup_prevents_double_fire_on_replay() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron(
                "c-1",
                "interval",
                "60000",
                "isolated",
                Some(scheduled),
            ))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        let now = at("2026-06-22T12:00:05Z");
        sched.tick(now, &mut HashMap::new());
        assert_eq!(lock_db(&db).max_round_seq("c-1").unwrap(), Some(1));

        // Simulate a crash before next_run advanced: rewind to the same slot.
        let mut rewound = lock_db(&db).get_cron("c-1").unwrap().unwrap();
        rewound.next_run = Some(scheduled.to_owned());
        lock_db(&db).update_cron(&rewound).unwrap();

        // Replay: same (cron_id, scheduled_at) → UNIQUE IGNORE → no second row.
        sched.tick(now, &mut HashMap::new());
        assert_eq!(
            lock_db(&db).max_round_seq("c-1").unwrap(),
            Some(1),
            "replay does not double-fire"
        );
        assert!(
            lock_db(&db).get_cron_log(2).unwrap().is_none(),
            "no second cron_log row"
        );
    }

    #[test]
    fn once_fires_within_grace_then_completes() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron(
                "c-once",
                "once",
                scheduled,
                "isolated",
                Some(scheduled),
            ))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        sched.tick(at("2026-06-22T12:00:10Z"), &mut HashMap::new());

        let row = lock_db(&db).get_cron("c-once").unwrap().unwrap();
        assert_eq!(row.status, "completed");
        assert!(row.next_run.is_none());
        let log = lock_db(&db).get_cron_log(1).unwrap().unwrap();
        assert_eq!(log.status, "dispatched");
        let tid = format!("c-once@{}", at(scheduled).timestamp());
        assert!(lock_db(&db).get_thread(&tid).unwrap().is_some());
    }

    #[test]
    fn once_overdue_is_logged_missed_with_no_thread() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron(
                "c-once",
                "once",
                scheduled,
                "isolated",
                Some(scheduled),
            ))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        // An hour late — well past the grace window.
        sched.tick(at("2026-06-22T13:00:00Z"), &mut HashMap::new());

        let log = lock_db(&db).get_cron_log(1).unwrap().unwrap();
        assert_eq!(log.status, "missed");
        assert!(log.thread_id.is_none());
        let row = lock_db(&db).get_cron("c-once").unwrap().unwrap();
        assert_eq!(row.status, "completed");
        let tid = format!("c-once@{}", at(scheduled).timestamp());
        assert!(
            lock_db(&db).get_thread(&tid).unwrap().is_none(),
            "missed fire creates no thread"
        );
    }

    #[test]
    fn shared_fire_skipped_when_thread_in_flight() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron(
                "c-sh",
                "interval",
                "60000",
                "shared",
                Some(scheduled),
            ))
            .unwrap();
        // A turn is already running on the shared thread.
        let tid = shared_thread_id("c-sh");
        lock_db(&db)
            .insert_thread(&ThreadRow {
                id: tid.clone(),
                task_id: "task-1".to_owned(),
                cron_id: Some("c-sh".to_owned()),
                source: "cron".to_owned(),
                prompt: "run the recurring job".to_owned(),
                engine: "claude".to_owned(),
                engine_session_id: None,
                status: "running".to_owned(),
                failure_reason: None,
                attempt: 0,
                max_attempts: 1,
                work_dir: None,
                started_at: None,
                finished_at: None,
                duration_ms: None,
                result: None,
                error: None,
                synced: false,
                created_at: "2026-06-22T11:59:00.000Z".to_owned(),
            })
            .unwrap();

        let sched = make_scheduler(Arc::clone(&db));
        let mut tally = HashMap::new();
        sched.tick(at("2026-06-22T12:00:05Z"), &mut tally);

        let log = lock_db(&db).get_cron_log(1).unwrap().unwrap();
        assert_eq!(log.status, "skipped");
        assert_eq!(log.skip_reason.as_deref(), Some(SKIP_SHARED_BUSY));
        assert!(log.thread_id.is_none());
        assert_eq!(tally.get("c-sh").unwrap().skipped, 1);

        // The in-flight turn is untouched: no re-pend, so no double execution.
        let thread = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(
            thread.status, "running",
            "busy shared thread is not re-pended"
        );
        assert_eq!(thread.attempt, 0);
    }

    #[test]
    fn shared_fire_reuses_one_thread_across_rounds() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let slot1 = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron("c-sh", "interval", "60000", "shared", Some(slot1)))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        let mut tally = HashMap::new();

        // Round 1 creates the shared thread (pending) and links it.
        sched.tick(at("2026-06-22T12:00:01Z"), &mut tally);
        let tid = shared_thread_id("c-sh");
        assert!(lock_db(&db).get_thread(&tid).unwrap().is_some());
        let log1 = lock_db(&db).get_cron_log(1).unwrap().unwrap();
        assert_eq!(log1.status, "dispatched");
        assert_eq!(log1.thread_id.as_deref(), Some(tid.as_str()));

        // Mark the turn finished (idle between turns) so the next fire can reuse it.
        let mut t = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        "active".clone_into(&mut t.status);
        lock_db(&db).update_thread(&t).unwrap();

        // Round 2 fires the advanced slot and reuses the SAME thread.
        let advanced = lock_db(&db).get_cron("c-sh").unwrap().unwrap();
        let slot2 = parse_iso(advanced.next_run.as_deref().unwrap()).unwrap();
        sched.tick(slot2 + TimeDelta::seconds(1), &mut tally);

        let log2 = lock_db(&db).get_cron_log(2).unwrap().unwrap();
        assert_eq!(log2.status, "dispatched");
        assert_eq!(
            log2.thread_id.as_deref(),
            Some(tid.as_str()),
            "shared thread reused across rounds"
        );
        assert_eq!(lock_db(&db).max_round_seq("c-sh").unwrap(), Some(2));
    }

    #[test]
    fn shared_fire_repends_resting_thread_keeping_session_clearing_turn_fields() {
        // P0-1: a later shared fire must make the ongoing thread runnable again
        // (`pending`), KEEPING engine_session_id (continuity) and the prompt
        // snapshot while clearing every per-turn field.
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let slot1 = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron("c-sh", "interval", "60000", "shared", Some(slot1)))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        let mut tally = HashMap::new();
        sched.tick(at("2026-06-22T12:00:01Z"), &mut tally);

        // Simulate the executor finishing turn 1: parked resting with a session
        // and stale per-turn residue (as a prior failed attempt would leave).
        let tid = shared_thread_id("c-sh");
        let mut t = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        "active".clone_into(&mut t.status);
        t.engine_session_id = Some("sess-1".to_owned());
        t.attempt = 2;
        t.failure_reason = Some("runner.timeout".to_owned());
        t.error = Some("stale error".to_owned());
        t.result = Some("turn 1 result".to_owned());
        t.started_at = Some("2026-06-22T12:00:01.000Z".to_owned());
        t.finished_at = Some("2026-06-22T12:00:02.000Z".to_owned());
        t.duration_ms = Some(1_000);
        lock_db(&db).update_thread(&t).unwrap();

        let advanced = lock_db(&db).get_cron("c-sh").unwrap().unwrap();
        let slot2 = parse_iso(advanced.next_run.as_deref().unwrap()).unwrap();
        sched.tick(slot2 + TimeDelta::seconds(1), &mut tally);

        let turn = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(turn.status, "pending", "second fire re-pends the thread");
        assert_eq!(
            turn.engine_session_id.as_deref(),
            Some("sess-1"),
            "engine session continuity is preserved"
        );
        assert_eq!(
            turn.prompt, "run the recurring job",
            "the prompt snapshot survives across turns"
        );
        assert_eq!(turn.attempt, 0, "attempt counter reset for the new turn");
        assert_eq!(
            turn.max_attempts,
            ExecutorConfig::default().max_attempts,
            "retry budget refreshed from config"
        );
        assert!(turn.failure_reason.is_none());
        assert!(turn.error.is_none());
        assert!(turn.result.is_none());
        assert!(turn.started_at.is_none());
        assert!(turn.finished_at.is_none());
        assert!(turn.duration_ms.is_none());
    }

    #[test]
    fn replayed_shared_slot_does_not_repend() {
        // A replay of an already-logged slot (crash between the cron_log insert
        // and the advance) is dedup'd by the ledger and must NOT re-pend the
        // shared thread — the turn for that slot already ran.
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let slot1 = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron("c-sh", "interval", "60000", "shared", Some(slot1)))
            .unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        let mut tally = HashMap::new();
        sched.tick(at("2026-06-22T12:00:01Z"), &mut tally);

        // Turn 1 executed and the thread is resting.
        let tid = shared_thread_id("c-sh");
        let mut t = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        "active".clone_into(&mut t.status);
        t.engine_session_id = Some("sess-1".to_owned());
        lock_db(&db).update_thread(&t).unwrap();

        // Rewind next_run to the already-logged slot and replay it.
        let mut rewound = lock_db(&db).get_cron("c-sh").unwrap().unwrap();
        rewound.next_run = Some(slot1.to_owned());
        lock_db(&db).update_cron(&rewound).unwrap();
        sched.tick(at("2026-06-22T12:00:02Z"), &mut tally);

        let thread = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(
            thread.status, "active",
            "replayed slot must not re-pend the shared thread"
        );
        assert_eq!(lock_db(&db).max_round_seq("c-sh").unwrap(), Some(1));
    }

    // ===== scheduler + executor integration (P0-1 / P1-5 regressions) =====

    /// Records the `resume` handle of every session the engine is asked to
    /// start, then replays a successful turn on `session_id`.
    struct RecordingEngine {
        session_id: &'static str,
        resumes: Mutex<Vec<Option<String>>>,
    }

    /// A scripted session (mirrors the executor tests' mock).
    struct ScriptSession(Vec<EngineEvent>);

    #[async_trait]
    impl Session for ScriptSession {
        async fn send(&self, _input: Input) -> Result<(), EngineError> {
            Ok(())
        }
        async fn interrupt(&self) -> Result<(), EngineError> {
            Ok(())
        }
        fn events(&self) -> BoxStream<'_, EngineEvent> {
            Box::pin(stream::iter(self.0.clone()))
        }
    }

    #[async_trait]
    impl Engine for RecordingEngine {
        fn name(&self) -> &'static str {
            "recording"
        }
        async fn start_session(&self, cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
            self.resumes.lock().unwrap().push(cfg.resume);
            Ok(Box::new(ScriptSession(vec![
                EngineEvent::SessionStarted {
                    session_id: self.session_id.to_owned(),
                },
                EngineEvent::Result(RunOutcome {
                    status: RunStatus::Success,
                    result: Some("turn done".to_owned()),
                    session_id: Some(self.session_id.to_owned()),
                    error: None,
                }),
            ])))
        }
        async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
            unreachable!("the executor drives sessions, not run_once")
        }
    }

    /// Cron threads carry their prompt snapshot, so the drainer never consults
    /// the prompt source in these scenarios.
    struct NoPrompt;
    impl PromptSource for NoPrompt {
        fn prompt_for(&self, _thread: &ThreadRow) -> Option<String> {
            None
        }
    }

    fn executor_over(db: &Arc<Mutex<Db>>, engine: Arc<dyn Engine>) -> ThreadExecutor {
        let dir = tempfile::tempdir().unwrap();
        ThreadExecutor::new(
            engine,
            Arc::clone(db),
            dir.path().to_path_buf(),
            WatchdogCfg {
                inactivity_timeout: None,
                ..WatchdogCfg::default()
            },
        )
    }

    /// The audit's named blind spot (P0-1): the SECOND fire of a
    /// shared-continuity cron must actually EXECUTE an engine run — resuming
    /// the same engine session — not merely write a `dispatched` ledger row.
    #[tokio::test]
    async fn shared_cron_second_fire_executes_a_second_engine_run() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let slot1 = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron("c-sh", "interval", "60000", "shared", Some(slot1)))
            .unwrap();
        let engine = Arc::new(RecordingEngine {
            session_id: "sess-shared",
            resumes: Mutex::new(Vec::new()),
        });
        let exec = executor_over(&db, Arc::clone(&engine) as Arc<dyn Engine>);
        let sched = make_scheduler(Arc::clone(&db));
        let mut tally = HashMap::new();
        let tid = shared_thread_id("c-sh");

        // Fire 1 + drain: turn 1 runs fresh (no resume) and parks `active`.
        sched.tick(at("2026-06-22T12:00:01Z"), &mut tally);
        exec.drain_pending(&NoPrompt).await;
        let after1 = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(after1.status, "active");
        assert_eq!(after1.engine_session_id.as_deref(), Some("sess-shared"));
        assert_eq!(*engine.resumes.lock().unwrap(), vec![None]);

        // Fire 2 + drain: the second fire EXECUTES a second engine run that
        // resumes the recorded session — the P0-1 defect was that no run
        // happened at all despite a `dispatched` cron_logs row.
        let advanced = lock_db(&db).get_cron("c-sh").unwrap().unwrap();
        let slot2 = parse_iso(advanced.next_run.as_deref().unwrap()).unwrap();
        sched.tick(slot2 + TimeDelta::seconds(1), &mut tally);
        exec.drain_pending(&NoPrompt).await;

        assert_eq!(
            *engine.resumes.lock().unwrap(),
            vec![None, Some("sess-shared".to_owned())],
            "the second fire must run the engine again, resuming the session"
        );
        let after2 = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(after2.status, "active");
        assert_eq!(
            after2.engine_session_id.as_deref(),
            Some("sess-shared"),
            "engine_session_id continuity kept across turns"
        );
        assert_eq!(lock_db(&db).max_round_seq("c-sh").unwrap(), Some(2));
    }

    /// A session-counting engine whose first run fails retryably
    /// (`runner.iteration_limit`) and whose second succeeds.
    struct FlakyEngine {
        sessions: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl Engine for FlakyEngine {
        fn name(&self) -> &'static str {
            "flaky"
        }
        async fn start_session(&self, _cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
            let first = self
                .sessions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0;
            let script = if first {
                vec![EngineEvent::Result(RunOutcome {
                    status: RunStatus::Error,
                    result: None,
                    session_id: Some("sess-flaky".to_owned()),
                    error: Some("run aborted: max turns reached".to_owned()),
                })]
            } else {
                vec![EngineEvent::Result(RunOutcome {
                    status: RunStatus::Success,
                    result: Some("done on retry".to_owned()),
                    session_id: Some("sess-flaky".to_owned()),
                    error: None,
                })]
            };
            Ok(Box::new(ScriptSession(script)))
        }
        async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
            unreachable!("the executor drives sessions, not run_once")
        }
    }

    /// P1-5: under the shipped default config a cron thread's retryable failure
    /// must actually retry — the config retry budget reaches cron threads
    /// (no more hardcoded `max_attempts: 1`) and attempt 2 happens.
    #[tokio::test]
    async fn cron_thread_retryable_failure_retries_under_default_config() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let scheduled = "2026-06-22T12:00:00.000Z";
        lock_db(&db)
            .insert_cron(&cron(
                "c-1",
                "interval",
                "60000",
                "isolated",
                Some(scheduled),
            ))
            .unwrap();
        let engine = Arc::new(FlakyEngine {
            sessions: std::sync::atomic::AtomicUsize::new(0),
        });
        let exec = executor_over(&db, Arc::clone(&engine) as Arc<dyn Engine>);
        let sched = make_scheduler(Arc::clone(&db));
        sched.tick(at("2026-06-22T12:00:01Z"), &mut HashMap::new());

        let tid = isolated_thread_id("c-1", at(scheduled));
        let minted = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(
            minted.max_attempts,
            ExecutorConfig::default().max_attempts,
            "config.executor.max_attempts reaches cron threads"
        );

        // Attempt 1 fails retryably → re-pended, not terminal.
        exec.drain_pending(&NoPrompt).await;
        let after1 = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(after1.status, "pending", "retryable failure is re-pended");
        assert_eq!(after1.attempt, 1);

        // Attempt 2 happens and succeeds.
        exec.drain_pending(&NoPrompt).await;
        let after2 = lock_db(&db).get_thread(&tid).unwrap().unwrap();
        assert_eq!(after2.status, "succeeded", "attempt 2 ran to success");
        assert_eq!(after2.result.as_deref(), Some("done on retry"));
        assert_eq!(
            engine.sessions.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the engine ran twice"
        );
    }

    #[test]
    fn run_count_reconciles_from_cron_logs_at_startup() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        // Stale cache: run_count 0 while the ledger already has three rounds.
        lock_db(&db)
            .insert_cron(&cron(
                "c-1",
                "interval",
                "60000",
                "isolated",
                Some("2026-06-22T12:10:00.000Z"),
            ))
            .unwrap();
        for (seq, slot) in [
            (1, "2026-06-22T12:00:00.000Z"),
            (2, "2026-06-22T12:01:00.000Z"),
            (3, "2026-06-22T12:02:00.000Z"),
        ] {
            lock_db(&db)
                .insert_cron_log(&CronLogRow {
                    id: 0,
                    cron_id: "c-1".to_owned(),
                    task_id: "task-1".to_owned(),
                    round_seq: seq,
                    scheduled_at: slot.to_owned(),
                    fired_at: None,
                    status: "dispatched".to_owned(),
                    skip_reason: None,
                    thread_id: None,
                    created_at: String::new(),
                })
                .unwrap();
        }
        let sched = make_scheduler(Arc::clone(&db));
        sched.reconcile_run_counts().unwrap();
        assert_eq!(
            lock_db(&db).get_cron("c-1").unwrap().unwrap().run_count,
            3,
            "run_count reconciled to cron_logs MAX(round_seq)"
        );
    }

    #[test]
    fn cron_state_summary_carries_local_counts() {
        let c = cron(
            "c-1",
            "interval",
            "60000",
            "isolated",
            Some("2026-06-22T12:01:00.000Z"),
        );
        let tally = CronTally {
            skipped: 2,
            last_status: Some("dispatched".to_owned()),
        };
        let data = cron_state_data(&c, 5, &tally);
        assert_eq!(data.round_count, 5);
        assert_eq!(data.skipped_count, 2);
        assert_eq!(data.last_status.as_deref(), Some("dispatched"));
        assert_eq!(data.continuity.as_deref(), Some("isolated"));
        assert_eq!(data.kind, "schedule");
    }

    #[test]
    fn emit_cron_state_enqueues_outbox_row() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        seed_task(&db);
        let c = cron(
            "c-1",
            "interval",
            "60000",
            "isolated",
            Some("2026-06-22T12:01:00.000Z"),
        );
        lock_db(&db).insert_cron(&c).unwrap();
        let sched = make_scheduler(Arc::clone(&db));
        sched.emit_cron_state(
            &c,
            1,
            &CronTally {
                skipped: 0,
                last_status: Some("dispatched".to_owned()),
            },
        );
        let rows = lock_db(&db).list_unacked_outbox().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].msg_type, "cron_state");
        assert_eq!(rows[0].task_id.as_deref(), Some("task-1"));
        // The cron id rides the secondary column so the transport can rebuild the
        // envelope `cronId` the host routes `cron_state` by (agent-architecture §10).
        assert_eq!(rows[0].thread_id.as_deref(), Some("c-1"));
    }
}
