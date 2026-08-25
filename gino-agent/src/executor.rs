//! Managed thread executor (PLAN-003 §5, PLAN-006 §3-D).
//!
//! The runner's execution plane is the [`ThreadExecutor`]: a **source-agnostic
//! pending-thread drainer** (agent-architecture §5/§6/§8) that claims `threads`
//! rows in `status='pending'` (regardless of whether a cron, a chat message, or
//! an api call created them), drives each through the lifecycle state machine
//! `pending → dispatched → running → {succeeded | failed | cancelled}` (a *shared*
//! thread settles to `active` between turns; a host `interrupt` settles the
//! in-flight turn `cancelled`), and emits the protocol-v2 result-plane frames
//! durably through the [`outbox`](crate::db). The runner drives it from a serial
//! worker loop (one engine run at a time) and reaches the live run via
//! [`ThreadExecutor::interrupt`].
//!
//! The executor is engine-agnostic: it dispatches only through the
//! [`Engine`](crate::engine::Engine) / [`Session`](crate::engine::Session) traits,
//! so Claude is just the first backend. The runner constructs the concrete engine
//! (with the resolved env, including `GINO_WORKSPACE_DIR` and the provisioned
//! `ENGINE_CONFIG_DIR`/`CLAUDE_CONFIG_DIR`) and hands it in as `Arc<dyn Engine>`.
//!
//! This module was born as `executor::v2` beside the v1 group/isolated `Executor`
//! it replaced; the v1 executor was removed in Lane G when the runner switched
//! over, and the migration shell has since been flattened away.
//!
//! ## Frame emission
//!
//! Frames are persisted straight into the at-least-once `outbox` via
//! [`Db::enqueue_outbox_correlated`](crate::db::Db::enqueue_outbox_correlated),
//! tagged with the `task_id`/`thread_id` correlation columns. Per agent-architecture
//! §8 the **outbox payload is the frame's `data`** (the envelope `{type, …ids,
//! data}` is rebuilt on send by the transport from the columns), so this module
//! stores [`ThreadMessageData`]/[`ThreadResultData`] payloads — it never builds the
//! outer [`OutboundFrameV2`](crate::protocol::v2::OutboundFrameV2). This keeps the
//! executor decoupled from the transport lane (E); the runner pokes the transport
//! flush after a drain so the enqueued rows ship over the live socket.
//!
//! ## Ordering
//!
//! Each thread carries a per-thread monotonic `seq` (assigned serially as messages
//! are emitted); the terminal `thread_result` reports `final_seq` so the host can
//! verify it received `1..=final_seq`. The counter is **persisted** in
//! `threads.last_seq`, so numbering continues across a runner restart (the host
//! dedups on `(thread_id, seq)`).
//!
//! The runner drives the whole module (drainer/interrupt/watchdog), so it carries
//! no module-level dead-code allow. The failure taxonomy it classifies into lives
//! in [`crate::failure`], whose not-yet-emitted reasons are covered by that module's
//! own allow.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::attachments::{AttachmentError, AttachmentManager, AttachmentSource};
use crate::db::{Db, DbResult, ThreadRow};
use crate::engine::{Engine, EngineEvent, LogLevel, RunStatus, Session, SessionCfg};
use crate::failure::{
    AgentFailure, FailureReason, RunnerFailure, classify_engine_error, classify_outcome,
};
use crate::protocol::v2::{
    AttachmentDescriptor, MessageKind, ProvisioningEntry, ThreadMessageData, ThreadResultData,
    ThreadStatus,
};
use crate::provisioning::Provisioner;
use crate::scheduler::{Cancel, now, parse_iso, to_iso};

/// Poison-tolerant DB lock (Lock 6): recover the guard rather than panic. Critical
/// sections are short and never span an `.await`.
pub(crate) fn lock_db(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The shared task-status gate (D1): `true` when `task_id` is `closed`/
/// `cancelled` and its work must stay parked — the scheduler skips its crons,
/// the drainer leaves its `pending` threads alone, and the chat re-scan leaves
/// its queued messages in the inbox. Parked work is never failed or deleted; a
/// `task_upsert{status:"active"}` resumes it. A lookup failure fails OPEN (the
/// work runs): parking is a task-status decision, not an availability one.
pub(crate) fn task_parked(db: &Mutex<Db>, task_id: &str) -> bool {
    match lock_db(db).task_execution_blocked(task_id) {
        Ok(blocked) => blocked,
        Err(err) => {
            warn!(task = %task_id, error = %err, "task status lookup failed; not parking");
            false
        }
    }
}

/// The per-task model override (OD15) for `task_id`, or `None` when the task
/// has none — then the engine keeps the globally configured `config.engine.model`.
/// A missing row (task rows are seeded lazily) and a lookup failure both answer
/// `None`: an unreadable definition must degrade to the global model, never fail
/// the run.
fn task_model(db: &Mutex<Db>, task_id: &str) -> Option<String> {
    match lock_db(db).get_task_row(task_id) {
        Ok(row) => row.and_then(|row| row.model),
        Err(err) => {
            warn!(task = %task_id, error = %err, "task model lookup failed; using the configured model");
            None
        }
    }
}

// -- thread.status string values (the engine lifecycle axis) ------------
const PENDING: &str = "pending";
const DISPATCHED: &str = "dispatched";
const RUNNING: &str = "running";
const ACTIVE: &str = "active";
const SUCCEEDED: &str = "succeeded";
const FAILED: &str = "failed";
const CANCELLED: &str = "cancelled";

/// Whether a thread is a one-shot (`isolated`) execution or a turn on an
/// ongoing (`shared`) thread. Decides the post-success resting state: an
/// isolated thread terminates `succeeded`; a shared thread settles to `active`
/// to await the next turn (agent-architecture §4/§5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Continuity {
    /// Fresh engine context per run; terminates `succeeded` on success.
    Isolated,
    /// Ongoing thread; settles to `active` (engine session reused) on success.
    Shared,
}

/// Watchdog thresholds (agent-architecture §5). A thread stuck `dispatched`
/// (claimed but the engine never started) past `dispatched_timeout`, or
/// `running` past `running_timeout`, is failed with `runner.timeout`. The
/// in-run `inactivity_timeout` kills a live engine that stops emitting events
/// ("kill only when the engine emits no events"). The total-time budget is
/// enforced on wall clock inside the run (the event wait is bounded by the
/// remaining budget), so a silent engine cannot wedge the serial plane even
/// with the inactivity watchdog disabled (P2-19).
#[derive(Clone, Copy, Debug)]
// The shared `_timeout` suffix is the meaningful distinction here (vs a count);
// dropping it would make `watchdog.running`/`dispatched` ambiguous at use sites.
#[allow(clippy::struct_field_names)]
pub(crate) struct WatchdogCfg {
    /// Max time a thread may sit `dispatched` before it is reaped (Td).
    pub(crate) dispatched_timeout: TimeDelta,
    /// Max total wall-clock for a `running` thread before it is reaped (Tr).
    pub(crate) running_timeout: TimeDelta,
    /// Max gap between engine events before a live run is killed; `None`
    /// disables the in-run inactivity watchdog (the total-time budget still
    /// bounds the run).
    pub(crate) inactivity_timeout: Option<std::time::Duration>,
    /// Bounded post-cancel drain (P1-6/P2-17): how long a cancelled turn may
    /// keep streaming (to settle with its own `result`) before the engine
    /// child is hard-killed by the session drop.
    pub(crate) cancel_drain_timeout: std::time::Duration,
}

impl Default for WatchdogCfg {
    fn default() -> Self {
        Self {
            dispatched_timeout: TimeDelta::seconds(60),
            running_timeout: TimeDelta::minutes(30),
            inactivity_timeout: Some(std::time::Duration::from_mins(5)),
            cancel_drain_timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// Resolves the engine prompt for a thread. The `threads` row carries no prompt
/// column (the prompt originates from an inbox `message` for chat threads or
/// from cron config for triggered threads), so the executor takes a resolver;
/// lane G supplies the real one. Returning `None` means "no runnable prompt",
/// which the drainer treats as `runner.invalid_request`.
pub(crate) trait PromptSource: Send + Sync {
    /// The prompt to run for `thread`, or `None` if none is available.
    fn prompt_for(&self, thread: &ThreadRow) -> Option<String>;
}

/// The terminal conclusion of one engine run, before retry/resume-safety policy
/// is applied.
enum Conclusion {
    /// The turn produced a result.
    Success {
        result: Option<String>,
        session_id: Option<String>,
    },
    /// The turn failed; `reason` drives retry + resume-safety.
    Failure {
        reason: FailureReason,
        error: String,
        session_id: Option<String>,
    },
    /// The in-flight turn was cancelled by a host `interrupt` (graceful, no
    /// retry). Settles the thread `cancelled` (agent-architecture §5).
    Cancelled { session_id: Option<String> },
}

/// The live engine run the executor is currently driving (at most one — the
/// drainer runs threads serially). Held so a host `interrupt` can reach the
/// running [`Session`] and trip its [`Cancel`] to settle the thread `cancelled`.
struct ActiveRun {
    thread_id: String,
    session: Arc<dyn Session>,
    cancel: Arc<Cancel>,
}

/// The out-of-band attachment plane wired onto an executor (PLAN-005, Lane F):
/// the byte backend + cache ([`AttachmentManager`]) and the per-thread inbound
/// descriptor resolver ([`AttachmentSource`]). Optional — when absent the
/// executor neither materializes inputs nor collects outputs (today's
/// behavior). Lane G constructs and wires it via [`ThreadExecutor::with_attachments`].
struct AttachmentRig {
    manager: Arc<AttachmentManager>,
    source: Arc<dyn AttachmentSource>,
}

/// The v2 pending-thread executor. Owns the engine, the DB handle, the
/// workspace root, the watchdog config, and the currently-running thread (for
/// graceful interrupt). The per-thread `seq` counter is persisted in
/// `threads.last_seq` so numbering continues across a runner restart.
pub(crate) struct ThreadExecutor {
    engine: Arc<dyn Engine>,
    db: Arc<Mutex<Db>>,
    workspace_dir: PathBuf,
    watchdog: WatchdogCfg,
    active: AsyncMutex<Option<ActiveRun>>,
    /// Out-of-band attachment plane (PLAN-005); `None` until Lane G wires it.
    attachments: Option<AttachmentRig>,
    /// Per-thread task config/skills provisioner (PLAN-007 §3/§6); `None` until
    /// the runner wires it. When present, each run materializes the task scope
    /// into `work_dir/project/` and the engine cwd becomes that `project/` dir;
    /// when absent the executor behaves as before (cwd = `work_dir`).
    provisioning: Option<Arc<Provisioner>>,
    /// The daemon shutdown token (P1-6); `None` until the runner wires it.
    /// When tripped it converges onto the same cancelled path a host
    /// `interrupt` takes: the active run settles within the bounded drain
    /// window and the pending drain stops starting new runs.
    shutdown: Option<Arc<Cancel>>,
    /// The transport flush signal (P2-16); `None` until the runner wires it.
    /// Poked after every persisted outbox frame so transcript events stream
    /// to the host near-real-time during a long run instead of arriving in
    /// one batch after the whole drain.
    flush: Option<Arc<Notify>>,
}

impl std::fmt::Debug for ThreadExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadExecutor")
            .field("engine", &self.engine.name())
            .field("workspace_dir", &self.workspace_dir)
            .field("watchdog", &self.watchdog)
            .finish_non_exhaustive()
    }
}

impl ThreadExecutor {
    /// Build a v2 executor over `engine`, persisting to `db`, materializing work
    /// dirs under `workspace_dir`, and reaping per the `watchdog` thresholds.
    pub(crate) fn new(
        engine: Arc<dyn Engine>,
        db: Arc<Mutex<Db>>,
        workspace_dir: PathBuf,
        watchdog: WatchdogCfg,
    ) -> Self {
        Self {
            engine,
            db,
            workspace_dir,
            watchdog,
            active: AsyncMutex::new(None),
            attachments: None,
            provisioning: None,
            shutdown: None,
            flush: None,
        }
    }

    /// Wire the transport flush signal (P2-16): the executor pokes it after
    /// each persisted outbox frame, so a `thread_message` streamed mid-run
    /// ships over the live socket immediately rather than after the whole
    /// drain. Builder-style so the existing [`Self::new`] call sites stay
    /// unchanged.
    pub(crate) fn with_flush(mut self, flush: Arc<Notify>) -> Self {
        self.flush = Some(flush);
        self
    }

    /// Whether the transport flush signal is wired (runner wiring seam).
    #[cfg(test)]
    pub(crate) fn flush_wired(&self) -> bool {
        self.flush.is_some()
    }

    /// Wire the daemon shutdown token (P1-6). Builder-style so the existing
    /// [`Self::new`] call sites (unit tests) stay unchanged. When the token
    /// trips, the active run is cancelled through the same path a host
    /// `interrupt` takes and [`Self::drain_pending`] stops starting new runs.
    pub(crate) fn with_shutdown(mut self, shutdown: Arc<Cancel>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Wire the out-of-band attachment plane (PLAN-005, Lane F): inbound
    /// descriptors are materialized into `work_dir/inputs/` before each run
    /// and `work_dir/output/` artifacts are uploaded after, their descriptors
    /// attached to the terminal `thread_result`. Builder-style so the existing
    /// [`Self::new`] call sites stay unchanged (Lane G opts in).
    pub(crate) fn with_attachments(
        mut self,
        manager: Arc<AttachmentManager>,
        source: Arc<dyn AttachmentSource>,
    ) -> Self {
        self.attachments = Some(AttachmentRig { manager, source });
        self
    }

    /// Wire the per-thread task provisioning plane (PLAN-007 §3/§6): before each
    /// run the task's desired entries are materialized into `work_dir/project/`
    /// (scope = task id, §9b), the isolated git working area `project/workspace/`
    /// is created, and the engine cwd becomes `project/`. Builder-style so the
    /// existing [`Self::new`] call sites (unit tests) stay on the back-compat path
    /// (cwd = `work_dir`, no `project/`).
    pub(crate) fn with_provisioning(mut self, provisioner: Arc<Provisioner>) -> Self {
        self.provisioning = Some(provisioner);
        self
    }

    // -- Pending-thread drainer (source-agnostic) -----------------------

    /// Drain every `pending` thread once, running each to a terminal (or
    /// retry-rescheduled) state. Returns the number of threads dispatched.
    /// Resolving no prompt for a pending thread fails it with
    /// `runner.invalid_request` so it is not re-drained forever; a thread under a
    /// closed/cancelled task is skipped and left `pending` ([`task_parked`], D1).
    pub(crate) async fn drain_pending(&self, prompts: &dyn PromptSource) -> usize {
        let pending = match lock_db(&self.db).list_pending_threads() {
            Ok(rows) => rows,
            Err(err) => {
                warn!(error = %err, "failed to list pending threads");
                return 0;
            }
        };
        let mut dispatched = 0;
        let mut parked = 0;
        for thread in pending {
            // Daemon shutdown (P1-6): stop starting new runs; the still-pending
            // threads re-run on the next boot.
            if self.shutdown_requested() {
                debug!("shutdown tripped; stopping the pending drain");
                break;
            }
            // Task-status gate (D1): a closed/cancelled task's thread stays
            // PARKED — left `pending`, never failed — and drains once the task
            // is re-activated. Counted into the drain summary rather than logged
            // per thread: this loop runs every poll interval.
            if task_parked(&self.db, &thread.task_id) {
                parked += 1;
                continue;
            }
            let Some((thread, prompt)) = self.resolve_prompt(thread, prompts) else {
                continue; // unrunnable: already failed `runner.invalid_request`
            };
            let continuity = self.continuity_for(&thread);
            self.run_thread(thread, prompt, continuity).await;
            dispatched += 1;
        }
        debug!(dispatched, parked, "drained pending threads");
        dispatched
    }

    /// Resolve the prompt a pending thread runs, honouring the **attempt-0
    /// snapshot** (agent-architecture §8, D10). A thread that already carries a
    /// `prompt` snapshot — every cron thread (the scheduler snapshots
    /// `crons.prompt` at fire time) and every retry/resume of any thread —
    /// **reuses** it verbatim and never re-resolves. Only a thread with no
    /// snapshot yet (a chat/api thread at attempt 0) goes to the
    /// [`PromptSource`], which **consumes** its inbox `message`; the resolved
    /// text is snapshotted into `threads.prompt` (persisted now) so a later
    /// retry reuses it instead of re-resolving a now-consumed message to
    /// `runner.invalid_request` — the chat-retry nuance D10 fixes. Returns
    /// `None` (after failing the thread `runner.invalid_request`) when no
    /// prompt is available.
    fn resolve_prompt(
        &self,
        mut thread: ThreadRow,
        prompts: &dyn PromptSource,
    ) -> Option<(ThreadRow, String)> {
        if !thread.prompt.trim().is_empty() {
            let prompt = thread.prompt.clone();
            return Some((thread, prompt));
        }
        let Some(prompt) = prompts.prompt_for(&thread) else {
            warn!(thread = %thread.id, "no prompt for pending thread; failing it");
            self.fail_unrunnable(&thread);
            return None;
        };
        thread.prompt.clone_from(&prompt);
        // Persist the snapshot immediately so a retry reuses it even if the run
        // never reaches `claim` (the inbox message is already consumed and
        // cannot be re-resolved).
        if let Err(err) = lock_db(&self.db).update_thread(&thread) {
            warn!(thread = %thread.id, error = %err, "failed to persist prompt snapshot");
        }
        Some((thread, prompt))
    }

    /// Fail a pending thread that has no resolvable prompt.
    fn fail_unrunnable(&self, thread: &ThreadRow) {
        if let Ok(Some(claimed)) = self.claim(thread) {
            let started = Self::started_or_now(&claimed);
            self.finalize(
                claimed,
                Conclusion::Failure {
                    reason: FailureReason::Runner(RunnerFailure::InvalidRequest),
                    error: "no prompt available for thread".to_owned(),
                    session_id: thread.engine_session_id.clone(),
                },
                started,
                Continuity::Isolated,
                Vec::new(),
            );
        }
    }

    // -- Single-thread run: the lifecycle state machine -----------------

    /// Run one thread end to end: `pending → dispatched → running → terminal`,
    /// streaming `thread_message` frames and emitting a terminal `thread_result`
    /// (unless a retryable failure reschedules it back to `pending`). Returns the
    /// final persisted row.
    pub(crate) async fn run_thread(
        &self,
        thread: ThreadRow,
        prompt: String,
        continuity: Continuity,
    ) -> ThreadRow {
        // 1. Claim: pending → dispatched (records started_at + work_dir).
        let thread = match self.claim(&thread) {
            Ok(Some(claimed)) => claimed,
            Ok(None) => {
                warn!(thread = %thread.id, "claim lost: thread no longer pending");
                return thread;
            }
            Err(err) => {
                warn!(thread = %thread.id, error = %err, "failed to claim thread");
                return thread;
            }
        };
        let started = Self::started_or_now(&thread);
        Self::ensure_work_dir(&thread);

        // 1b. Materialize inbound attachments into work_dir/inputs (A3) before
        // the engine starts. Unavailable inputs fail the thread terminally.
        if let Err(conclusion) = self.materialize_inbound(&thread).await {
            return self
                .conclude(thread, conclusion, started, continuity, Vec::new())
                .await;
        }

        // 2. dispatched → running, then start the engine session.
        let thread = match self.mark_running(&thread) {
            Ok(running) => running,
            Err(err) => {
                warn!(thread = %thread.id, error = %err, "failed to mark thread running");
                return thread;
            }
        };
        // Resolve the engine cwd: with the provisioning rig wired this
        // materializes the task scope into `work_dir/project/` and runs the
        // engine there; otherwise it is the `work_dir` itself (back-compat).
        let cwd = self.prepare_cwd(&thread).await;
        let cfg = SessionCfg {
            cwd,
            resume: thread.engine_session_id.clone(),
            initial_prompt: Some(prompt),
            task_id: Some(thread.task_id.clone()),
            thread_id: Some(thread.id.clone()),
            // Per-task model (OD15): read HERE, at spawn time, so a `task_upsert`
            // that changes the model reaches the next run without disturbing the
            // one in flight — the same apply-on-next-run policy as `config_update`.
            model: task_model(&self.db, &thread.task_id),
            ..SessionCfg::default()
        };
        let session: Arc<dyn Session> = match self.engine.start_session(cfg).await {
            Ok(session) => Arc::from(session),
            Err(err) => {
                let conclusion = Conclusion::Failure {
                    reason: classify_engine_error(&err),
                    error: format!("start_session failed: {err}"),
                    session_id: thread.engine_session_id.clone(),
                };
                return self
                    .conclude(thread, conclusion, started, continuity, Vec::new())
                    .await;
            }
        };

        // 3. Drive the event stream to a conclusion, then kill the session.
        // Publish the live run so a host `interrupt` can reach this session and
        // trip `cancel` (→ `cancelled`); clear it once driving completes.
        let cancel = Arc::new(Cancel::new());
        self.set_active(&thread.id, &session, &cancel).await;
        let mut stream = session.events();
        let conclusion = self
            .drive(
                &thread,
                &mut stream,
                started,
                thread.engine_session_id.clone(),
                &cancel,
            )
            .await;
        drop(stream);
        self.clear_active().await;
        drop(session);

        // 4. Collect output artifacts (A3) for a completed run, then finalize:
        // apply retry / resume-safety, persist, and emit the terminal result
        // carrying the outbound descriptors.
        let outputs = self.collect_outbound(&thread, &conclusion).await;
        self.conclude(thread, conclusion, started, continuity, outputs)
            .await
    }

    /// [`Self::finalize`] plus terminal-state provisioning cleanup (P1-11):
    /// when the run settles into a terminal state (not `active`, not a retry
    /// re-pend), the task-scope manifest rows are released — the scope's link
    /// tree lives under this thread's now-dead `work_dir`, so keeping the rows
    /// would make every startup repair rebuild symlinks inside dead work dirs
    /// and would pin the referenced store entries against GC forever.
    /// Decision (§9b is silent on scope death): cleanup keys on the THREAD's
    /// terminal state rather than task deletion, because the manifest's
    /// `link_path` roots at the thread's `project/`; a shared thread parks
    /// `active` between turns, so its scope survives until it truly ends. The
    /// next fire re-materializes from the still-cached bytes (store hit or
    /// attachment cache), so releasing costs no network round trip. The sync
    /// watchdog/offline reapers keep calling bare [`Self::finalize`]; a scope
    /// they leave behind is bounded (one row set per task, `UNIQUE(scope,
    /// target)`) and is replaced-then-released by the task's next fire.
    async fn conclude(
        &self,
        thread: ThreadRow,
        conclusion: Conclusion,
        started: DateTime<Utc>,
        continuity: Continuity,
        outputs: Vec<AttachmentDescriptor>,
    ) -> ThreadRow {
        let done = self.finalize(thread, conclusion, started, continuity, outputs);
        if matches!(done.status.as_str(), SUCCEEDED | FAILED | CANCELLED)
            && let Some(provisioner) = self.provisioning.as_ref()
            && let Err(err) = provisioner.release_scope(&done.task_id).await
        {
            warn!(thread = %done.id, task = %done.task_id, error = %err,
                "terminal provisioning cleanup failed (continuing)");
        }
        done
    }

    /// Materialize inbound attachments into `work_dir/inputs` (A3) before the
    /// engine starts. A no-op without an attachment rig or inbound descriptors.
    /// On failure the inputs are unavailable, so the thread fails terminally:
    /// transport/IO errors are retryable `runner.runtime_offline`; integrity,
    /// size/type, unsafe-name and unsupported-backend errors are non-retryable
    /// `runner.invalid_request`.
    async fn materialize_inbound(&self, thread: &ThreadRow) -> Result<(), Conclusion> {
        let Some(rig) = self.attachments.as_ref() else {
            return Ok(());
        };
        let descriptors = rig.source.inbound_for(thread);
        if descriptors.is_empty() {
            return Ok(());
        }
        let Some(work_dir) = thread.work_dir.as_deref() else {
            return Ok(());
        };
        match rig
            .manager
            .materialize_inputs(Path::new(work_dir), &descriptors)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                warn!(thread = %thread.id, error = %err, "inbound attachment materialization failed");
                Err(Conclusion::Failure {
                    reason: materialize_failure_reason(&err),
                    error: format!("inbound attachment materialization failed: {err}"),
                    session_id: thread.engine_session_id.clone(),
                })
            }
        }
    }

    /// Upload `work_dir/output/` artifacts (A3) after a completed run, returning
    /// the outbound descriptors for the terminal `thread_result`. Collected only
    /// for a result-producing or cancelled run — a retryable failure re-runs, so
    /// its partial artifacts are not shipped. A no-op without an attachment rig.
    async fn collect_outbound(
        &self,
        thread: &ThreadRow,
        conclusion: &Conclusion,
    ) -> Vec<AttachmentDescriptor> {
        if !matches!(
            conclusion,
            Conclusion::Success { .. } | Conclusion::Cancelled { .. }
        ) {
            return Vec::new();
        }
        let Some(rig) = self.attachments.as_ref() else {
            return Vec::new();
        };
        let Some(work_dir) = thread.work_dir.as_deref() else {
            return Vec::new();
        };
        rig.manager.collect_outputs(Path::new(work_dir)).await
    }

    /// Resolve the engine working directory for a run (PLAN-007 §3/§6).
    ///
    /// Without the provisioning rig the cwd is the thread's `work_dir` itself
    /// (back-compat; unit tests). With it wired, the per-thread `project/` layout
    /// is materialized first: `work_dir/project/` (the engine cwd) holds the
    /// task's instruction file + `.claude/skills` symlinks, and
    /// `project/workspace/` is the isolated git working area — created empty so a
    /// repo there never sees the injected config (PLAN-007 §3/§4). The attachment
    /// plane is UNCHANGED: `inputs/`/`output/` stay siblings under `work_dir`, not
    /// under `project/`.
    ///
    /// Degrade posture (mirrors the startup global reconcile in
    /// [`crate::runner`]): a `project/` creation or task reconcile failure is
    /// logged but NOT fatal — the engine still launches with cwd=`project/` and
    /// whatever is already materialized, rather than failing the thread.
    async fn prepare_cwd(&self, thread: &ThreadRow) -> PathBuf {
        let Some(work_dir) = thread.work_dir.as_deref().map(PathBuf::from) else {
            return PathBuf::new();
        };
        let Some(provisioner) = self.provisioning.as_ref() else {
            return work_dir;
        };
        let project_dir = work_dir.join("project");
        // Create `project/` and the isolated git working area `project/workspace/`
        // in one shot (best-effort, mirroring `ensure_work_dir`).
        if let Err(err) = std::fs::create_dir_all(project_dir.join("workspace")) {
            warn!(thread = %thread.id, error = %err, "failed to create project dir (continuing)");
        }
        // Materialize the task's desired entries into `project/` under the TASK
        // id scope (§9b: scope = 'global' | <task_id> — P1-11 fix; thread-id
        // scoping left one immortal row set per fire). `project/` itself stays
        // per-thread: a successive fire reconciles the same scope against its
        // new root, upserting the rows (UNIQUE(scope, target)) to point at the
        // fresh tree instead of accumulating. A reconcile error degrades — see
        // above.
        let entries = self.task_entries(&thread.task_id);
        if let Err(err) = provisioner
            .reconcile(&thread.task_id, &project_dir, &entries)
            .await
        {
            warn!(thread = %thread.id, error = %err, "task provisioning reconcile failed (continuing)");
        }
        project_dir
    }

    /// The task's desired provisioning entries (PLAN-007 §5), parsed from the
    /// persisted `tasks.entries` JSON. An absent column, a DB error or malformed
    /// JSON yields an empty set — reconciling against which simply installs
    /// nothing and removes any stale links for the scope.
    fn task_entries(&self, task_id: &str) -> Vec<ProvisioningEntry> {
        let json = match lock_db(&self.db).get_task_entries(task_id) {
            Ok(Some(json)) => json,
            Ok(None) => return Vec::new(),
            Err(err) => {
                warn!(task = %task_id, error = %err, "failed to read task entries (continuing)");
                return Vec::new();
            }
        };
        serde_json::from_str(&json).unwrap_or_else(|err| {
            warn!(task = %task_id, error = %err, "failed to parse task entries JSON (continuing)");
            Vec::new()
        })
    }

    /// Resolve a thread's continuity: a cron thread inherits its trigger's
    /// `crons.continuity` (`shared` → reuse the engine session between turns);
    /// a chat thread is ongoing (`shared`); everything else is one-shot
    /// (`isolated`). Used by [`Self::drain_pending`].
    fn continuity_for(&self, thread: &ThreadRow) -> Continuity {
        let shared = match thread.cron_id.as_deref() {
            Some(cron_id) => lock_db(&self.db)
                .get_cron(cron_id)
                .ok()
                .flatten()
                .is_some_and(|cron| cron.continuity == "shared"),
            None => thread.source == "chat",
        };
        if shared {
            Continuity::Shared
        } else {
            Continuity::Isolated
        }
    }

    /// Gracefully cancel the in-flight turn (host `interrupt`, and the daemon
    /// shutdown path in [`crate::runner`]): trip its [`Cancel`] so
    /// [`Self::drive`] settles the thread `cancelled`, and ask the engine to
    /// interrupt the current turn (no kill). A no-op when idle.
    pub(crate) async fn interrupt(&self) {
        let active = self.active.lock().await;
        if let Some(run) = active.as_ref() {
            run.cancel.cancel();
            if let Err(err) = run.session.interrupt().await {
                warn!(thread = %run.thread_id, error = %err, "engine interrupt failed");
            }
        } else {
            debug!("interrupt received with no active thread; ignoring");
        }
    }

    /// Drop any leftover active-run handle at shutdown, after the worker task
    /// has stopped (P1-6): releasing the session `Arc` kills the engine child
    /// (`kill_on_drop`), so no engine process survives the daemon.
    pub(crate) async fn kill_active(&self) {
        if let Some(run) = self.active.lock().await.take() {
            warn!(thread = %run.thread_id, "killing leftover engine session at shutdown");
        }
    }

    /// Whether the daemon shutdown token is wired and tripped.
    fn shutdown_requested(&self) -> bool {
        self.shutdown.as_deref().is_some_and(Cancel::is_cancelled)
    }

    /// Whether this run should stop: its own [`Cancel`] (host `interrupt`) or
    /// the daemon shutdown token — both cancellation sources converge on the
    /// one cancelled path (P1-6).
    fn cancel_requested(&self, cancel: &Cancel) -> bool {
        cancel.is_cancelled() || self.shutdown_requested()
    }

    /// Resolve once the run is cancelled by either source (see
    /// [`Self::cancel_requested`]).
    async fn run_cancelled(&self, cancel: &Cancel) {
        match self.shutdown.as_deref() {
            Some(shutdown) => tokio::select! {
                () = cancel.cancelled() => {}
                () = shutdown.cancelled() => {}
            },
            None => cancel.cancelled().await,
        }
    }

    /// Publish the currently-driven run so [`Self::interrupt`] can reach it.
    async fn set_active(&self, thread_id: &str, session: &Arc<dyn Session>, cancel: &Arc<Cancel>) {
        *self.active.lock().await = Some(ActiveRun {
            thread_id: thread_id.to_owned(),
            session: Arc::clone(session),
            cancel: Arc::clone(cancel),
        });
    }

    /// Drop the active-run handle once driving completes.
    async fn clear_active(&self) {
        *self.active.lock().await = None;
    }

    /// Claim a pending thread: `pending → dispatched`, stamping `started_at`
    /// (the attempt's start — both watchdogs key off it, the schema has no
    /// separate `dispatched_at`) and assigning the per-thread `work_dir`.
    /// The flip is a CAS guarded on `status = 'pending'` (audit P2-18);
    /// `None` means the claim was lost — the row is no longer pending (a
    /// concurrent claimer won) and this caller must not drive the thread.
    fn claim(&self, thread: &ThreadRow) -> DbResult<Option<ThreadRow>> {
        let mut claimed = thread.clone();
        DISPATCHED.clone_into(&mut claimed.status);
        let started_at = to_iso(now());
        let work_dir = self.work_dir_for(&thread.id);
        let won = lock_db(&self.db).claim_pending_thread(&thread.id, &started_at, &work_dir)?;
        claimed.started_at = Some(started_at);
        claimed.work_dir = Some(work_dir);
        Ok(won.then_some(claimed))
    }

    /// `dispatched → running` — the engine is about to start.
    fn mark_running(&self, thread: &ThreadRow) -> DbResult<ThreadRow> {
        let mut running = thread.clone();
        RUNNING.clone_into(&mut running.status);
        lock_db(&self.db).update_thread(&running)?;
        Ok(running)
    }

    /// Consume engine events until a terminal conclusion: a `Result`, an
    /// inactivity/total-time watchdog trip, a cancel (host `interrupt` or
    /// daemon shutdown — see [`Self::drain_cancelled`]), or the stream closing
    /// early.
    async fn drive(
        &self,
        thread: &ThreadRow,
        stream: &mut BoxStream<'_, EngineEvent>,
        started: DateTime<Utc>,
        prior_session: Option<String>,
        cancel: &Cancel,
    ) -> Conclusion {
        let mut session_id = prior_session;
        loop {
            // Host interrupt or daemon shutdown: settle `cancelled` after the
            // bounded post-cancel drain (graceful, no retry).
            if self.cancel_requested(cancel) {
                return self.drain_cancelled(stream, session_id).await;
            }
            // Total-time watchdog (Tr), on WALL CLOCK: the event wait below is
            // bounded by the remaining budget, so a silent engine cannot wedge
            // the serial plane even with the inactivity watchdog off (P2-19).
            let Some(remaining) = Self::remaining_budget(started, self.watchdog.running_timeout)
            else {
                return Conclusion::Failure {
                    reason: FailureReason::Runner(RunnerFailure::Timeout),
                    error: "watchdog: running exceeded the total time budget".to_owned(),
                    session_id,
                };
            };
            // Await the next event, racing the cancel (so a cancel mid-turn
            // settles promptly). The wait is bounded by the inactivity gap
            // (when enabled) capped to the remaining total budget.
            let gap = self
                .watchdog
                .inactivity_timeout
                .map_or(remaining, |gap| gap.min(remaining));
            let event = tokio::select! {
                biased;
                () = self.run_cancelled(cancel) => {
                    return self.drain_cancelled(stream, session_id).await;
                }
                res = timeout(gap, stream.next()) => match res {
                    Ok(event) => event,
                    Err(_elapsed) => {
                        let exhausted = Self::remaining_budget(
                            started,
                            self.watchdog.running_timeout,
                        )
                        .is_none();
                        let error = if exhausted {
                            "watchdog: running exceeded the total time budget"
                        } else {
                            "watchdog: engine inactive (no events)"
                        };
                        return Conclusion::Failure {
                            reason: FailureReason::Runner(RunnerFailure::Timeout),
                            error: error.to_owned(),
                            session_id,
                        };
                    }
                },
            };
            match event {
                None => {
                    return Conclusion::Failure {
                        reason: FailureReason::Agent(AgentFailure::ProcessFailure),
                        error: "engine exited before producing a result".to_owned(),
                        session_id,
                    };
                }
                Some(EngineEvent::SessionStarted { session_id: id }) => {
                    session_id = Some(id);
                }
                Some(EngineEvent::Output { text }) => {
                    // A tool-use-only assistant turn maps to an empty text
                    // chunk (audit P3): emitting it would burn an outbox seq
                    // on a contentless frame. Suppressed HERE (not in the
                    // engine mapping) so the event still arrives and feeds
                    // the inactivity watchdog above.
                    if !text.is_empty() {
                        self.stream_message(thread, MessageKind::AssistantText, text);
                    }
                }
                Some(EngineEvent::Log { level, message }) => {
                    let kind = if matches!(level, LogLevel::Error) {
                        MessageKind::Error
                    } else {
                        MessageKind::Status
                    };
                    self.stream_message(thread, kind, message);
                }
                Some(EngineEvent::Result(outcome)) => {
                    let resolved = outcome.session_id.or(session_id);
                    return match outcome.status {
                        RunStatus::Success => Conclusion::Success {
                            result: outcome.result,
                            session_id: resolved,
                        },
                        RunStatus::Error => {
                            // Classify off the structured cause only; the
                            // text is (or quotes) model output and rides
                            // along as detail, never as a policy input.
                            let reason = classify_outcome(outcome.error_kind);
                            let text = outcome
                                .error
                                .or(outcome.result)
                                .unwrap_or_else(|| "engine reported an error".to_owned());
                            Conclusion::Failure {
                                reason,
                                error: text,
                                session_id: resolved,
                            }
                        }
                    };
                }
            }
        }
    }

    /// Wall-clock time left in the total run budget; `None` once exhausted.
    fn remaining_budget(started: DateTime<Utc>, budget: TimeDelta) -> Option<std::time::Duration> {
        (started + budget - now())
            .to_std()
            .ok()
            .filter(|remaining| !remaining.is_zero())
    }

    /// Bounded post-cancel drain (P1-6, P2-17). The turn was cancelled — by a
    /// host `interrupt` or the daemon shutdown — and [`Self::interrupt`] asked
    /// the engine to stop gracefully. Give it `cancel_drain_timeout` to settle
    /// with its own `result`: an engine that settles has flushed its session
    /// state, so the resume handle it reports is trustworthy and KEPT. An
    /// engine that does not settle in the window (or exits without a result)
    /// is hard-killed by the session drop in [`Self::run_thread`]; the kill
    /// can tear session persistence mid-write, so the unverified resume
    /// handle is DROPPED — a hard-killed turn is never resumed.
    async fn drain_cancelled(
        &self,
        stream: &mut BoxStream<'_, EngineEvent>,
        mut session_id: Option<String>,
    ) -> Conclusion {
        let deadline = tokio::time::Instant::now() + self.watchdog.cancel_drain_timeout;
        loop {
            let Ok(event) = tokio::time::timeout_at(deadline, stream.next()).await else {
                debug!("cancelled turn did not settle within the drain window; hard-killing");
                return Conclusion::Cancelled { session_id: None };
            };
            match event {
                None => return Conclusion::Cancelled { session_id: None },
                Some(EngineEvent::SessionStarted { session_id: id }) => {
                    session_id = Some(id);
                }
                Some(EngineEvent::Result(outcome)) => {
                    return Conclusion::Cancelled {
                        session_id: outcome.session_id.or(session_id),
                    };
                }
                Some(_) => {}
            }
        }
    }

    /// Apply terminal policy and persist + emit. Success records the result and
    /// (for shared continuity) parks the thread `active`. Failure applies retry
    /// (`runner.*` while `attempt < max_attempts`) and resume-safety (drop
    /// `engine_session_id` on a poisoning reason) before either rescheduling to
    /// `pending` or recording `failed` + a terminal `thread_result`.
    fn finalize(
        &self,
        thread: ThreadRow,
        conclusion: Conclusion,
        started: DateTime<Utc>,
        continuity: Continuity,
        outputs: Vec<AttachmentDescriptor>,
    ) -> ThreadRow {
        let finished = now();
        let duration_ms = (finished - started).num_milliseconds().max(0);
        // Outbound artifact descriptors ride the terminal `thread_result` only
        // (a retry/reschedule emits no terminal result); `None` when empty.
        let outbound = (!outputs.is_empty()).then_some(outputs);
        match conclusion {
            Conclusion::Success { result, session_id } => {
                let mut done = thread;
                let resting = match continuity {
                    Continuity::Shared => ACTIVE,
                    Continuity::Isolated => SUCCEEDED,
                };
                resting.clone_into(&mut done.status);
                done.result.clone_from(&result);
                done.error = None;
                done.failure_reason = None;
                done.engine_session_id.clone_from(&session_id);
                done.finished_at = Some(to_iso(finished));
                done.duration_ms = Some(duration_ms);
                self.persist_and_emit(&done, |db| {
                    Self::emit_result(
                        db,
                        &done,
                        ThreadStatus::Succeeded,
                        result,
                        None,
                        None,
                        session_id,
                        outbound,
                    )
                });
                done
            }
            Conclusion::Failure {
                reason,
                error,
                session_id,
            } => {
                let attempt = thread.attempt + 1;
                // Resume-unsafe reasons drop the session so the next turn is fresh.
                let kept_session = if reason.resume_safe() {
                    session_id
                } else {
                    None
                };
                if reason.is_retryable() && attempt < thread.max_attempts {
                    self.reschedule(thread, attempt, kept_session, reason, error)
                } else {
                    self.record_failed(thread, attempt, kept_session, reason, error, duration_ms)
                }
            }
            Conclusion::Cancelled { session_id } => {
                // Terminal `cancelled`, no retry. Resume-handle policy (P2-17):
                // `session_id` is `Some` only when the engine settled its own
                // `result` inside the post-cancel drain window (state flushed,
                // handle verified — see `drain_cancelled`); a hard-killed turn
                // arrives here with `None`, dropping the handle so the next
                // turn never resumes a session SIGKILL may have torn mid-write.
                let mut done = thread;
                CANCELLED.clone_into(&mut done.status);
                done.engine_session_id.clone_from(&session_id);
                done.result = None;
                done.error = None;
                done.failure_reason = None;
                done.finished_at = Some(to_iso(finished));
                done.duration_ms = Some(duration_ms);
                self.persist_and_emit(&done, |db| {
                    Self::emit_result(
                        db,
                        &done,
                        ThreadStatus::Cancelled,
                        None,
                        None,
                        None,
                        session_id,
                        outbound,
                    )
                });
                done
            }
        }
    }

    /// Reschedule a retryable failure: back to `pending` for another attempt,
    /// emitting a `status` message (not a terminal `thread_result`).
    fn reschedule(
        &self,
        thread: ThreadRow,
        attempt: i64,
        kept_session: Option<String>,
        reason: FailureReason,
        error: String,
    ) -> ThreadRow {
        let mut retry = thread;
        // Build the status note first (borrows `error`), then move `error` in.
        let note = format!("retrying after {reason}: {error}");
        PENDING.clone_into(&mut retry.status);
        retry.attempt = attempt;
        retry.engine_session_id = kept_session;
        retry.started_at = None;
        retry.finished_at = None;
        retry.duration_ms = None;
        retry.result = None;
        retry.error = Some(error);
        retry.failure_reason = None;
        self.persist_and_emit(&retry, |db| {
            Self::emit_message(db, &retry, MessageKind::Status, Some(note), None)
        });
        retry
    }

    /// Record a terminal failure and emit the terminal `thread_result`.
    fn record_failed(
        &self,
        thread: ThreadRow,
        attempt: i64,
        kept_session: Option<String>,
        reason: FailureReason,
        error: String,
        duration_ms: i64,
    ) -> ThreadRow {
        let mut failed = thread;
        FAILED.clone_into(&mut failed.status);
        failed.attempt = attempt;
        failed.failure_reason = Some(reason.as_str().to_owned());
        failed.error = Some(error.clone());
        failed.result = None;
        failed.engine_session_id.clone_from(&kept_session);
        failed.finished_at = Some(to_iso(now()));
        failed.duration_ms = Some(duration_ms);
        self.persist_and_emit(&failed, |db| {
            Self::emit_result(
                db,
                &failed,
                ThreadStatus::Failed,
                None,
                Some(error),
                Some(reason),
                kept_session,
                None,
            )
        });
        failed
    }

    // -- Watchdog (reap stale / orphaned threads) -----------------------

    /// Whether `thread` (in `dispatched`/`running`) has overrun its watchdog
    /// budget as of `at`. Other states never go stale.
    pub(crate) fn is_stale(&self, thread: &ThreadRow, at: DateTime<Utc>) -> bool {
        let reference = thread
            .started_at
            .as_deref()
            .and_then(parse_iso)
            .or_else(|| parse_iso(&thread.created_at));
        let Some(reference) = reference else {
            return false;
        };
        let elapsed = at - reference;
        match thread.status.as_str() {
            DISPATCHED => elapsed > self.watchdog.dispatched_timeout,
            RUNNING => elapsed > self.watchdog.running_timeout,
            _ => false,
        }
    }

    /// Fail every stale `dispatched`/`running` thread among `candidates` with
    /// `runner.timeout` (agent-architecture §5). Since `runner.timeout` is
    /// retryable, a candidate with attempts left is rescheduled rather than
    /// terminated. Returns the number reaped. The candidate set is supplied by
    /// the caller (lane G — the live in-flight set, or a startup DB scan).
    pub(crate) fn reap_stale(&self, candidates: &[ThreadRow]) -> usize {
        let at = now();
        let mut reaped = 0;
        for thread in candidates {
            if !self.is_stale(thread, at) {
                continue;
            }
            let started = Self::started_or_now(thread);
            self.finalize(
                thread.clone(),
                Conclusion::Failure {
                    reason: FailureReason::Runner(RunnerFailure::Timeout),
                    error: format!(
                        "watchdog: thread stuck in '{}' past its deadline",
                        thread.status
                    ),
                    session_id: thread.engine_session_id.clone(),
                },
                started,
                Continuity::Isolated,
                Vec::new(),
            );
            reaped += 1;
        }
        debug!(reaped, "reaped stale threads");
        reaped
    }

    /// Fail every non-terminal thread among `candidates` with
    /// `runner.runtime_offline` — used on `close`/offline to flush orphans
    /// (agent-architecture §5). Retryable, so they re-run when back online.
    pub(crate) fn fail_offline(&self, candidates: &[ThreadRow]) -> usize {
        let mut failed = 0;
        for thread in candidates {
            if !matches!(thread.status.as_str(), DISPATCHED | RUNNING | ACTIVE) {
                continue;
            }
            let started = Self::started_or_now(thread);
            self.finalize(
                thread.clone(),
                Conclusion::Failure {
                    reason: FailureReason::Runner(RunnerFailure::RuntimeOffline),
                    error: "runtime offline: orphaned in-flight thread".to_owned(),
                    session_id: thread.engine_session_id.clone(),
                },
                started,
                Continuity::Isolated,
                Vec::new(),
            );
            failed += 1;
        }
        failed
    }

    // -- Outbox emission (frame data only; envelope rebuilt by transport) --

    /// Enqueue one streamed transcript message from a live run, logging (not
    /// propagating) a failure — a lost transcript line must not abort the run.
    fn stream_message(&self, thread: &ThreadRow, kind: MessageKind, content: String) {
        if let Err(err) = Self::emit_message(&lock_db(&self.db), thread, kind, Some(content), None)
        {
            warn!(thread = %thread.id, error = %err, "failed to enqueue thread_message");
        } else {
            // P2-16: ship the frame over the live socket now — without the
            // poke it would sit in the outbox until the post-drain flush.
            self.poke_flush();
        }
    }

    /// Poke the transport flush (when wired) so freshly enqueued outbox
    /// frames transmit immediately (P2-16). `Notify::notify_one` stores a
    /// permit, so a poke never blocks and never gets lost.
    fn poke_flush(&self) {
        if let Some(flush) = &self.flush {
            flush.notify_one();
        }
    }

    /// Stream one `thread_message` transcript event (per-thread monotonic
    /// `seq`), correlated by `task_id`/`thread_id`, on the caller-held `db`
    /// (a lone lock guard from [`Self::stream_message`], or the open finalize
    /// transaction from [`Self::persist_and_emit`]).
    fn emit_message(
        db: &Db,
        thread: &ThreadRow,
        kind: MessageKind,
        content: Option<String>,
        tool: Option<String>,
    ) -> DbResult<()> {
        let data = ThreadMessageData {
            seq: Self::next_seq(db, &thread.id)?,
            kind,
            role: None,
            content,
            tool,
            input: None,
            output: None,
        };
        db.enqueue_outbox_correlated(
            "thread_message",
            &data,
            Some(&thread.task_id),
            Some(&thread.id),
        )?;
        Ok(())
    }

    /// Emit the terminal `thread_result`, carrying `final_seq` = the last `seq`
    /// streamed on the thread. Runs on the caller-held `db` — in production
    /// always inside the [`Self::persist_and_emit`] transaction (P1-8).
    // One argument per `thread_result` `data` field (status/result/error/
    // failure_reason/engine_session_id/attachments); bundling them into a
    // struct here would only move the same fields behind one more name.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the thread_result frame fields one-to-one"
    )]
    fn emit_result(
        db: &Db,
        thread: &ThreadRow,
        status: ThreadStatus,
        result: Option<String>,
        error: Option<String>,
        reason: Option<FailureReason>,
        session_id: Option<String>,
        attachments: Option<Vec<AttachmentDescriptor>>,
    ) -> DbResult<()> {
        let data = ThreadResultData {
            status,
            result,
            error,
            failure_reason: reason.map(|r| r.as_str().to_owned()),
            engine_session_id: session_id,
            duration_ms: thread.duration_ms.unwrap_or(0),
            final_seq: db.get_thread_last_seq(&thread.id)?,
            attachments,
        };
        db.enqueue_outbox_correlated(
            "thread_result",
            &data,
            Some(&thread.task_id),
            Some(&thread.id),
        )?;
        Ok(())
    }

    // -- Small helpers --------------------------------------------------

    /// Persist a thread transition and enqueue its outbox frame in ONE
    /// `BEGIN IMMEDIATE` transaction (P1-8): the thread row and the frame
    /// commit together or not at all, so a crash between them can no longer
    /// strand a terminal thread the host never hears about. On error nothing
    /// is written — the row keeps its previous state for the watchdog /
    /// next drain to pick up again.
    fn persist_and_emit(&self, thread: &ThreadRow, emit: impl FnOnce(&Db) -> DbResult<()>) {
        let committed = {
            let guard = lock_db(&self.db);
            guard.immediate_transaction(|db| {
                db.update_thread(thread)?;
                emit(db)
            })
        };
        if let Err(err) = committed {
            warn!(thread = %thread.id, error = %err, "failed to commit thread state + outbox frame atomically");
        } else {
            // P2-16: the terminal frame is durable — stream it out now.
            self.poke_flush();
        }
    }

    /// The per-thread `work_dir`: `<workspace>/threads/<sanitized-id>`.
    fn work_dir_for(&self, thread_id: &str) -> String {
        self.workspace_dir
            .join("threads")
            .join(sanitize_id(thread_id))
            .to_string_lossy()
            .into_owned()
    }

    /// Create the thread's `work_dir` (best-effort; a failure is logged, not
    /// fatal — the engine surfaces the real error if it cannot use the dir).
    fn ensure_work_dir(thread: &ThreadRow) {
        if let Some(dir) = thread.work_dir.as_deref()
            && let Err(err) = std::fs::create_dir_all(dir)
        {
            warn!(thread = %thread.id, dir, error = %err, "failed to create work_dir");
        }
    }

    fn started_or_now(thread: &ThreadRow) -> DateTime<Utc> {
        thread
            .started_at
            .as_deref()
            .and_then(parse_iso)
            .unwrap_or_else(now)
    }

    /// Next per-thread `seq` (monotonic, starts at 1). Persisted to
    /// `threads.last_seq` so numbering **continues across a runner restart** —
    /// the host dedups on `(thread_id, seq)`, so restarting from 1 would collide
    /// and drop emits (agent-architecture §5/§6). A read+bump on the
    /// caller-held `db` (one lock guard / open transaction).
    fn next_seq(db: &Db, thread_id: &str) -> DbResult<i64> {
        let next = db.get_thread_last_seq(thread_id)? + 1;
        db.set_thread_last_seq(thread_id, next)?;
        Ok(next)
    }
}

/// Map an inbound-materialization error to a failure reason: a transport/IO
/// hiccup is retryable (`runner.runtime_offline` — re-fetch when back online);
/// integrity, size/type, unsafe-name and unsupported-backend errors are
/// non-retryable (`runner.invalid_request`).
fn materialize_failure_reason(err: &AttachmentError) -> FailureReason {
    match err {
        AttachmentError::Backend(_) | AttachmentError::Io(_) => {
            FailureReason::Runner(RunnerFailure::RuntimeOffline)
        }
        _ => FailureReason::Runner(RunnerFailure::InvalidRequest),
    }
}

/// Sanitize a thread id into a filesystem-safe directory component: keep
/// `[A-Za-z0-9-_@]`, map everything else (path separators, `.` — so no `..`
/// traversal) to `_`.
fn sanitize_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '@') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;
