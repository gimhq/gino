//! Managed thread executor (PLAN-003 §5, PLAN-006 §3-D).
//!
//! The runner's execution plane is the v2 [`ThreadExecutor`] (in [`v2`]): a
//! **source-agnostic pending-thread drainer** that claims `threads` rows in
//! `status='pending'`, drives each through the lifecycle state machine
//! `pending → dispatched → running → {succeeded | failed | cancelled}` (a *shared*
//! thread settles to `active` between turns), and streams the protocol-v2
//! result-plane frames durably through the [`outbox`](crate::db).
//!
//! The executor is engine-agnostic: it dispatches only through the
//! [`Engine`](crate::engine::Engine) / [`Session`](crate::engine::Session) traits,
//! so Claude is just the first backend. The runner constructs the concrete engine
//! (with the resolved env, including `GINO_WORKSPACE_DIR` and the provisioned
//! `ENGINE_CONFIG_DIR`/`CLAUDE_CONFIG_DIR`) and hands it in as `Arc<dyn Engine>`.
//!
//! The v1 group/isolated `Executor` this replaced was removed in Lane G when the
//! runner switched to the v2 path; only the shared poison-tolerant [`lock_db`]
//! helper it defined is retained, for [`v2`].

use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::db::Db;

/// Poison-tolerant DB lock (Lock 6): recover the guard rather than panic. Critical
/// sections are short and never span an `.await`.
fn lock_db(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

// ===========================================================================
// v2 executor (lane D) — pending-thread drainer + thread lifecycle state
// machine + failure taxonomy + retry + resume-safety + watchdog.
// ===========================================================================

/// The protocol-v2 execution engine (agent-architecture §5/§6/§8, PLAN-006 §3-D).
///
/// The v2 [`ThreadExecutor`] is a **source-agnostic pending-thread drainer**: it
/// claims `threads` rows in `status='pending'` (regardless of whether a cron, a
/// chat message, or an api call created them), drives each through the lifecycle
/// state machine `pending → dispatched → running → {succeeded | failed |
/// cancelled}` (a *shared* thread settles to `active` between turns; a host
/// `interrupt` settles the in-flight turn `cancelled`), and emits the protocol-v2
/// result-plane frames durably through the outbox. The runner drives it from a
/// serial worker loop (one engine run at a time) and reaches the live run via
/// [`ThreadExecutor::interrupt`].
///
/// ## Frame emission
///
/// Frames are persisted straight into the at-least-once `outbox` via
/// [`Db::enqueue_outbox_correlated`](crate::db::Db::enqueue_outbox_correlated),
/// tagged with the `task_id`/`thread_id` correlation columns. Per agent-architecture
/// §8 the **outbox payload is the frame's `data`** (the envelope `{type, …ids,
/// data}` is rebuilt on send by the transport from the columns), so this module
/// stores [`ThreadMessageData`]/[`ThreadResultData`] payloads — it never builds the
/// outer [`OutboundFrameV2`](crate::protocol::v2::OutboundFrameV2). This keeps the
/// executor decoupled from the transport lane (E); the runner pokes the transport
/// flush after a drain so the enqueued rows ship over the live socket.
///
/// ## Ordering
///
/// Each thread carries a per-thread monotonic `seq` (assigned serially as messages
/// are emitted); the terminal `thread_result` reports `final_seq` so the host can
/// verify it received `1..=final_seq`. The counter is **persisted** in
/// `threads.last_seq`, so numbering continues across a runner restart (the host
/// dedups on `(thread_id, seq)`).
///
/// The runner drives the whole module (drainer/interrupt/watchdog), so it carries
/// no module-level dead-code allow. The failure taxonomy it classifies into lives
/// in [`crate::failure`], whose not-yet-emitted reasons are covered by that module's
/// own allow.
pub(crate) mod v2 {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, TimeDelta, Utc};
    use futures_util::StreamExt;
    use futures_util::stream::BoxStream;
    use tokio::sync::{Mutex as AsyncMutex, Notify};
    use tokio::time::timeout;
    use tracing::{debug, warn};

    use super::lock_db;
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
        /// `runner.invalid_request` so it is not re-drained forever.
        pub(crate) async fn drain_pending(&self, prompts: &dyn PromptSource) -> usize {
            let pending = match lock_db(&self.db).list_pending_threads() {
                Ok(rows) => rows,
                Err(err) => {
                    warn!(error = %err, "failed to list pending threads");
                    return 0;
                }
            };
            let mut dispatched = 0;
            for thread in pending {
                // Daemon shutdown (P1-6): stop starting new runs; the still-pending
                // threads re-run on the next boot.
                if self.shutdown_requested() {
                    debug!("shutdown tripped; stopping the pending drain");
                    break;
                }
                let Some((thread, prompt)) = self.resolve_prompt(thread, prompts) else {
                    continue; // unrunnable: already failed `runner.invalid_request`
                };
                let continuity = self.continuity_for(&thread);
                self.run_thread(thread, prompt, continuity).await;
                dispatched += 1;
            }
            debug!(dispatched, "drained pending threads");
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
        async fn set_active(
            &self,
            thread_id: &str,
            session: &Arc<dyn Session>,
            cancel: &Arc<Cancel>,
        ) {
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
                let Some(remaining) =
                    Self::remaining_budget(started, self.watchdog.running_timeout)
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
                    Some(EngineEvent::Usage { .. }) => {}
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
        fn remaining_budget(
            started: DateTime<Utc>,
            budget: TimeDelta,
        ) -> Option<std::time::Duration> {
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
                        self.record_failed(
                            thread,
                            attempt,
                            kept_session,
                            reason,
                            error,
                            duration_ms,
                        )
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
            if let Err(err) =
                Self::emit_message(&lock_db(&self.db), thread, kind, Some(content), None)
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
    mod tests {
        use std::sync::{Arc, Mutex};

        use async_trait::async_trait;
        use chrono::TimeDelta;
        use futures_util::StreamExt;
        use futures_util::stream::{self, BoxStream};

        use std::path::PathBuf;

        use super::{Continuity, PromptSource, ThreadExecutor, WatchdogCfg};
        use crate::attachments::{
            AttachmentBackend, AttachmentError, AttachmentManager, AttachmentSource, sha256_hex,
        };
        use crate::config::AttachmentsConfig;
        use crate::db::{Db, TaskRow, ThreadRow};
        use crate::engine::{
            Engine, EngineError, EngineEvent, Input, Payload, RunErrorKind, RunOutcome, RunStatus,
            Session, SessionCfg,
        };
        use crate::protocol::v2::{
            AttachmentDescriptor, EntryKind, ProvisioningEntry, ThreadMessageData,
            ThreadResultData, ThreadStatus,
        };
        use crate::provisioning::{BlobFetcher, Provisioner, ProvisioningError};
        use crate::scheduler::{now, to_iso};
        use crate::transport::http::UploadedAttachment;

        // -- Deterministic stub engine (PLAN-006 §5b) -----------------------

        /// A scripted engine: each session replays `script` then, if `stall`,
        /// pends forever (drives the inactivity watchdog).
        struct MockEngine {
            script: Vec<EngineEvent>,
            stall: bool,
        }

        struct MockSession {
            script: Vec<EngineEvent>,
            stall: bool,
        }

        #[async_trait]
        impl Engine for MockEngine {
            fn name(&self) -> &'static str {
                "mock"
            }
            async fn start_session(
                &self,
                _cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                Ok(Box::new(MockSession {
                    script: self.script.clone(),
                    stall: self.stall,
                }))
            }
            async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
                for event in &self.script {
                    if let EngineEvent::Result(outcome) = event {
                        return Ok(outcome.clone());
                    }
                }
                Ok(RunOutcome {
                    status: RunStatus::Success,
                    result: None,
                    session_id: None,
                    error: None,
                    error_kind: None,
                })
            }
        }

        #[async_trait]
        impl Session for MockSession {
            async fn send(&self, _input: Input) -> Result<(), EngineError> {
                Ok(())
            }
            async fn interrupt(&self) -> Result<(), EngineError> {
                Ok(())
            }
            fn events(&self) -> BoxStream<'_, EngineEvent> {
                let scripted = stream::iter(self.script.clone());
                if self.stall {
                    Box::pin(scripted.chain(stream::pending::<EngineEvent>()))
                } else {
                    Box::pin(scripted)
                }
            }
        }

        struct StaticPrompt(&'static str);
        impl PromptSource for StaticPrompt {
            fn prompt_for(&self, _thread: &ThreadRow) -> Option<String> {
                Some(self.0.to_owned())
            }
        }

        // -- Fixtures -------------------------------------------------------

        fn started(session_id: &str) -> EngineEvent {
            EngineEvent::SessionStarted {
                session_id: session_id.to_owned(),
            }
        }

        fn out(text: &str) -> EngineEvent {
            EngineEvent::Output {
                text: text.to_owned(),
            }
        }

        fn ok_result(result: &str, session_id: &str) -> EngineEvent {
            EngineEvent::Result(RunOutcome {
                status: RunStatus::Success,
                result: Some(result.to_owned()),
                session_id: Some(session_id.to_owned()),
                error: None,
                error_kind: None,
            })
        }

        /// An errored result with no structured cause (`error_kind: None`) —
        /// the shape of an unclassifiable engine failure.
        fn err_result(error: &str) -> EngineEvent {
            err_result_kind(error, None)
        }

        fn err_result_kind(error: &str, error_kind: Option<RunErrorKind>) -> EngineEvent {
            EngineEvent::Result(RunOutcome {
                status: RunStatus::Error,
                result: None,
                session_id: None,
                error: Some(error.to_owned()),
                error_kind,
            })
        }

        fn test_db() -> Arc<Mutex<Db>> {
            let db = Db::open_in_memory().unwrap();
            db.insert_task_row(&TaskRow {
                id: "task-1".to_owned(),
                name: None,
                kind: None,
                status: "active".to_owned(),
                created_at: to_iso(now()),
                updated_at: to_iso(now()),
            })
            .unwrap();
            Arc::new(Mutex::new(db))
        }

        fn pending_thread(id: &str, max_attempts: i64) -> ThreadRow {
            ThreadRow {
                id: id.to_owned(),
                task_id: "task-1".to_owned(),
                cron_id: None,
                source: "api".to_owned(),
                // No snapshot yet: an attempt-0 api/chat thread resolves (and
                // snapshots) its prompt via the PromptSource on first drain.
                prompt: String::new(),
                engine: "mock".to_owned(),
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
                created_at: to_iso(now()),
            }
        }

        fn no_inactivity() -> WatchdogCfg {
            WatchdogCfg {
                inactivity_timeout: None,
                ..WatchdogCfg::default()
            }
        }

        fn build(
            db: &Arc<Mutex<Db>>,
            script: Vec<EngineEvent>,
            stall: bool,
            watchdog: WatchdogCfg,
        ) -> ThreadExecutor {
            let engine: Arc<dyn Engine> = Arc::new(MockEngine { script, stall });
            let dir = tempfile::tempdir().unwrap();
            ThreadExecutor::new(engine, Arc::clone(db), dir.path().to_path_buf(), watchdog)
        }

        fn messages(db: &Arc<Mutex<Db>>) -> Vec<ThreadMessageData> {
            lock(db)
                .list_unacked_outbox()
                .unwrap()
                .into_iter()
                .filter(|row| row.msg_type == "thread_message")
                .map(|row| serde_json::from_str(&row.payload).unwrap())
                .collect()
        }

        fn results(db: &Arc<Mutex<Db>>) -> Vec<ThreadResultData> {
            lock(db)
                .list_unacked_outbox()
                .unwrap()
                .into_iter()
                .filter(|row| row.msg_type == "thread_result")
                .map(|row| serde_json::from_str(&row.payload).unwrap())
                .collect()
        }

        fn lock(db: &Arc<Mutex<Db>>) -> std::sync::MutexGuard<'_, Db> {
            db.lock().unwrap()
        }

        // -- Tests ----------------------------------------------------------

        #[tokio::test]
        async fn run_thread_succeeds_streams_ordered_seq_and_keeps_session() {
            let db = test_db();
            let script = vec![
                started("sess-1"),
                EngineEvent::Output {
                    text: "hello".to_owned(),
                },
                EngineEvent::Output {
                    text: " world".to_owned(),
                },
                ok_result("done", "sess-1"),
            ];
            let exec = build(&db, script, false, no_inactivity());
            lock(&db)
                .insert_thread(&pending_thread("th-ok", 1))
                .unwrap();

            let final_row = exec
                .run_thread(
                    pending_thread("th-ok", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;

            assert_eq!(final_row.status, "succeeded");
            assert_eq!(final_row.result.as_deref(), Some("done"));
            assert_eq!(final_row.engine_session_id.as_deref(), Some("sess-1"));
            assert!(final_row.started_at.is_some() && final_row.finished_at.is_some());
            assert!(final_row.duration_ms.is_some());

            // The persisted row matches what run_thread returned.
            let stored = lock(&db).get_thread("th-ok").unwrap().unwrap();
            assert_eq!(stored.status, "succeeded");
            assert_eq!(stored.engine_session_id.as_deref(), Some("sess-1"));

            // Two streamed messages, seq 1 then 2 (monotonic), all correlated.
            let msgs = messages(&db);
            let seqs: Vec<i64> = msgs.iter().map(|m| m.seq).collect();
            assert_eq!(seqs, vec![1, 2], "per-thread seq is monotonic from 1");

            // One terminal result; final_seq == the last streamed seq.
            let res = results(&db);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].status, ThreadStatus::Succeeded);
            assert_eq!(res[0].final_seq, 2);
            assert_eq!(res[0].result.as_deref(), Some("done"));
            assert_eq!(res[0].engine_session_id.as_deref(), Some("sess-1"));
        }

        /// Audit P3: a tool-use-only assistant turn maps to an EMPTY `Output`
        /// chunk — it must not burn an outbox seq on a contentless
        /// `assistant_text` frame.
        #[tokio::test]
        async fn empty_assistant_text_is_not_emitted() {
            let db = test_db();
            let script = vec![
                started("sess-1"),
                out(""),
                out("real content"),
                ok_result("done", "sess-1"),
            ];
            let exec = build(&db, script, false, no_inactivity());
            lock(&db)
                .insert_thread(&pending_thread("th-empty", 1))
                .unwrap();

            let final_row = exec
                .run_thread(
                    pending_thread("th-empty", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(final_row.status, "succeeded");

            let msgs = messages(&db);
            assert_eq!(msgs.len(), 1, "the empty chunk is suppressed");
            assert_eq!(msgs[0].content.as_deref(), Some("real content"));
            assert_eq!(msgs[0].seq, 1, "no seq was wasted on the empty chunk");
            assert_eq!(results(&db)[0].final_seq, 1);
        }

        // -- P2-16: per-message flush poke streams transcript mid-run --------

        /// An engine whose session emits one message, BLOCKS until `gate` is
        /// notified, then emits a second message and the result. The test
        /// notifies the gate only once the mock host has RECEIVED the first
        /// message over the live socket, so the run can only complete if
        /// transcript frames stream out DURING the run (not after the drain).
        struct GatedEngine {
            gate: Arc<tokio::sync::Notify>,
        }

        struct GatedSession {
            gate: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl Engine for GatedEngine {
            fn name(&self) -> &'static str {
                "mock"
            }
            async fn start_session(
                &self,
                _cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                Ok(Box::new(GatedSession {
                    gate: Arc::clone(&self.gate),
                }))
            }
            async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
                Ok(RunOutcome::errored("run_once is not used by this test"))
            }
        }

        #[async_trait]
        impl Session for GatedSession {
            async fn send(&self, _input: Input) -> Result<(), EngineError> {
                Ok(())
            }
            async fn interrupt(&self) -> Result<(), EngineError> {
                Ok(())
            }
            fn events(&self) -> BoxStream<'_, EngineEvent> {
                let gate = Arc::clone(&self.gate);
                Box::pin(
                    stream::iter(vec![started("sess-live"), out("one")])
                        .chain(stream::once(async move {
                            gate.notified().await;
                            out("two")
                        }))
                        .chain(stream::iter(vec![ok_result("done", "sess-live")])),
                )
            }
        }

        /// P2-16 regression: transcript frames reach the host DURING a
        /// multi-message run. Without the executor's per-message flush poke the
        /// first frame would sit in the outbox until after the drain, the gate
        /// would never open, and this test would time out.
        #[tokio::test]
        async fn transcript_streams_to_the_host_during_the_run() {
            use tokio_tungstenite::tungstenite::Message;

            let db = test_db();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let gate = Arc::new(tokio::sync::Notify::new());

            let server_gate = Arc::clone(&gate);
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let mut contents = Vec::new();
                loop {
                    let text = match ws.next().await {
                        Some(Ok(Message::Text(text))) => text.as_str().to_owned(),
                        Some(Ok(_)) => continue,
                        other => panic!("connection ended before the result: {other:?}"),
                    };
                    let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
                    match frame["type"].as_str() {
                        Some("thread_message") => {
                            let content = frame["data"]["content"]
                                .as_str()
                                .unwrap_or_default()
                                .to_owned();
                            if content == "one" {
                                // The first transcript frame arrived over the
                                // live socket: release the still-running engine.
                                server_gate.notify_one();
                            }
                            contents.push(content);
                        }
                        Some("thread_result") => break,
                        _ => {}
                    }
                }
                contents
            });

            let transport = crate::transport::Transport::open(
                format!("ws://{addr}/agent/ws/agent-stream"),
                secrecy::SecretString::from("tok"),
                Arc::clone(&db),
                crate::transport::TransportConfig::default(),
            );

            let engine: Arc<dyn Engine> = Arc::new(GatedEngine { gate });
            let dir = tempfile::tempdir().unwrap();
            let exec = ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                dir.path().to_path_buf(),
                no_inactivity(),
            )
            .with_flush(transport.flush_handle());
            lock(&db)
                .insert_thread(&pending_thread("th-live", 1))
                .unwrap();

            let final_row = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                exec.run_thread(
                    pending_thread("th-live", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                ),
            )
            .await
            .expect("the run must complete: transcript frames must stream during the run");

            assert_eq!(final_row.status, "succeeded");
            let contents = server.await.unwrap();
            assert_eq!(contents, vec!["one".to_owned(), "two".to_owned()]);
            transport.shutdown().await;
        }

        #[tokio::test]
        async fn claim_then_running_walk_the_state_machine() {
            let db = test_db();
            let exec = build(&db, vec![], false, no_inactivity());
            let thread = pending_thread("th-sm", 1);
            lock(&db).insert_thread(&thread).unwrap();

            let claimed = exec.claim(&thread).unwrap().unwrap();
            assert_eq!(claimed.status, "dispatched");
            assert!(claimed.started_at.is_some(), "dispatch stamps started_at");
            assert!(claimed.work_dir.is_some(), "dispatch assigns work_dir");
            assert_eq!(
                lock(&db).get_thread("th-sm").unwrap().unwrap().status,
                "dispatched"
            );

            // P2-18 CAS regression: a second claim on the same (now dispatched)
            // thread loses — exactly one claimer ever drives a pending row.
            assert!(
                exec.claim(&thread).unwrap().is_none(),
                "second claim must lose the CAS"
            );

            let running = exec.mark_running(&claimed).unwrap();
            assert_eq!(running.status, "running");
            assert_eq!(
                lock(&db).get_thread("th-sm").unwrap().unwrap().status,
                "running"
            );
        }

        #[tokio::test]
        async fn runner_failure_retries_while_attempts_remain() {
            let db = test_db();
            // The structured `error_max_turns` cause classifies as
            // runner.iteration_limit (retryable).
            let exec = build(
                &db,
                vec![
                    started("s"),
                    err_result_kind("run aborted", Some(RunErrorKind::MaxTurns)),
                ],
                false,
                no_inactivity(),
            );
            lock(&db)
                .insert_thread(&pending_thread("th-retry", 2))
                .unwrap();

            let row = exec
                .run_thread(
                    pending_thread("th-retry", 2),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;

            // Rescheduled, not terminated: back to pending, attempt incremented.
            assert_eq!(row.status, "pending");
            assert_eq!(row.attempt, 1);
            assert!(row.failure_reason.is_none());
            // No terminal result emitted yet for a retry.
            assert!(results(&db).is_empty(), "retry emits no thread_result");
        }

        #[tokio::test]
        async fn success_prose_is_never_classified() {
            // P2-14 regression: a successful turn whose text happens to contain
            // error-looking prose ("took 1500ms", "400") stays a success — the
            // classifier never sees result text.
            let db = test_db();
            let exec = build(
                &db,
                vec![
                    started("s"),
                    ok_result("the fetch took 1500ms and returned 400 rows", "s"),
                ],
                false,
                no_inactivity(),
            );
            lock(&db)
                .insert_thread(&pending_thread("th-prose", 1))
                .unwrap();

            let row = exec
                .run_thread(
                    pending_thread("th-prose", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;

            assert_eq!(row.status, "succeeded");
            assert!(row.failure_reason.is_none());
            assert_eq!(row.engine_session_id.as_deref(), Some("s"));
        }

        #[tokio::test]
        async fn agent_failure_does_not_retry() {
            let db = test_db();
            // An error with no structured cause → agent.unknown (not retryable)
            // even with attempts to spare; the prose is never sniffed.
            let exec = build(
                &db,
                vec![started("s"), err_result("HTTP 401 Unauthorized")],
                false,
                no_inactivity(),
            );
            lock(&db)
                .insert_thread(&pending_thread("th-auth", 5))
                .unwrap();

            let row = exec
                .run_thread(
                    pending_thread("th-auth", 5),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;

            assert_eq!(row.status, "failed");
            assert_eq!(row.failure_reason.as_deref(), Some("agent.unknown"));
            let res = results(&db);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].status, ThreadStatus::Failed);
            assert_eq!(res[0].failure_reason.as_deref(), Some("agent.unknown"));
        }

        #[tokio::test]
        async fn prose_false_positives_no_longer_poison_the_session() {
            // P2-14 regression: error prose containing "400" (or timing text
            // like "took 1500ms") used to substring-classify as
            // runner.invalid_request / agent.provider_server_error, discarding
            // a healthy engine_session_id. With no structured cause the reason
            // is agent.unknown and the resume handle SURVIVES.
            let db = test_db();
            let exec = build(
                &db,
                vec![
                    started("sess-keep"),
                    err_result("the request took 1500ms and returned HTTP 400 Bad Request"),
                ],
                false,
                no_inactivity(),
            );
            lock(&db)
                .insert_thread(&pending_thread("th-keep", 1))
                .unwrap();
            let row = exec
                .run_thread(
                    pending_thread("th-keep", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(row.status, "failed");
            assert_eq!(row.failure_reason.as_deref(), Some("agent.unknown"));
            assert_eq!(
                row.engine_session_id.as_deref(),
                Some("sess-keep"),
                "prose false positives must not drop the resume handle"
            );
            assert_eq!(
                results(&db)[0].engine_session_id.as_deref(),
                Some("sess-keep")
            );
        }

        #[tokio::test]
        async fn resume_unsafe_reason_still_drops_the_session() {
            // The drop mechanic itself is unchanged: a resume-unsafe reason
            // (now only reachable from structured sources, e.g. inbound
            // materialization) still poisons the handle at finalize time.
            use crate::failure::{FailureReason, RunnerFailure};

            let db = test_db();
            let exec = build(&db, vec![], false, no_inactivity());
            let mut thread = pending_thread("th-poison", 1);
            thread.status = "running".to_owned();
            thread.engine_session_id = Some("sess-poison".to_owned());
            lock(&db).insert_thread(&thread).unwrap();

            let row = exec.finalize(
                thread,
                super::Conclusion::Failure {
                    reason: FailureReason::Runner(RunnerFailure::InvalidRequest),
                    error: "malformed request".to_owned(),
                    session_id: Some("sess-poison".to_owned()),
                },
                now(),
                Continuity::Isolated,
                Vec::new(),
            );
            assert_eq!(row.status, "failed");
            assert_eq!(
                row.failure_reason.as_deref(),
                Some("runner.invalid_request")
            );
            assert!(
                row.engine_session_id.is_none(),
                "poisoned session is dropped"
            );
            assert!(results(&db)[0].engine_session_id.is_none());
        }

        #[tokio::test]
        async fn drain_pending_runs_every_pending_thread() {
            let db = test_db();
            let exec = build(
                &db,
                vec![started("s"), ok_result("ok", "s")],
                false,
                no_inactivity(),
            );
            lock(&db).insert_thread(&pending_thread("th-a", 1)).unwrap();
            lock(&db).insert_thread(&pending_thread("th-b", 1)).unwrap();

            let dispatched = exec.drain_pending(&StaticPrompt("go")).await;
            assert_eq!(dispatched, 2);
            assert_eq!(
                lock(&db).get_thread("th-a").unwrap().unwrap().status,
                "succeeded"
            );
            assert_eq!(
                lock(&db).get_thread("th-b").unwrap().unwrap().status,
                "succeeded"
            );
            assert!(lock(&db).list_pending_threads().unwrap().is_empty());
        }

        #[tokio::test]
        async fn retry_reuses_prompt_snapshot_instead_of_re_resolving() {
            use std::sync::atomic::{AtomicUsize, Ordering};

            // The D10 chat-retry fix. A chat/api thread snapshots its resolved
            // prompt at attempt 0, and a retry RE-RUNS with that snapshot rather
            // than re-resolving a now-consumed inbox `message` (which would wrongly
            // yield runner.invalid_request). The PromptSource yields the prompt
            // once then `None` (a consumed message); the engine fails the first
            // turn retryably (runner.iteration_limit) then succeeds on the retry.
            struct ConsumeOnce {
                calls: AtomicUsize,
            }
            impl PromptSource for ConsumeOnce {
                fn prompt_for(&self, _thread: &ThreadRow) -> Option<String> {
                    (self.calls.fetch_add(1, Ordering::SeqCst) == 0)
                        .then(|| "the snapshotted instruction".to_owned())
                }
            }

            struct FlakyEngine {
                sessions: AtomicUsize,
            }
            #[async_trait]
            impl Engine for FlakyEngine {
                fn name(&self) -> &'static str {
                    "flaky"
                }
                async fn start_session(
                    &self,
                    _cfg: SessionCfg,
                ) -> Result<Box<dyn Session>, EngineError> {
                    let script = if self.sessions.fetch_add(1, Ordering::SeqCst) == 0 {
                        vec![
                            started("s1"),
                            err_result_kind("run aborted", Some(RunErrorKind::MaxTurns)),
                        ]
                    } else {
                        vec![started("s2"), ok_result("done on retry", "s2")]
                    };
                    Ok(Box::new(MockSession {
                        script,
                        stall: false,
                    }))
                }
                async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
                    Ok(RunOutcome {
                        status: RunStatus::Success,
                        result: None,
                        session_id: None,
                        error: None,
                        error_kind: None,
                    })
                }
            }

            let db = test_db();
            let engine: Arc<dyn Engine> = Arc::new(FlakyEngine {
                sessions: AtomicUsize::new(0),
            });
            let dir = tempfile::tempdir().unwrap();
            let exec = ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                dir.path().to_path_buf(),
                no_inactivity(),
            );
            let prompts = ConsumeOnce {
                calls: AtomicUsize::new(0),
            };
            // A fresh attempt-0 api thread carries no snapshot yet.
            lock(&db)
                .insert_thread(&pending_thread("th-flaky", 2))
                .unwrap();

            // Drain 1: resolve + snapshot at attempt 0; the run fails retryably and
            // the thread is re-pended for the retry.
            exec.drain_pending(&prompts).await;
            let after_first = lock(&db).get_thread("th-flaky").unwrap().unwrap();
            assert_eq!(after_first.status, "pending", "retryable failure re-pends");
            assert_eq!(after_first.attempt, 1);
            assert_eq!(
                after_first.prompt, "the snapshotted instruction",
                "attempt-0 resolution is snapshotted into threads.prompt"
            );
            assert_eq!(
                prompts.calls.load(Ordering::SeqCst),
                1,
                "the prompt is resolved exactly once, at attempt 0"
            );

            // Drain 2: the retry REUSES the snapshot — the PromptSource is never
            // consulted again — and re-runs to success (no runner.invalid_request).
            exec.drain_pending(&prompts).await;
            let after_retry = lock(&db).get_thread("th-flaky").unwrap().unwrap();
            assert_eq!(
                prompts.calls.load(Ordering::SeqCst),
                1,
                "retry reuses threads.prompt; PromptSource is NOT re-consulted"
            );
            assert_eq!(
                after_retry.status, "succeeded",
                "the retry re-ran with the snapshot and succeeded"
            );
            assert_eq!(after_retry.result.as_deref(), Some("done on retry"));
        }

        #[tokio::test]
        async fn shared_thread_settles_active_on_success() {
            let db = test_db();
            let exec = build(
                &db,
                vec![started("s"), ok_result("turn", "s")],
                false,
                no_inactivity(),
            );
            lock(&db)
                .insert_thread(&pending_thread("th-shared", 1))
                .unwrap();
            let row = exec
                .run_thread(
                    pending_thread("th-shared", 1),
                    "go".to_owned(),
                    Continuity::Shared,
                )
                .await;
            assert_eq!(
                row.status, "active",
                "a shared thread stays active between turns"
            );
            assert_eq!(row.engine_session_id.as_deref(), Some("s"));
        }

        #[tokio::test]
        async fn watchdog_reaps_stale_dispatched_and_running() {
            let db = test_db();
            let watchdog = WatchdogCfg {
                dispatched_timeout: TimeDelta::seconds(60),
                running_timeout: TimeDelta::minutes(30),
                inactivity_timeout: None,
                ..WatchdogCfg::default()
            };
            let exec = build(&db, vec![], false, watchdog);

            let mut stuck_dispatch = pending_thread("th-disp", 1);
            stuck_dispatch.status = "dispatched".to_owned();
            stuck_dispatch.started_at = Some(to_iso(now() - TimeDelta::seconds(120)));

            let mut stuck_run = pending_thread("th-run", 1);
            stuck_run.status = "running".to_owned();
            stuck_run.started_at = Some(to_iso(now() - TimeDelta::minutes(45)));

            let mut fresh = pending_thread("th-fresh", 1);
            fresh.status = "running".to_owned();
            fresh.started_at = Some(to_iso(now()));

            for thread in [&stuck_dispatch, &stuck_run, &fresh] {
                lock(&db).insert_thread(thread).unwrap();
            }
            let candidates = vec![stuck_dispatch, stuck_run, fresh];
            let reaped = exec.reap_stale(&candidates);
            assert_eq!(reaped, 2, "both stale threads reaped, the fresh one spared");

            let disp = lock(&db).get_thread("th-disp").unwrap().unwrap();
            let run = lock(&db).get_thread("th-run").unwrap().unwrap();
            let alive = lock(&db).get_thread("th-fresh").unwrap().unwrap();
            assert_eq!(disp.status, "failed");
            assert_eq!(disp.failure_reason.as_deref(), Some("runner.timeout"));
            assert_eq!(run.status, "failed");
            assert_eq!(run.failure_reason.as_deref(), Some("runner.timeout"));
            assert_eq!(alive.status, "running", "fresh thread untouched");
            assert_eq!(results(&db).len(), 2);
        }

        #[tokio::test]
        async fn inactivity_watchdog_fails_a_stalled_engine() {
            let db = test_db();
            // SessionStarted then the stream pends forever → inactivity trips.
            let watchdog = WatchdogCfg {
                inactivity_timeout: Some(std::time::Duration::from_millis(80)),
                ..WatchdogCfg::default()
            };
            let exec = build(&db, vec![started("s")], true, watchdog);
            lock(&db)
                .insert_thread(&pending_thread("th-stall", 1))
                .unwrap();

            let row = exec
                .run_thread(
                    pending_thread("th-stall", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(row.status, "failed");
            assert_eq!(row.failure_reason.as_deref(), Some("runner.timeout"));
        }

        #[tokio::test]
        async fn interrupt_settles_the_running_thread_cancelled() {
            let db = test_db();
            // SessionStarted then the stream pends forever, so the run is live until
            // the interrupt trips the cancel and settles it `cancelled`. The engine
            // ignores the interrupt (no post-cancel result), so the drain window
            // expires and the turn is hard-killed — keep the window short.
            let exec = Arc::new(build(
                &db,
                vec![started("sess-cancel")],
                true,
                WatchdogCfg {
                    inactivity_timeout: None,
                    cancel_drain_timeout: std::time::Duration::from_millis(100),
                    ..WatchdogCfg::default()
                },
            ));
            lock(&db)
                .insert_thread(&pending_thread("th-cancel", 1))
                .unwrap();

            let runner = Arc::clone(&exec);
            let handle = tokio::spawn(async move {
                runner
                    .run_thread(
                        pending_thread("th-cancel", 1),
                        "go".to_owned(),
                        Continuity::Isolated,
                    )
                    .await
            });

            // Wait until the engine is running, then interrupt the in-flight turn.
            for _ in 0..200 {
                if lock(&db).get_thread("th-cancel").unwrap().unwrap().status == "running" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            exec.interrupt().await;

            let row = handle.await.unwrap();
            assert_eq!(row.status, "cancelled");
            assert!(row.failure_reason.is_none(), "cancel is not a failure");
            // P2-17 policy: the engine never settled, so the turn was hard-killed
            // and the unverified resume handle is dropped.
            assert!(
                row.engine_session_id.is_none(),
                "a hard-killed turn keeps no resume handle"
            );
            let res = results(&db);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].status, ThreadStatus::Cancelled);
        }

        /// P2-17: a cancelled turn whose engine settles gracefully (its own
        /// `result` arrives inside the post-cancel drain window) KEEPS the
        /// verified resume handle instead of dropping it.
        #[tokio::test]
        async fn cancelled_turn_that_settles_gracefully_keeps_the_resume_handle() {
            use tokio::sync::mpsc;

            /// Stalls until interrupted, then settles like a live engine does on
            /// a graceful turn cancel: its own post-interrupt `result`.
            struct SettlingEngine;
            struct SettlingSession {
                tx: mpsc::UnboundedSender<EngineEvent>,
                rx: Mutex<Option<mpsc::UnboundedReceiver<EngineEvent>>>,
            }

            #[async_trait]
            impl Engine for SettlingEngine {
                fn name(&self) -> &'static str {
                    "settling"
                }
                async fn start_session(
                    &self,
                    _cfg: SessionCfg,
                ) -> Result<Box<dyn Session>, EngineError> {
                    let (tx, rx) = mpsc::unbounded_channel();
                    tx.send(EngineEvent::SessionStarted {
                        session_id: "sess-live".to_owned(),
                    })
                    .unwrap();
                    Ok(Box::new(SettlingSession {
                        tx,
                        rx: Mutex::new(Some(rx)),
                    }))
                }
                async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
                    unreachable!("the executor drives sessions")
                }
            }

            #[async_trait]
            impl Session for SettlingSession {
                async fn send(&self, _input: Input) -> Result<(), EngineError> {
                    Ok(())
                }
                async fn interrupt(&self) -> Result<(), EngineError> {
                    let _ = self.tx.send(EngineEvent::Result(RunOutcome {
                        status: RunStatus::Error,
                        result: None,
                        session_id: Some("sess-settled".to_owned()),
                        error: Some("aborted by user".to_owned()),
                        error_kind: None,
                    }));
                    Ok(())
                }
                fn events(&self) -> BoxStream<'_, EngineEvent> {
                    match self.rx.lock().unwrap().take() {
                        Some(rx) => Box::pin(stream::unfold(rx, |mut rx| async move {
                            rx.recv().await.map(|event| (event, rx))
                        })),
                        None => Box::pin(stream::empty()),
                    }
                }
            }

            let db = test_db();
            let engine: Arc<dyn Engine> = Arc::new(SettlingEngine);
            let dir = tempfile::tempdir().unwrap();
            let exec = Arc::new(ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                dir.path().to_path_buf(),
                no_inactivity(),
            ));
            lock(&db)
                .insert_thread(&pending_thread("th-settle", 1))
                .unwrap();

            let runner = Arc::clone(&exec);
            let handle = tokio::spawn(async move {
                runner
                    .run_thread(
                        pending_thread("th-settle", 1),
                        "go".to_owned(),
                        Continuity::Isolated,
                    )
                    .await
            });
            for _ in 0..200 {
                if lock(&db).get_thread("th-settle").unwrap().unwrap().status == "running" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            exec.interrupt().await;

            let row = handle.await.unwrap();
            assert_eq!(row.status, "cancelled");
            assert_eq!(
                row.engine_session_id.as_deref(),
                Some("sess-settled"),
                "a gracefully settled cancel keeps the verified resume handle"
            );
        }

        /// P1-6 regression: a daemon shutdown while a run is active cancels the
        /// run through the same path as a host interrupt (bounded drain — no
        /// `running_timeout` wait) and stops the pending drain from starting new
        /// runs (the still-pending threads re-run on the next boot).
        #[tokio::test]
        async fn shutdown_cancels_the_active_run_and_stops_the_drain() {
            let db = test_db();
            let shutdown = Arc::new(crate::scheduler::Cancel::new());
            let engine: Arc<dyn Engine> = Arc::new(MockEngine {
                script: vec![started("s")],
                stall: true,
            });
            let dir = tempfile::tempdir().unwrap();
            let exec = Arc::new(
                ThreadExecutor::new(
                    engine,
                    Arc::clone(&db),
                    dir.path().to_path_buf(),
                    WatchdogCfg {
                        inactivity_timeout: None,
                        cancel_drain_timeout: std::time::Duration::from_millis(100),
                        ..WatchdogCfg::default()
                    },
                )
                .with_shutdown(Arc::clone(&shutdown)),
            );
            lock(&db).insert_thread(&pending_thread("th-a", 1)).unwrap();
            lock(&db).insert_thread(&pending_thread("th-b", 1)).unwrap();

            let drainer = Arc::clone(&exec);
            let handle =
                tokio::spawn(async move { drainer.drain_pending(&StaticPrompt("go")).await });

            // Wait until one thread is live, then shut down.
            for _ in 0..200 {
                if !lock(&db)
                    .list_threads_by_status("running")
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            shutdown.cancel();

            // The drain settles within seconds — nowhere near running_timeout.
            let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
                .await
                .expect("shutdown did not settle the pending drain")
                .unwrap();
            assert_eq!(dispatched, 1, "only the in-flight thread was dispatched");
            // One lock per statement: two `lock(&db)` temporaries in a single
            // expression deadlock on the non-reentrant mutex.
            let status_a = lock(&db).get_thread("th-a").unwrap().unwrap().status;
            let status_b = lock(&db).get_thread("th-b").unwrap().unwrap().status;
            let mut statuses = [status_a, status_b];
            statuses.sort();
            assert_eq!(
                statuses,
                ["cancelled", "pending"],
                "the active run settles cancelled; the queued thread is left for the next boot"
            );
        }

        /// P2-19 regression: with the inactivity watchdog DISABLED (config 0) a
        /// silent engine must still be bounded by the total-time watchdog on wall
        /// clock — the old code only evaluated it on event arrival, wedging the
        /// serial execution plane forever.
        #[tokio::test]
        async fn total_time_watchdog_fires_on_wall_clock_without_events() {
            let db = test_db();
            let watchdog = WatchdogCfg {
                running_timeout: TimeDelta::milliseconds(200),
                inactivity_timeout: None,
                ..WatchdogCfg::default()
            };
            let exec = build(&db, vec![started("s")], true, watchdog);
            lock(&db)
                .insert_thread(&pending_thread("th-wedge", 1))
                .unwrap();

            let row = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                exec.run_thread(
                    pending_thread("th-wedge", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                ),
            )
            .await
            .expect("a wedged engine must be reaped on wall clock");
            assert_eq!(row.status, "failed");
            assert_eq!(row.failure_reason.as_deref(), Some("runner.timeout"));
        }

        #[tokio::test]
        async fn seq_continues_across_runs_via_persisted_last_seq() {
            // The persisted `threads.last_seq` makes seq numbering continue across a
            // fresh executor (a restart): the second run keeps counting, not resets.
            let db = test_db();
            lock(&db)
                .insert_thread(&pending_thread("th-seq", 1))
                .unwrap();

            let first = build(
                &db,
                vec![started("s"), out("a"), ok_result("r1", "s")],
                false,
                no_inactivity(),
            );
            first
                .run_thread(
                    pending_thread("th-seq", 1),
                    "go".to_owned(),
                    Continuity::Shared,
                )
                .await;
            assert_eq!(lock(&db).get_thread_last_seq("th-seq").unwrap(), 1);

            // A brand-new executor (no in-process counter) resumes from last_seq.
            let second = build(
                &db,
                vec![out("b"), ok_result("r2", "s")],
                false,
                no_inactivity(),
            );
            // Claim is a CAS on `pending` (P2-18), and the drain plane only
            // ever runs pending rows — a shared re-fire re-pends first (the
            // scheduler's repend_shared_thread), mirrored here.
            let mut resumed = lock(&db).get_thread("th-seq").unwrap().unwrap();
            "pending".clone_into(&mut resumed.status);
            lock(&db).update_thread(&resumed).unwrap();
            let row = second
                .run_thread(resumed, "again".to_owned(), Continuity::Shared)
                .await;
            assert_eq!(row.status, "active");
            // Two messages total (seq 1 then 2), the second result's final_seq is 2.
            let res = results(&db);
            assert_eq!(
                res.last().unwrap().final_seq,
                2,
                "seq continued, did not reset"
            );
        }

        /// P1-8 regression: finalize commits the terminal thread row and its
        /// `thread_result` outbox frame in ONE transaction — they appear
        /// together or not at all. A failure enqueuing the frame (forced by
        /// dropping the outbox table) must roll the terminal persist back too,
        /// so a crash between the two writes can never strand a terminal
        /// thread the host is never told about.
        #[tokio::test]
        async fn finalize_commits_thread_state_and_result_frame_atomically() {
            let db = test_db();
            let script = vec![started("sess-1"), ok_result("done", "sess-1")];
            let exec = build(&db, script, false, no_inactivity());
            lock(&db)
                .insert_thread(&pending_thread("th-atomic", 1))
                .unwrap();
            // Force the transaction's outbox write to fail mid-finalize.
            lock(&db).exec_raw("DROP TABLE outbox").unwrap();

            let pending = lock(&db).get_thread("th-atomic").unwrap().unwrap();
            let returned = exec
                .run_thread(pending, "go".to_owned(), Continuity::Isolated)
                .await;
            assert_eq!(returned.status, "succeeded", "the run itself concluded");

            // NOT terminal in the DB: the terminal persist rolled back with the
            // failed enqueue (previously it committed alone — the P1-8 defect).
            let persisted = lock(&db).get_thread("th-atomic").unwrap().unwrap();
            assert_eq!(
                persisted.status, "running",
                "terminal state must not commit without its thread_result frame"
            );

            // Restore the outbox (empty) and re-run the thread: now BOTH commit.
            lock(&db).restore_outbox_table().unwrap();
            assert!(results(&db).is_empty(), "no orphan thread_result frame");
            let mut repend = persisted;
            "pending".clone_into(&mut repend.status);
            lock(&db).update_thread(&repend).unwrap();
            let repended = lock(&db).get_thread("th-atomic").unwrap().unwrap();
            exec.run_thread(repended, "go".to_owned(), Continuity::Isolated)
                .await;
            let persisted = lock(&db).get_thread("th-atomic").unwrap().unwrap();
            assert_eq!(persisted.status, "succeeded");
            assert_eq!(
                results(&db).len(),
                1,
                "the terminal row and its frame land together"
            );
        }

        // -- Attachment wiring (Lane F) -------------------------------------

        /// Backend serving a preloaded download + recording uploads. On `Arc` so
        /// the manager owns a `'static` box while the test keeps a handle.
        #[derive(Default)]
        struct RigBackend {
            store: Mutex<std::collections::HashMap<String, Vec<u8>>>,
            uploads: Mutex<Vec<(String, Vec<u8>)>>,
        }

        #[async_trait]
        impl AttachmentBackend for Arc<RigBackend> {
            async fn download(
                &self,
                descriptor: &AttachmentDescriptor,
            ) -> Result<Vec<u8>, AttachmentError> {
                self.store
                    .lock()
                    .unwrap()
                    .get(&descriptor.reference)
                    .cloned()
                    .ok_or(AttachmentError::Unsupported)
            }
            async fn upload(
                &self,
                name: &str,
                _content_type: &str,
                _sha256: &str,
                bytes: Vec<u8>,
            ) -> Result<UploadedAttachment, AttachmentError> {
                let mut uploads = self.uploads.lock().unwrap();
                let id = format!("att-{}", uploads.len());
                uploads.push((name.to_owned(), bytes));
                Ok(UploadedAttachment {
                    reference: format!("/agent/attachments/{id}"),
                    id,
                })
            }
        }

        /// Resolves a fixed inbound descriptor list for any thread.
        struct StaticAttachments(Vec<AttachmentDescriptor>);
        impl AttachmentSource for StaticAttachments {
            fn inbound_for(&self, _thread: &ThreadRow) -> Vec<AttachmentDescriptor> {
                self.0.clone()
            }
        }

        /// Engine that writes one artifact into `cwd/output/` when a session starts
        /// (so `collect_outputs` has something to upload), then succeeds.
        struct WritingEngine {
            rel: String,
            bytes: Vec<u8>,
        }

        #[async_trait]
        impl Engine for WritingEngine {
            fn name(&self) -> &'static str {
                "writing"
            }
            async fn start_session(
                &self,
                cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                let out = cfg.cwd.join("output");
                std::fs::create_dir_all(&out).unwrap();
                std::fs::write(out.join(&self.rel), &self.bytes).unwrap();
                Ok(Box::new(MockSession {
                    script: vec![started("sess-att"), ok_result("done", "sess-att")],
                    stall: false,
                }))
            }
            async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
                Ok(RunOutcome {
                    status: RunStatus::Success,
                    result: Some("done".to_owned()),
                    session_id: Some("sess-att".to_owned()),
                    error: None,
                    error_kind: None,
                })
            }
        }

        #[tokio::test]
        async fn attachments_round_trip_onto_thread_result_without_bytes_in_frames() {
            let db = test_db();
            let ws = tempfile::tempdir().unwrap();

            // Preload one inbound blob; the backend also records uploads.
            let backend = Arc::new(RigBackend::default());
            let input_bytes = b"INPUT-BYTES-SECRET".to_vec();
            backend
                .store
                .lock()
                .unwrap()
                .insert("/agent/attachments/in-1".to_owned(), input_bytes.clone());
            let inbound = AttachmentDescriptor {
                id: "in-1".to_owned(),
                name: "prompt.txt".to_owned(),
                content_type: "text/plain".to_owned(),
                size: i64::try_from(input_bytes.len()).unwrap(),
                sha256: sha256_hex(&input_bytes),
                reference: "/agent/attachments/in-1".to_owned(),
                direction: Some("inbound".to_owned()),
            };

            let output_bytes = b"OUTPUT-BYTES-SECRET".to_vec();
            let engine: Arc<dyn Engine> = Arc::new(WritingEngine {
                rel: "result.txt".to_owned(),
                bytes: output_bytes.clone(),
            });
            let manager = Arc::new(AttachmentManager::new(
                Box::new(Arc::clone(&backend)),
                ws.path(),
                AttachmentsConfig::default(),
            ));
            let source = Arc::new(StaticAttachments(vec![inbound]));
            let exec = ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                ws.path().to_path_buf(),
                no_inactivity(),
            )
            .with_attachments(manager, source);

            lock(&db)
                .insert_thread(&pending_thread("th-att", 1))
                .unwrap();
            let final_row = exec
                .run_thread(
                    pending_thread("th-att", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(final_row.status, "succeeded");

            // Inbound descriptor was materialized into work_dir/inputs/.
            let work_dir = final_row.work_dir.clone().unwrap();
            let input_path = std::path::Path::new(&work_dir)
                .join("inputs")
                .join("prompt.txt");
            assert_eq!(std::fs::read(&input_path).unwrap(), input_bytes);

            // The output artifact was uploaded out-of-band (one recorded upload).
            assert_eq!(backend.uploads.lock().unwrap().len(), 1);

            // The terminal thread_result carries the OUTBOUND descriptor.
            let res = results(&db);
            assert_eq!(res.len(), 1);
            let atts = res[0]
                .attachments
                .clone()
                .expect("thread_result carries output attachments");
            assert_eq!(atts.len(), 1);
            assert_eq!(atts[0].name, "result.txt");
            assert_eq!(atts[0].direction.as_deref(), Some("outbound"));
            assert_eq!(atts[0].sha256, sha256_hex(&output_bytes));
            assert_eq!(atts[0].reference, "/agent/attachments/att-0");

            // CRITICAL (PLAN-005 risk 1): bytes NEVER ride a frame. No outbox
            // payload (thread_message or thread_result) contains the raw bytes.
            for row in lock(&db).list_unacked_outbox().unwrap() {
                assert!(
                    !row.payload.contains("INPUT-BYTES-SECRET"),
                    "input bytes leaked into a {} frame",
                    row.msg_type
                );
                assert!(
                    !row.payload.contains("OUTPUT-BYTES-SECRET"),
                    "output bytes leaked into a {} frame",
                    row.msg_type
                );
            }
        }

        // -- Per-thread task provisioning (PLAN-007 §3/§6) ------------------

        /// An engine that records the `cwd` it is launched with, then replays a
        /// trivial successful run. Lets a test assert where `run_thread` points the
        /// engine (work_dir vs work_dir/project).
        struct CwdProbe {
            seen: Arc<Mutex<Option<PathBuf>>>,
        }

        #[async_trait]
        impl Engine for CwdProbe {
            fn name(&self) -> &'static str {
                "cwd-probe"
            }
            async fn start_session(
                &self,
                cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                *self.seen.lock().unwrap() = Some(cfg.cwd.clone());
                Ok(Box::new(MockSession {
                    script: vec![started("sess-prov"), ok_result("done", "sess-prov")],
                    stall: false,
                }))
            }
            async fn run_once(&self, _payload: Payload) -> Result<RunOutcome, EngineError> {
                Ok(RunOutcome {
                    status: RunStatus::Success,
                    result: Some("done".to_owned()),
                    session_id: Some("sess-prov".to_owned()),
                    error: None,
                    error_kind: None,
                })
            }
        }

        /// A single-blob [`BlobFetcher`] for the provisioning rig: serves `bytes` at
        /// `reference`, verifying the requested `sha256` (the real fetcher's
        /// contract) and counting fetches (to prove store hits skip the fetch).
        struct OneBlob {
            reference: String,
            bytes: Vec<u8>,
            fetches: std::sync::atomic::AtomicUsize,
        }

        impl OneBlob {
            fn new(reference: &str, bytes: Vec<u8>) -> Self {
                Self {
                    reference: reference.to_owned(),
                    bytes,
                    fetches: std::sync::atomic::AtomicUsize::new(0),
                }
            }
            fn fetch_count(&self) -> usize {
                self.fetches.load(std::sync::atomic::Ordering::SeqCst)
            }
        }

        #[async_trait]
        impl BlobFetcher for OneBlob {
            async fn fetch(
                &self,
                reference: &str,
                sha256: &str,
            ) -> Result<Vec<u8>, ProvisioningError> {
                self.fetches
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(reference, self.reference, "unexpected fetch reference");
                assert_eq!(sha256_hex(&self.bytes), sha256, "blob sha mismatch");
                Ok(self.bytes.clone())
            }
        }

        /// Build an executor + provisioning rig over one desired `task-1` entry
        /// (`task-claude` → `CLAUDE.md` with `bytes`), returning the handles the
        /// P1-11 regression tests inspect.
        fn provisioning_rig(
            db: &Arc<Mutex<Db>>,
            ws: &std::path::Path,
            bytes: &[u8],
        ) -> (ThreadExecutor, Arc<Provisioner>, Arc<OneBlob>) {
            let entry = ProvisioningEntry {
                id: "task-claude".to_owned(),
                sha256: sha256_hex(bytes),
                target: "CLAUDE.md".to_owned(),
                kind: EntryKind::File,
            };
            lock(db)
                .set_task_entries("task-1", Some(&serde_json::to_string(&[entry]).unwrap()))
                .unwrap();
            let blob = Arc::new(OneBlob::new(
                "/agent/attachments/task-claude",
                bytes.to_vec(),
            ));
            // Clone the concrete handle, then coerce to the trait object (unsizing
            // happens at the `let`, not inside `Arc::clone`).
            let concrete = Arc::clone(&blob);
            let fetcher: Arc<dyn BlobFetcher> = concrete;
            let provisioner = Arc::new(Provisioner::new(ws.to_path_buf(), Arc::clone(db), fetcher));
            let engine: Arc<dyn Engine> = Arc::new(CwdProbe {
                seen: Arc::new(Mutex::new(None)),
            });
            let exec =
                ThreadExecutor::new(engine, Arc::clone(db), ws.to_path_buf(), no_inactivity())
                    .with_provisioning(Arc::clone(&provisioner));
            (exec, provisioner, blob)
        }

        /// With the provisioning rig wired, a run materializes the task scope into
        /// `work_dir/project/` (scope = task id, P1-11), creates the isolated
        /// `project/workspace/` git area, and launches the engine with cwd=`project/`.
        #[tokio::test]
        async fn provisioned_run_materializes_task_scope_in_project_dir() {
            let db = test_db();
            let ws = tempfile::tempdir().unwrap();

            // Persist the task's desired entries (what `apply_task_def` writes).
            let bytes = b"# task CLAUDE.md\n".to_vec();
            let entry = ProvisioningEntry {
                id: "task-claude".to_owned(),
                sha256: sha256_hex(&bytes),
                target: "CLAUDE.md".to_owned(),
                kind: EntryKind::File,
            };
            lock(&db)
                .set_task_entries("task-1", Some(&serde_json::to_string(&[entry]).unwrap()))
                .unwrap();

            let fetcher: Arc<dyn BlobFetcher> = Arc::new(OneBlob::new(
                "/agent/attachments/task-claude",
                bytes.clone(),
            ));
            let provisioner = Arc::new(Provisioner::new(
                ws.path().to_path_buf(),
                Arc::clone(&db),
                fetcher,
            ));

            let seen = Arc::new(Mutex::new(None));
            let engine: Arc<dyn Engine> = Arc::new(CwdProbe {
                seen: Arc::clone(&seen),
            });
            let exec = ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                ws.path().to_path_buf(),
                no_inactivity(),
            )
            .with_provisioning(provisioner);

            lock(&db)
                .insert_thread(&pending_thread("th-prov", 1))
                .unwrap();
            let final_row = exec
                .run_thread(
                    pending_thread("th-prov", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(final_row.status, "succeeded");

            let work_dir = PathBuf::from(final_row.work_dir.clone().unwrap());
            let project_dir = work_dir.join("project");

            // The engine ran in work_dir/project, NOT work_dir.
            assert_eq!(seen.lock().unwrap().clone().unwrap(), project_dir);
            // The isolated git working area exists (created empty).
            assert!(project_dir.join("workspace").is_dir());
            // The task instruction file is a symlink into the store → the bytes.
            let link = project_dir.join("CLAUDE.md");
            assert!(
                std::fs::symlink_metadata(&link)
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "CLAUDE.md must be a symlink into the store"
            );
            assert_eq!(std::fs::read(&link).unwrap(), bytes);
            // P1-11: nothing is scoped by thread id, and the isolated run's terminal
            // cleanup already released the task scope — the manifest is empty.
            assert!(lock(&db).list_config_entries("th-prov").unwrap().is_empty());
            assert!(lock(&db).list_all_config_entries().unwrap().is_empty());
        }

        /// P1-11 regression (accumulation): two successive isolated fires of the
        /// same task reconcile and then release the SAME task-id scope — no
        /// `config_entries` rows pile up (the old thread-id scoping left one
        /// immortal row set per fire), and the second fire hits the store instead
        /// of re-fetching the blob.
        #[tokio::test]
        async fn successive_isolated_fires_do_not_accumulate_config_entries() {
            let db = test_db();
            let ws = tempfile::tempdir().unwrap();
            let bytes = b"# task CLAUDE.md\n".to_vec();
            let (exec, _prov, blob) = provisioning_rig(&db, ws.path(), &bytes);

            for id in ["th-fire-1", "th-fire-2"] {
                lock(&db).insert_thread(&pending_thread(id, 1)).unwrap();
                let row = exec
                    .run_thread(pending_thread(id, 1), "go".to_owned(), Continuity::Isolated)
                    .await;
                assert_eq!(row.status, "succeeded");
            }

            // No rows survive under any scope — neither task- nor thread-keyed.
            assert!(lock(&db).list_all_config_entries().unwrap().is_empty());
            // The second fire reused the store entry: one fetch across both fires.
            assert_eq!(blob.fetch_count(), 1, "store hit must skip the re-fetch");
        }

        /// P1-11 regression (replace semantics): rows are keyed by TASK id — a
        /// shared thread parks `active` (non-terminal), so its scope survives —
        /// and a later fire of the same task repoints the one row per target
        /// (`UNIQUE(scope, target)`) at its fresh `project/` tree instead of
        /// accumulating a second row set.
        #[tokio::test]
        async fn task_scope_rows_replace_across_fires() {
            let db = test_db();
            let ws = tempfile::tempdir().unwrap();
            let bytes = b"# task CLAUDE.md\n".to_vec();
            let (exec, _prov, _blob) = provisioning_rig(&db, ws.path(), &bytes);

            lock(&db)
                .insert_thread(&pending_thread("th-s1", 1))
                .unwrap();
            let first = exec
                .run_thread(
                    pending_thread("th-s1", 1),
                    "go".to_owned(),
                    Continuity::Shared,
                )
                .await;
            assert_eq!(first.status, "active");
            let rows = lock(&db).list_config_entries("task-1").unwrap();
            assert_eq!(rows.len(), 1, "scope is the task id: one row per target");
            assert!(lock(&db).list_config_entries("th-s1").unwrap().is_empty());
            let first_link = rows[0].link_path.clone();

            lock(&db)
                .insert_thread(&pending_thread("th-s2", 1))
                .unwrap();
            let second = exec
                .run_thread(
                    pending_thread("th-s2", 1),
                    "go".to_owned(),
                    Continuity::Shared,
                )
                .await;
            assert_eq!(second.status, "active");
            let rows = lock(&db).list_config_entries("task-1").unwrap();
            assert_eq!(rows.len(), 1, "second fire replaced, not accumulated");
            assert_ne!(
                rows[0].link_path, first_link,
                "row repointed to the new thread's project tree"
            );
        }

        /// P1-11 regression (terminal cleanup unroots repair + GC): once a thread
        /// is terminal its scope rows are gone, so startup repair does NOT rebuild
        /// the dead thread's symlink tree (no re-fetch either) and a subsequent GC
        /// removes the now-unreferenced store entry.
        #[tokio::test]
        async fn terminal_cleanup_unroots_store_gc_and_repair() {
            let db = test_db();
            let ws = tempfile::tempdir().unwrap();
            let bytes = b"# task CLAUDE.md\n".to_vec();
            let sha = sha256_hex(&bytes);
            let (exec, prov, blob) = provisioning_rig(&db, ws.path(), &bytes);

            lock(&db)
                .insert_thread(&pending_thread("th-dead", 1))
                .unwrap();
            let row = exec
                .run_thread(
                    pending_thread("th-dead", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(row.status, "succeeded");

            // Terminal cleanup released the scope; the store entry is unrooted but
            // survives until GC.
            assert!(lock(&db).list_all_config_entries().unwrap().is_empty());
            let store_path =
                crate::provisioning::store::store_path(ws.path(), "task-claude", &sha).unwrap();
            assert!(store_path.is_file(), "store entry survives until GC");

            // Repair roots on live rows only: a lost link inside the dead thread's
            // project tree is not rebuilt and nothing is re-fetched.
            let link = PathBuf::from(row.work_dir.clone().unwrap()).join("project/CLAUDE.md");
            std::fs::remove_file(&link).unwrap();
            let fetched = blob.fetch_count();
            prov.repair().await.unwrap();
            assert!(
                !link.exists(),
                "repair must not rebuild a dead thread's tree"
            );
            assert_eq!(
                blob.fetch_count(),
                fetched,
                "repair must not re-fetch for a dead scope"
            );

            // GC ages the unreferenced store entry out.
            prov.gc().await.unwrap();
            assert!(!store_path.exists(), "gc removes the unrooted store entry");
        }

        /// Without the provisioning rig the engine cwd stays the `work_dir` itself and
        /// no `project/` dir is created (back-compat — the existing unit/e2e paths).
        #[tokio::test]
        async fn unprovisioned_run_keeps_cwd_at_work_dir() {
            let db = test_db();
            let ws = tempfile::tempdir().unwrap();
            let seen = Arc::new(Mutex::new(None));
            let engine: Arc<dyn Engine> = Arc::new(CwdProbe {
                seen: Arc::clone(&seen),
            });
            let exec = ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                ws.path().to_path_buf(),
                no_inactivity(),
            );

            lock(&db)
                .insert_thread(&pending_thread("th-np", 1))
                .unwrap();
            let final_row = exec
                .run_thread(
                    pending_thread("th-np", 1),
                    "go".to_owned(),
                    Continuity::Isolated,
                )
                .await;
            assert_eq!(final_row.status, "succeeded");

            let work_dir = PathBuf::from(final_row.work_dir.clone().unwrap());
            assert_eq!(seen.lock().unwrap().clone().unwrap(), work_dir);
            assert!(
                !work_dir.join("project").exists(),
                "no project/ without the provisioning rig"
            );
        }

        // -- Shutdown/cancel child-kill path (P1-6) --------------------------

        /// P1-6 regression, against the REAL Claude engine plumbing: an engine
        /// child that ignores the graceful interrupt is killed once the
        /// post-cancel drain window expires — it must not keep executing past the
        /// cancelled turn. Linux-only: child liveness is observed via `/proc`.
        #[cfg(target_os = "linux")]
        #[tokio::test]
        async fn drain_window_expiry_kills_the_engine_child() {
            use std::io::Write;
            use std::os::unix::fs::PermissionsExt;

            use crate::engine::claude::{ClaudeEngine, ClaudeSettings};

            let db = test_db();
            let dir = tempfile::tempdir().unwrap();
            let pid_path = dir.path().join("child.pid");

            // A fake `claude` that announces itself, records its pid, then
            // ignores stdin (including the interrupt) forever.
            let stub = dir.path().join("stubborn-claude.sh");
            let script = format!(
                "#!/usr/bin/env bash\n\
                 echo $$ > {pid}\n\
                 printf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-kill\",\"model\":\"m\",\"tools\":[]}}'\n\
                 exec sleep 600\n",
                pid = pid_path.display()
            );
            {
                let mut file = std::fs::File::create(&stub).unwrap();
                file.write_all(script.as_bytes()).unwrap();
                let mut perms = file.metadata().unwrap().permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&stub, perms).unwrap();
            }

            let engine: Arc<dyn Engine> = Arc::new(ClaudeEngine::new(ClaudeSettings {
                binary: stub,
                ..ClaudeSettings::default()
            }));
            let exec = Arc::new(ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                dir.path().to_path_buf(),
                WatchdogCfg {
                    inactivity_timeout: None,
                    cancel_drain_timeout: std::time::Duration::from_millis(200),
                    ..WatchdogCfg::default()
                },
            ));
            lock(&db)
                .insert_thread(&pending_thread("th-kill", 1))
                .unwrap();

            let runner = Arc::clone(&exec);
            let handle = tokio::spawn(async move {
                runner
                    .run_thread(
                        pending_thread("th-kill", 1),
                        "go".to_owned(),
                        Continuity::Isolated,
                    )
                    .await
            });

            // Wait for the child to announce its pid, then interrupt the run.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let pid = loop {
                if let Ok(text) = std::fs::read_to_string(&pid_path)
                    && let Ok(pid) = text.trim().parse::<u32>()
                {
                    break pid;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "engine child never started"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            };
            exec.interrupt().await;

            // The run settles cancelled within the drain window (hard kill: no
            // resume handle), not after running_timeout.
            let row = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
                .await
                .expect("cancelled run did not settle within the drain window")
                .unwrap();
            assert_eq!(row.status, "cancelled");
            assert!(
                row.engine_session_id.is_none(),
                "a hard-killed turn keeps no resume handle"
            );

            // The child received the kill: it leaves the process table (or sits
            // as a zombie awaiting reap — either way it is no longer executing).
            let dead_by = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let state = std::fs::read_to_string(format!("/proc/{pid}/stat"))
                    .ok()
                    .and_then(|stat| {
                        stat.rsplit(')')
                            .next()
                            .and_then(|tail| tail.trim_start().chars().next())
                    });
                match state {
                    None | Some('Z') => break,
                    Some(_) => {
                        assert!(
                            std::time::Instant::now() < dead_by,
                            "engine child survived the drain-window kill"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                }
            }
        }
    }
}
