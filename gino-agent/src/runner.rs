//! The persistent gino-agent daemon (PLAN-003 §1/§5/§6, PLAN-006 §3 Lane G).
//!
//! [`run_daemon`] wires the whole node agent together on the **protocol-v2** path
//! and serves until shutdown:
//!
//! 1. resolve [`Config`] from bootstrap inputs,
//! 2. load-or-register the runner [`identity`](crate::register),
//! 3. open the reconnecting [`Transport`] (v2 frames, outbox/inbox),
//! 4. build the engine and the v2 [`ThreadExecutor`] (the pending-thread drainer),
//! 5. recover crash-orphan threads from a previous process,
//! 6. start the local [`Scheduler`] (fires crons → pending threads + `cron_state`),
//! 7. start the **executor worker** (drains pending threads, flushes the outbox),
//! 8. **serve**: route inbound v2 frames to the executor / scheduler / config.
//!
//! ## Two planes, one runtime
//!
//! The scheduler (scheduling plane) and the executor (result plane) communicate
//! only through the DB: a fire inserts a `pending` thread and enqueues `cron_state`,
//! then nudges the [`Scheduler::drain_signal`]; the **executor worker** wakes on
//! that nudge (or a bounded poll), drains every `pending` thread via the
//! [`ThreadExecutor`], and pokes the transport's [`flush`](Transport::flush_handle)
//! so the freshly-enqueued result-plane frames stream out over the live socket.
//!
//! The runner is **autonomous and offline-tolerant**: the scheduler keeps firing
//! and the executor keeps draining while the orchestrator is unreachable; result
//! frames persist in the at-least-once outbox and replay on reconnect. The
//! transport drives its own heartbeat and reconnect.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::TimeDelta;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::attachment_source::DbAttachmentSource;
use crate::attachments::{self, AttachmentManager, AttachmentSource};
use crate::config::{Config, ConfigOverlay, ExecutorConfig};
use crate::db::{Db, TaskRow, ThreadRow};
use crate::engine::claude::{ClaudeEngine, ClaudeSettings};
use crate::engine::{Engine, EngineError, Session, SessionCfg};
use crate::executor::{PromptSource, ThreadExecutor, WatchdogCfg};
use crate::protocol::v2::{CronUpsertData, InboundFrameV2, ProvisioningEntry, TaskUpsertData};
use crate::provisioning::{BlobFetcher, Provisioner};
use crate::register::{self, ResolvedIdentity};
use crate::scheduler::{Cancel, Scheduler, apply_cron_delete, apply_cron_upsert, now, to_iso};
use crate::transport::http::HttpClient;
use crate::transport::{Inbound, MAX_INBOX_RETRIES, Transport, TransportConfig};

/// Engine a chat/api thread defaults to (the only engine today; sourced from the
/// task/config when more engines land).
const DEFAULT_ENGINE: &str = "claude";

/// Sub-directory of the workspace dir holding the GLOBAL engine-config scope root
/// (PLAN-007 §1/§6): the runner materializes the global instruction file + skills
/// here and launches the engine with `ENGINE_CONFIG_DIR`/`CLAUDE_CONFIG_DIR`
/// pointed at it.
const ENGINE_CONFIG_SUBDIR: &str = "engine-config";

/// Safety-net cadence for the executor worker when no drain nudge arrives — picks
/// up freshly-pending threads and flushes any outbox rows the host has not yet
/// acked.
const WORKER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Bounded graceful drain at shutdown (P1-6): how long the executor worker gets
/// to settle the interrupted active run (post-interrupt drain, output collection,
/// finalize) before it is aborted and the leftover engine session killed.
/// Comfortably above the executor's in-run `cancel_drain_timeout` and inside
/// systemd's default `TimeoutStopSec` (90 s).
const SHUTDOWN_DRAIN_WINDOW: Duration = Duration::from_secs(15);

/// Lockfile inside the (0700-hardened) workspace dir backing the daemon's
/// single-instance flock.
const DAEMON_LOCK_FILE: &str = "agent.lock";

/// The daemon's single-instance guard (audit P2-18): an exclusive advisory
/// lock (`flock`-style, via [`std::fs::File::try_lock`]) on a lockfile in the
/// state dir, held for the daemon's whole lifetime and released by the OS on
/// any exit, clean or not. A second daemon fails fast instead of racing the
/// first over migrations and thread claims. CLI subcommands never take this
/// lock — they are short-lived readers/writers of the shared WAL DB.
#[derive(Debug)]
struct DaemonLock {
    /// The open lockfile handle; dropping it releases the lock.
    _file: std::fs::File,
}

/// Acquire the exclusive daemon lock, failing fast (no blocking wait) when
/// another process — or another handle in this one — already holds it. The
/// lock is advisory and per open-file-description, so it works across
/// processes and is testable with two handles in one.
fn acquire_daemon_lock(workspace_dir: &Path) -> Result<DaemonLock> {
    let path = workspace_dir.join(DAEMON_LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening daemon lockfile {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(DaemonLock { _file: file }),
        Err(std::fs::TryLockError::WouldBlock) => Err(anyhow::anyhow!(
            "another gino-agent daemon is already running (exclusive lock on {} is held); \
             stop it before starting a new one",
            path.display()
        )),
        Err(std::fs::TryLockError::Error(err)) => {
            Err(err).with_context(|| format!("locking daemon lockfile {}", path.display()))
        }
    }
}

/// Create the DB/workspace directories, then restrict the workspace to
/// owner-only (`0700`): it holds the DB (connect token), engine work dirs and
/// the provisioning store. Hardening runs on every startup so pre-existing
/// world-readable deployments are healed, not only fresh installs.
fn prepare_state_dirs(paths: &crate::config::Paths) -> Result<()> {
    if let Some(parent) = paths.db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating db directory {}", parent.display()))?;
    }
    std::fs::create_dir_all(&paths.workspace_dir).with_context(|| {
        format!(
            "creating workspace directory {}",
            paths.workspace_dir.display()
        )
    })?;
    crate::db::harden_state_dir(&paths.workspace_dir).with_context(|| {
        format!(
            "hardening workspace directory permissions {}",
            paths.workspace_dir.display()
        )
    })?;
    Ok(())
}

/// Build the tokio runtime and drive the daemon to completion. Kept sync so the
/// CLI dispatcher in [`crate`] stays sync.
pub(crate) fn run_daemon(
    host: Option<String>,
    register_token: Option<String>,
    name: Option<String>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;
    runtime.block_on(run_async(host, register_token, name))
}

/// Resolve config → register → connect → start scheduler + executor → serve.
async fn run_async(
    host: Option<String>,
    register_token: Option<String>,
    name: Option<String>,
) -> Result<()> {
    let mut config = Config::default();
    config.apply(ConfigOverlay::bootstrap(host, register_token, name));
    // env/CLI bootstrap (PLAN-003 §3): resolve the workspace/DB paths from
    // `GINO_WORKSPACE_DIR` / `GINO_DB_PATH` *before* opening the DB, so ops — or the
    // e2e harness — can relocate local state off the default mount. Both unset → the
    // documented defaults stand; later host layers may still override per precedence.
    config.apply(ConfigOverlay::paths_bootstrap(
        std::env::var_os("GINO_WORKSPACE_DIR").map(PathBuf::from),
        std::env::var_os("GINO_DB_PATH").map(PathBuf::from),
    ));
    config.validate()?;

    prepare_state_dirs(&config.paths)?;

    // Single-instance guard (P2-18), taken BEFORE the DB open so a second
    // daemon can never run migrations — let alone claim threads — against a
    // live instance's database. Held (via the binding) until run_async returns.
    let _daemon_lock = acquire_daemon_lock(&config.paths.workspace_dir)?;

    let db = Db::open(&config.paths.db_path)
        .with_context(|| format!("opening db at {}", config.paths.db_path.display()))?;
    let http = HttpClient::new().context("building the HTTP client")?;

    let identity = register::load_or_register(&mut config, &db, &http)
        .await
        .context("resolving runner identity")?;
    info!(agent_id = %identity.agent_id, "runner identity resolved");

    // PLAN-007-7 (A): authoritative startup config fetch. On a restart the runner
    // reuses its persisted identity (no second register), so `GET /agent/config` —
    // not the register response — re-supplies the versioned desired set the version
    // gate reads (see [`fetch_and_apply_config`]). Non-fatal / offline-tolerant.
    fetch_and_apply_config(&mut config, &identity, &http).await;

    let ws_url = config.ws_url(&identity.agent_id)?;
    let db = Arc::new(Mutex::new(db));
    let transport_config = TransportConfig {
        heartbeat: Duration::from_millis(config.heartbeat_interval_ms),
        ..TransportConfig::default()
    };
    // The dial happens in the background (P0-2): the daemon boots and serves
    // local work while the host is unreachable; outbound frames queue durably
    // and replay once a connection is up.
    let mut transport = Transport::open(
        ws_url,
        identity.connect_token,
        Arc::clone(&db),
        transport_config,
    );
    info!("transport started; connecting in the background");

    // The GLOBAL provisioning scope root (PLAN-007 §1/§6). Created up front so the
    // engine always launches with a stable config dir — even with no entries.
    let engine_config_dir = config.paths.workspace_dir.join(ENGINE_CONFIG_SUBDIR);
    std::fs::create_dir_all(&engine_config_dir).with_context(|| {
        format!(
            "creating engine config directory {}",
            engine_config_dir.display()
        )
    })?;

    // One shutdown token for every plane (P1-6): the serve loop, the scheduler,
    // the executor worker AND the active engine run (via the executor's converged
    // cancel path) all watch this token, so a shutdown signal cancels an in-flight
    // turn instead of waiting out its running_timeout.
    let cancel = Arc::new(Cancel::new());

    // The v2 execution plane: the source-agnostic pending-thread drainer, its
    // out-of-band attachment plane (PLAN-005, Lane Z1) and the global config/skills
    // provisioner (PLAN-007) — all over one shared content-addressed manager — then
    // the startup global reconcile. A resolved message's inbound descriptors reach
    // the executor through the durable `thread_attachments` snapshot (P1-10b).
    let flush = transport.flush_handle();
    let (executor, provisioner, settings) = build_execution_plane(
        &config,
        &http,
        &db,
        &engine_config_dir,
        &cancel,
        Arc::clone(&flush),
    )
    .await;
    let prompts = Arc::new(InboxPromptSource {
        db: Arc::clone(&db),
    });

    // Crash recovery: any thread left `dispatched`/`running`/`active` by a previous
    // process has no live driver — fail it `runner.runtime_offline` (retryable, so
    // it re-pends and re-runs on this process).
    let recovered = executor.fail_offline(&in_flight_threads(&db));
    if recovered > 0 {
        info!(recovered, "recovered crash-orphan threads at startup");
    }

    // One retry budget for every thread source (cron fires + chat messages).
    let max_attempts = config.executor.max_attempts;
    let scheduler = Scheduler::new(
        Arc::clone(&db),
        Arc::clone(&cancel),
        max_attempts,
        config.retention.days,
    );
    let (nudge, drain) = (scheduler.nudge(), scheduler.drain_signal());
    let scheduler_task = tokio::spawn(scheduler.run());

    let worker = tokio::spawn(run_worker(
        Arc::clone(&executor),
        Arc::clone(&db),
        Arc::clone(&prompts),
        Arc::clone(&drain),
        Arc::clone(&flush),
        Arc::clone(&cancel),
        max_attempts,
    ));
    info!("scheduler + executor running; serving inbound/outbound frames");

    let context = ServeContext {
        executor: Arc::clone(&executor),
        db: Arc::clone(&db),
        nudge,
        drain: Arc::clone(&drain),
        chat_max_attempts: max_attempts,
        provisioner,
        engine_config_dir,
        config: Mutex::new(config),
        engine_settings: settings,
    };
    serve(&mut transport, &context, &cancel).await;

    // Shutdown (P1-6): drain the execution plane within a bounded window (the
    // active run is cancelled, not waited out), stop the scheduler, fail any
    // leftover in-flight threads (so the host sees a terminal result rather than
    // a silent gap), then drain the transport. No engine child survives this.
    shutdown_worker(&cancel, &executor, worker, SHUTDOWN_DRAIN_WINDOW).await;
    if scheduler_task.await.is_err_and(|err| err.is_panic()) {
        warn!("scheduler task panicked during shutdown");
    }
    let failed = executor.fail_offline(&in_flight_threads(&db));
    if failed > 0 {
        info!(failed, "failed in-flight threads on shutdown");
    }
    flush.notify_one(); // best-effort live flush; durable rows replay on next connect
    transport.shutdown().await;
    info!("gino-agent stopped");
    Ok(())
}

/// Stop the execution plane within a bounded drain window (P1-6): trip the
/// shutdown token (stops the worker loop and, through the executor's converged
/// cancel path, the active run), gracefully interrupt the running engine turn
/// (the same [`ThreadExecutor::interrupt`] machinery a host `interrupt` frame
/// uses), then await the worker. A worker that has not settled when the window
/// closes is aborted, and any leftover engine session is dropped — killing the
/// engine child (`kill_on_drop`) — so no engine process survives the daemon.
async fn shutdown_worker(
    cancel: &Cancel,
    executor: &ThreadExecutor,
    mut worker: tokio::task::JoinHandle<()>,
    window: Duration,
) {
    cancel.cancel();
    executor.interrupt().await;
    match tokio::time::timeout(window, &mut worker).await {
        Ok(joined) => {
            if joined.is_err_and(|err| err.is_panic()) {
                warn!("executor worker panicked during shutdown");
            }
        }
        Err(_elapsed) => {
            warn!("executor worker did not settle within the shutdown drain window; aborting it");
            worker.abort();
            let _ = worker.await;
        }
    }
    executor.kill_active().await;
}

/// The executor worker: drains every `pending` thread on each scheduler/route
/// nudge (or a bounded poll), runs a periodic stale-thread reap, then pokes the
/// transport flush so the result-plane frames the drain enqueued stream out.
async fn run_worker(
    executor: Arc<ThreadExecutor>,
    db: Arc<Mutex<Db>>,
    prompts: Arc<InboxPromptSource>,
    drain: Arc<Notify>,
    flush: Arc<Notify>,
    cancel: Arc<Cancel>,
    chat_max_attempts: i64,
) {
    loop {
        // Register drain interest before draining so a fire that lands mid-drain
        // still wakes the next pass.
        let nudged = drain.notified();
        tokio::pin!(nudged);
        nudged.as_mut().enable();
        tokio::select! {
            () = cancel.cancelled() => break,
            () = &mut nudged => {}
            () = tokio::time::sleep(WORKER_POLL_INTERVAL) => {}
        }
        if cancel.is_cancelled() {
            break;
        }

        // Watchdog reap runs between drains — the drainer is serial, so no thread is
        // live here and there is nothing to race. Live runs are bounded by the
        // in-run watchdog inside [`ThreadExecutor::drive`].
        executor.reap_stale(&in_flight_threads(&db));
        drain_with_queued_messages(&executor, &db, prompts.as_ref(), chat_max_attempts).await;
        flush.notify_one();
    }
    debug!("executor worker stopped");
}

/// Drain every `pending` thread, then keep re-pending threads whose inbox holds a
/// queued `message` (one that arrived while the thread was in flight and was only
/// logged "queued") and draining again until no queued message remains — the
/// post-terminal re-scan that closes P0-4 (the second of two quick messages must
/// EXECUTE once the first run finishes, not rot in the inbox). Terminates because
/// every re-pended thread's next run consumes its queued message row.
async fn drain_with_queued_messages(
    executor: &ThreadExecutor,
    db: &Mutex<Db>,
    prompts: &InboxPromptSource,
    chat_max_attempts: i64,
) {
    executor.drain_pending(prompts).await;
    while repend_queued_messages(db, chat_max_attempts) > 0 {
        executor.drain_pending(prompts).await;
    }
}

/// Re-pend every resting thread that still has an unprocessed inbox `message`
/// targeting it (P0-4). An untargeted (absent-`threadId`) row maps to its
/// deterministically minted thread ([`minted_thread_id`]) — recreating it if a
/// crash lost the mint. Threads that are in flight (or already `pending`) are
/// left alone: their message stays queued for the next pass. Cron threads are
/// never re-pended here — their prompt source is `crons.prompt`, so a re-pend
/// would re-run the cron without ever consuming the message. Returns how many
/// threads were made `pending`.
fn repend_queued_messages(db: &Mutex<Db>, chat_max_attempts: i64) -> usize {
    let rows = match lock_db(db).list_inbox_by_type("message") {
        Ok(rows) => rows,
        Err(err) => {
            warn!(error = %err, "failed to scan inbox for queued messages");
            return 0;
        }
    };
    let mut pended = 0;
    for row in rows {
        let Some(task_id) = row.task_id.clone() else {
            continue;
        };
        let tid = row
            .thread_id
            .clone()
            .unwrap_or_else(|| minted_thread_id(&task_id, row.id));
        let is_cron_thread = matches!(
            lock_db(db).get_thread(&tid),
            Ok(Some(thread)) if thread.source == "cron"
        );
        if is_cron_thread {
            debug!(thread = %tid, inbox = row.id, "queued message targets a cron thread; leaving it");
            continue;
        }
        if ensure_chat_thread(db, &task_id, &tid, chat_max_attempts) {
            pended += 1;
        }
    }
    pended
}

/// The in-flight threads (`dispatched` + `running`) — the crash-orphan recovery /
/// watchdog candidate set (agent-architecture §5). A `pending` thread re-runs on
/// its own; an `active` thread is *resting* between shared-turn runs (not in
/// flight), so it is deliberately excluded — failing it would spuriously re-run it.
fn in_flight_threads(db: &Mutex<Db>) -> Vec<ThreadRow> {
    let guard = lock_db(db);
    let mut rows = guard
        .list_threads_by_status("dispatched")
        .unwrap_or_default();
    rows.extend(guard.list_threads_by_status("running").unwrap_or_default());
    rows
}

/// Resolves engine prompts for the drainer. This is consulted **only at attempt 0
/// for a thread with no snapshot** — the executor reuses `threads.prompt` for cron
/// threads (snapshotted at fire time) and for every retry/resume (D10). A `cron`
/// thread runs its trigger's `crons.prompt` (the recurring instruction, D10:(a) —
/// no longer the v1 `tasks.prompt` shim); a `chat`/`api` thread runs the text of
/// its oldest unprocessed inbox `message`, which it **consumes** (marks processed)
/// so the row neither replays nor re-resolves. `None` (no runnable prompt) fails
/// the thread `runner.invalid_request`.
struct InboxPromptSource {
    db: Arc<Mutex<Db>>,
}

impl PromptSource for InboxPromptSource {
    fn prompt_for(&self, thread: &ThreadRow) -> Option<String> {
        match thread.source.as_str() {
            // A cron thread's instruction is its trigger's crons.prompt (D10:(a)).
            "cron" => thread
                .cron_id
                .as_deref()
                .and_then(|cron_id| lock_db(&self.db).get_cron(cron_id).ok().flatten())
                .map(|cron| cron.prompt)
                .filter(|prompt| !prompt.trim().is_empty()),
            _ => resolve_inbox_message(&self.db, thread),
        }
    }
}

/// Find, consume, and return the text of the oldest unprocessed inbox `message`
/// for `thread` (matched by `threadId`, falling back to `taskId` for an untargeted
/// chat message). Consuming it (mark processed) prevents both a replay re-run and a
/// re-resolve; an empty message yields `None` (which fails the thread) but is still
/// consumed so it cannot loop.
///
/// For a runnable message this also snapshots its inbound attachment descriptors
/// into the durable `thread_attachments` table (P1-10b), BEFORE consuming the row:
/// once processed the message is invisible to every later query, so a
/// consumed-but-unsnapshotted crash window would silently strip the thread of its
/// inputs. Snapshotted only when the prompt is non-empty, so a failed (unrunnable)
/// thread — which never reaches materialization — leaves no orphan row; on a
/// snapshot write failure the message is left unconsumed (a later resolve retries)
/// and the resolve fails rather than running without the inputs.
fn resolve_inbox_message(db: &Mutex<Db>, thread: &ThreadRow) -> Option<String> {
    let guard = lock_db(db);
    let rows = guard.list_inbox_by_type("message").ok()?;
    let row = rows.into_iter().find(|row| {
        row.thread_id.as_deref() == Some(thread.id.as_str())
            || (row.thread_id.is_none() && row.task_id.as_deref() == Some(thread.task_id.as_str()))
    })?;
    let message = match serde_json::from_str::<InboundFrameV2>(&row.payload) {
        Ok(InboundFrameV2::Message { data, .. }) if !data.text.trim().is_empty() => Some(data),
        _ => None,
    };
    if let Some(data) = &message
        && let Some(attachments) = data.attachments.as_ref().filter(|a| !a.is_empty())
        && let Err(err) = guard.set_thread_attachments(&thread.id, attachments)
    {
        warn!(thread = %thread.id, error = %err, "failed to snapshot inbound attachments; leaving the message unconsumed");
        return None;
    }
    if let Err(err) = guard.mark_inbox_processed(row.id) {
        warn!(id = row.id, error = %err, "failed to consume inbox message");
    }
    message.map(|data| data.text)
}

/// Map the [`ExecutorConfig`] knobs onto the executor's watchdog thresholds. A
/// zero `inactivity_timeout_secs` disables the in-run inactivity watchdog only —
/// the total-time watchdog still bounds a silent engine on wall clock (P2-19),
/// so the serial plane can never wedge indefinitely.
fn watchdog_cfg(cfg: &ExecutorConfig) -> WatchdogCfg {
    WatchdogCfg {
        dispatched_timeout: TimeDelta::seconds(cfg.dispatched_timeout_secs),
        running_timeout: TimeDelta::seconds(cfg.running_timeout_secs),
        inactivity_timeout: (cfg.inactivity_timeout_secs > 0)
            .then(|| Duration::from_secs(cfg.inactivity_timeout_secs)),
        ..WatchdogCfg::default()
    }
}

/// PLAN-007-7 (A): fetch the authoritative config from `GET /agent/config/:id` and
/// merge it onto `config`. The payload is the same host-assigned overlay the register
/// response carries (engine credentials, limits, the versioned global provisioning
/// set, …); fetching it at startup is what lets a **restart** re-acquire that config
/// without a second registration. Non-fatal and offline-tolerant: an unbuildable URL,
/// a transport/HTTP failure, or an unparseable payload is logged and the prior
/// (register/bootstrap) config stands — and `config.config_version` stays `None`,
/// which the reconcile version gate treats as non-authoritative (repair-only, never
/// wipe).
async fn fetch_and_apply_config(
    config: &mut Config,
    identity: &ResolvedIdentity,
    http: &HttpClient,
) {
    let url = match config.config_url(&identity.agent_id) {
        Ok(url) => url,
        Err(err) => {
            debug!(error = %err, "no config url; skipping startup config fetch");
            return;
        }
    };
    let mut value = match http.fetch_config(&url, &identity.connect_token).await {
        Ok(value) => value,
        Err(err) => {
            warn!(error = %err, "startup config fetch failed; keeping register/bootstrap config (offline-tolerant)");
            return;
        }
    };
    // The config refresh may echo `protocolVersion` (as the register response does);
    // strip it so the strict (`deny_unknown_fields`) overlay parse stays happy.
    // Protocol negotiation already happened at register; the overlay's own `version`
    // field is the unrelated GLOBAL config version the reconcile gate reads.
    if let Some(obj) = value.as_object_mut() {
        obj.remove("protocolVersion");
    }
    match serde_json::from_value::<ConfigOverlay>(value) {
        Ok(overlay) => {
            info!(version = ?overlay.version(), "startup config fetched and applied");
            config.apply(overlay);
        }
        Err(err) => {
            warn!(error = %err, "startup config payload did not parse; keeping register/bootstrap config");
        }
    }
}

/// Build the full execution plane over ONE shared content-addressed manager
/// (PLAN-007 §2): the [`ThreadExecutor`] (+ its attachment plane), the global
/// config/skills [`Provisioner`], and the startup global reconcile. The manager is
/// built once and shared between both planes so bytes are fetched + cached once; a
/// `None` manager (absent host base / connect token) degrades both planes off. The
/// returned provisioner is `None` in that same degraded case. The startup reconcile
/// (repair → reconcile(global) → gc) is non-fatal — see [`reconcile_global_startup`].
/// Also returns the live engine-settings seam (audit P2-19) the executor's engine
/// snapshots at every spawn, so the `config_update` route can replace its contents.
async fn build_execution_plane(
    config: &Config,
    http: &HttpClient,
    db: &Arc<Mutex<Db>>,
    engine_config_dir: &Path,
    shutdown: &Arc<Cancel>,
    flush: Arc<Notify>,
) -> (
    Arc<ThreadExecutor>,
    Option<Arc<Provisioner>>,
    SharedEngineSettings,
) {
    let settings: SharedEngineSettings =
        Arc::new(Mutex::new(engine_settings(config, engine_config_dir)));
    let manager = build_attachment_manager(config, http);

    // The shared content-addressed provisioner over the manager (`impl BlobFetcher
    // for AttachmentManager`), reusing the SAME cache as the attachment plane. Built
    // before the executor so the one handle drives BOTH the global startup reconcile
    // (below) and the per-thread task scope (wired into the executor next).
    let provisioner = manager.clone().map(|manager| {
        let fetcher: Arc<dyn BlobFetcher> = manager;
        Arc::new(Provisioner::new(
            config.paths.workspace_dir.clone(),
            Arc::clone(db),
            fetcher,
        ))
    });

    let executor = build_executor(
        config,
        Arc::clone(db),
        &settings,
        manager,
        provisioner.clone(),
        Arc::clone(shutdown),
        flush,
    );

    if let Some(provisioner) = &provisioner {
        reconcile_global_startup(
            provisioner,
            db,
            engine_config_dir,
            &config.entries,
            config.config_version,
        )
        .await;
    }
    (executor, provisioner, settings)
}

/// Build the v2 [`ThreadExecutor`] and (when present) wire its out-of-band
/// attachment plane (PLAN-005, Lane Z1) onto the shared `manager`. The inbound
/// descriptors reach the executor through a [`DbAttachmentSource`] over the
/// durable `thread_attachments` snapshot the prompt drainer writes (the message
/// is consumed before materialization, and retries/restarts must resolve the
/// same set — P1-10b; see [`crate::attachment_source`]). The plane is non-fatal:
/// a `None` manager (absent host base / connect token) leaves attachments off
/// and the executor behaves as before.
fn build_executor(
    config: &Config,
    db: Arc<Mutex<Db>>,
    settings: &SharedEngineSettings,
    manager: Option<Arc<AttachmentManager>>,
    provisioner: Option<Arc<Provisioner>>,
    shutdown: Arc<Cancel>,
    flush: Arc<Notify>,
) -> Arc<ThreadExecutor> {
    let engine: Arc<dyn Engine> = Arc::new(ReloadableEngine {
        settings: Arc::clone(settings),
    });
    // The daemon shutdown token (P1-6): a shutdown cancels the active run through
    // the executor's converged cancel path and stops the pending drain. The
    // transport flush signal (P2-16): the executor pokes it after each persisted
    // frame so transcript events stream mid-run over the live socket.
    let mut executor = ThreadExecutor::new(
        engine,
        Arc::clone(&db),
        config.paths.workspace_dir.clone(),
        watchdog_cfg(&config.executor),
    )
    .with_shutdown(shutdown)
    .with_flush(flush);
    if let Some(manager) = manager {
        info!("attachment plane active (host-proxy byte backend)");
        let source: Arc<dyn AttachmentSource> = Arc::new(DbAttachmentSource::new(db));
        executor = executor.with_attachments(manager, source);
    } else {
        debug!("attachment plane disabled (no host base / connect token)");
    }
    // Per-thread task provisioning (PLAN-007 §3/§6): materialize the task scope
    // into `work_dir/project/` and run the engine there. Shares the same handle
    // (and cache) as the global startup reconcile; `None` keeps cwd = `work_dir`.
    if let Some(provisioner) = provisioner {
        info!("task provisioning plane active (per-thread project/ scope)");
        executor = executor.with_provisioning(provisioner);
    } else {
        debug!("task provisioning plane disabled (no host base / connect token)");
    }
    Arc::new(executor)
}

/// Resolve the Claude engine settings from the config, exporting the workspace
/// dir (`GINO_WORKSPACE_DIR`, read by the PreCompact `archive` hook subprocess)
/// and pointing the engine at the provisioned global config dir (PLAN-007 §6) on
/// top of the credential env. Rebuilt on a live `config_update` (audit P2-19) so
/// a rotated credential reaches the next engine run without a restart.
fn engine_settings(config: &Config, engine_config_dir: &Path) -> ClaudeSettings {
    let mut settings = ClaudeSettings::from_config(config);
    settings.env.push((
        "GINO_WORKSPACE_DIR".to_owned(),
        config.paths.workspace_dir.display().to_string(),
    ));
    // Launch the engine reading the global instruction file + skills the runner
    // materialized; `ClaudeSettings` exports `ENGINE_CONFIG_DIR`/`CLAUDE_CONFIG_DIR`.
    settings.config_dir = Some(engine_config_dir.to_path_buf());
    settings
}

/// The live engine-settings seam (audit P2-19): the executor's engine snapshots
/// these settings at every spawn, and the `config_update` route replaces them
/// after merging the overlay onto the live [`Config`] — so a rotated
/// `anthropicApiKey` takes effect without a daemon restart.
type SharedEngineSettings = Arc<Mutex<ClaudeSettings>>;

/// An [`Engine`] over the [`SharedEngineSettings`] seam: each call snapshots the
/// current settings and delegates to a [`ClaudeEngine`] built from that snapshot.
/// Decision (protocol.md leaves `config_update` apply semantics open):
/// APPLY-ON-NEXT-RUN — a session already spawned keeps the environment it was
/// born with; only the next spawn reads the updated settings. A mid-flight run
/// is never hot-swapped.
#[derive(Debug)]
struct ReloadableEngine {
    settings: SharedEngineSettings,
}

#[async_trait]
impl Engine for ReloadableEngine {
    fn name(&self) -> &'static str {
        "claude"
    }

    async fn start_session(&self, cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
        self.snapshot().start_session(cfg).await
    }
}

impl ReloadableEngine {
    /// A [`ClaudeEngine`] over the settings as they stand right now.
    fn snapshot(&self) -> ClaudeEngine {
        ClaudeEngine::new(lock(&self.settings).clone())
    }
}

/// Build the shared content-addressed [`AttachmentManager`] (PLAN-005, agent-
/// architecture §9; unified with PLAN-007 §2): the config-selected byte backend
/// ([`attachments::build_backend`] — host-proxy by default) over a workspace-scoped
/// cache. ONE manager backs both the executor's attachment plane and the config/
/// skills [`Provisioner`], so bytes are fetched and cached once. Returns `None`
/// (both planes degrade off) when the host base or connect token the byte backend
/// needs is unavailable; post-registration both are present, so it is `Some`.
fn build_attachment_manager(config: &Config, http: &HttpClient) -> Option<Arc<AttachmentManager>> {
    let connect_token = config.connect_token.clone()?;
    let http_base = match config.http_base() {
        Ok(base) => base.to_owned(),
        Err(err) => {
            debug!(error = %err, "no host base; attachment/provisioning planes disabled");
            return None;
        }
    };
    let backend =
        attachments::build_backend(&config.attachments, http.clone(), http_base, connect_token);
    let manager = AttachmentManager::new(
        backend,
        &config.paths.workspace_dir,
        config.attachments.clone(),
    );
    Some(Arc::new(manager))
}

/// Run the **version-gated** startup global provisioning sequence (PLAN-007 §6/§7).
///
/// The host owns a monotonic GLOBAL config `version` scoping the desired `entries`
/// set; the runner reconciles the global scope ONLY when that version advances past
/// the persisted applied version. This is the restart-durability fix: on a restart
/// the runner reuses its persisted identity (no second register), so the register
/// response no longer carries the desired set — but the §A startup fetch re-supplies
/// the SAME version, which now takes the repair-only path instead of the old
/// unconditional `reconcile("global", &config.entries)` that wiped every global
/// symlink + row (because `config.entries` was empty on a restart) and then let `gc`
/// collect the orphaned store.
///
/// * **authoritative new** — `fetched` is `Some` and `> applied` (incl. the first
///   versioned set, when `applied` is `None`): reconcile the global scope to
///   `entries` — add / repoint / **delete** to match the desired set, *including
///   removing all when `entries` is empty* (the authoritative-empty case: a versioned
///   empty set means the host genuinely wants none). The applied version is persisted
///   only after a SUCCESSFUL reconcile, so a transient failure re-reconciles on the
///   next restart rather than being masked as already-applied.
/// * **same version** (`fetched == applied`) or **non-authoritative** (`fetched` is
///   `None` — no version served / fetch failed / offline): NEVER reconcile-to-empty.
///   The installed set is left intact.
///
/// `repair` and `gc` run in **every** branch — `repair` recreates symlinks a restart
/// lost from the persisted manifest (the only self-heal in the repair-only branches),
/// and `gc` refcounts the whole store against the manifest (safe everywhere: a
/// repair-only branch leaves rows intact, so nothing referenced is collected). Each
/// step is non-fatal — a failure is logged and the daemon proceeds.
async fn reconcile_global_startup(
    provisioner: &Provisioner,
    db: &Mutex<Db>,
    engine_config_dir: &Path,
    entries: &[ProvisioningEntry],
    fetched_version: Option<i64>,
) {
    let applied = lock_db(db)
        .get_applied_global_version()
        .unwrap_or_else(|err| {
            warn!(error = %err, "failed to read applied global config version; treating as unset");
            None
        });
    // Authoritative-new ⇔ a version is served AND it advances past what we applied
    // (an absent applied version is the never-reconciled / fresh-DB case).
    let authoritative_new =
        matches!(fetched_version, Some(fetched) if applied.is_none_or(|a| fetched > a));

    let mut reconciled_ok = false;
    if authoritative_new {
        match provisioner
            .reconcile("global", engine_config_dir, entries)
            .await
        {
            Ok(()) => {
                reconciled_ok = true;
                info!(
                    entries = entries.len(),
                    version = ?fetched_version,
                    "global provisioning reconciled to new config version"
                );
            }
            Err(err) => {
                warn!(error = %err, "startup global reconcile failed (continuing; version not marked applied)");
            }
        }
    } else {
        // The restart-durability invariant: same version (or no authoritative version
        // at all) ⇒ keep the installed set; repair below still self-heals a lost link.
        debug!(
            applied = ?applied,
            fetched = ?fetched_version,
            "global config version not advanced; repair-only (no reconcile, store-durable)"
        );
    }

    if let Err(err) = provisioner.repair().await {
        warn!(error = %err, "startup provisioning repair failed (continuing)");
    }

    // Persist the applied version only after a successful reconcile of the new set, so
    // a subsequent restart at the same version takes the repair-only path.
    if reconciled_ok
        && let Some(fetched) = fetched_version
        && let Err(err) = lock_db(db).set_applied_global_version(fetched)
    {
        warn!(error = %err, "failed to persist applied global config version");
    }

    if let Err(err) = provisioner.gc().await {
        warn!(error = %err, "startup provisioning gc failed (continuing)");
    }
}

/// Shared handles the serve loop hands to the inbound router.
struct ServeContext {
    executor: Arc<ThreadExecutor>,
    db: Arc<Mutex<Db>>,
    /// Wakes the scheduler after a trigger-definition change.
    nudge: Arc<Notify>,
    /// Wakes the executor worker after a chat message creates a pending thread.
    drain: Arc<Notify>,
    /// Retry budget stamped on executor-created (chat) threads.
    chat_max_attempts: i64,
    /// Global config/skills provisioner (PLAN-007); `None` when the byte backend
    /// is absent, in which case `config_update` provisioning is a no-op.
    provisioner: Option<Arc<Provisioner>>,
    /// The global scope root the provisioner materializes into (= the engine's
    /// `ENGINE_CONFIG_DIR`), re-reconciled live on `config_update`.
    engine_config_dir: PathBuf,
    /// The merged live config. A WS `config_update` overlay lands here (the last
    /// layer of the config.rs precedence chain) and drives the engine-settings
    /// rebuild below.
    config: Mutex<Config>,
    /// The live engine-settings seam (audit P2-19): replaced on `config_update`,
    /// snapshotted by [`ReloadableEngine`] at every engine spawn.
    engine_settings: SharedEngineSettings,
}

/// Resolve when a daemon shutdown signal arrives (P1-6): SIGINT (Ctrl-C) and —
/// on unix — SIGTERM (what systemd `stop`/`restart` sends) are handled
/// identically. A listener that cannot be installed is logged and disabled;
/// the other signal and the internal cancel/`close` paths still shut down.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to listen for SIGINT; that signal cannot trigger shutdown");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    () = ctrl_c => {}
                    _ = term.recv() => {}
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to listen for SIGTERM; that signal cannot trigger shutdown");
                ctrl_c.await;
            }
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

/// Pump inbound v2 frames until a shutdown signal or a `close`. Outbound frames
/// are produced straight into the durable outbox by the executor/scheduler and
/// shipped by the transport (flush/replay), so the serve loop has no outbound
/// branch. Transport loss never ends the loop (P0-2): the manager reconnects
/// forever, and even a dead manager task only stops the frame pump — the
/// daemon (scheduler + executor) keeps serving local work until shutdown.
async fn serve(transport: &mut Transport, context: &ServeContext, cancel: &Cancel) {
    loop {
        tokio::select! {
            biased;
            () = shutdown_signal() => {
                info!("shutdown signal received");
                break;
            }
            () = cancel.cancelled() => break,
            inbound = transport.recv() => {
                let Some(inbound) = inbound else {
                    // Reconnect never exhausts, so a closed channel means the
                    // manager task itself died. Local execution must outlive
                    // the transport: keep running, wait for a real shutdown.
                    warn!("transport channel closed; daemon keeps serving local work");
                    wait_for_shutdown(cancel).await;
                    break;
                };
                if route(inbound, context).await.is_break() {
                    info!("close received; shutting down");
                    break;
                }
            }
        }
    }
}

/// Block until a shutdown signal or an internal cancel — the tail of [`serve`]
/// once the transport channel has closed (P0-2: transport death must not shut
/// the daemon down; the scheduler and executor keep running until a real
/// shutdown).
async fn wait_for_shutdown(cancel: &Cancel) {
    tokio::select! {
        () = shutdown_signal() => {
            info!("shutdown signal received");
        }
        () = cancel.cancelled() => {}
    }
}

/// Route one inbound v2 frame to the executor / scheduler / config, then settle
/// its inbox row: a successfully applied frame is marked processed; a FAILED
/// apply leaves the row unprocessed (it replays on reconnect) and bumps its
/// processing-failure counter, so the poison policy counts real failures — never
/// reconnects (P1-12/P2-19). Returns [`ControlFlow::Break`] for a `close`.
///
/// A `message` is the one frame **not** settled here: its text is the chat
/// thread's prompt, consumed (and marked) later by the [`InboxPromptSource`].
async fn route(inbound: Inbound, context: &ServeContext) -> ControlFlow<()> {
    let Inbound { inbox_id, frame } = inbound;
    let defer_processing = matches!(frame, InboundFrameV2::Message { .. });
    let mut applied = true;
    let flow = match frame {
        InboundFrameV2::Message {
            task_id, thread_id, ..
        } => {
            // Idempotent thread minting (P0-3): an absent `threadId` derives the
            // thread id from the inbox row, so a reconnect replay of the same row
            // finds the already-minted thread instead of racing a fresh one.
            let tid = thread_id.unwrap_or_else(|| minted_thread_id(&task_id, inbox_id));
            ensure_chat_thread(&context.db, &task_id, &tid, context.chat_max_attempts);
            context.drain.notify_one();
            ControlFlow::Continue(())
        }
        InboundFrameV2::Interrupt { .. } => {
            context.executor.interrupt().await;
            ControlFlow::Continue(())
        }
        InboundFrameV2::TaskUpsert { task_id, data, .. } => {
            applied = apply_task_def(&context.db, &task_id, &data);
            ControlFlow::Continue(())
        }
        InboundFrameV2::TaskDelete { task_id, .. } => {
            applied = delete_task(&context.db, &task_id);
            ControlFlow::Continue(())
        }
        InboundFrameV2::CronUpsert {
            task_id,
            cron_id,
            data,
            ..
        } => {
            applied = handle_cron_upsert(&context.db, &task_id, &cron_id, &data, &context.nudge);
            ControlFlow::Continue(())
        }
        InboundFrameV2::CronDelete { cron_id, .. } => {
            applied = apply_trigger(
                || apply_cron_delete(&context.db, &cron_id),
                "cron_delete",
                &context.nudge,
            );
            ControlFlow::Continue(())
        }
        InboundFrameV2::ConfigUpdate { data, .. } => {
            debug!(keys = ?config_keys(&data), "config_update received");
            // Applied on both planes (audit P2-19): the overlay merges onto the
            // live config + engine settings (rotated credentials reach the next
            // engine run), and a re-pushed global provisioning set is reconciled
            // live (PLAN-007 §6). Always marked processed: the host re-pushes
            // config on connect, so a replay is worthless.
            apply_config_update(context, &data).await;
            ControlFlow::Continue(())
        }
        InboundFrameV2::Close { .. } => ControlFlow::Break(()),
        // The transport intercepts `ack`; an unknown frame is forward-compat noise.
        InboundFrameV2::Ack { .. } | InboundFrameV2::Unknown => ControlFlow::Continue(()),
    };
    if !defer_processing {
        conclude_inbox_row(&context.db, inbox_id, applied);
    }
    flow
}

/// Settle a routed inbox row. Applied → processed. Failed → left unprocessed
/// (replayed on reconnect) with its processing-failure counter bumped; once the
/// counter reaches [`MAX_INBOX_RETRIES`] the row is discarded (marked processed)
/// so a permanently failing frame cannot replay forever.
fn conclude_inbox_row(db: &Mutex<Db>, inbox_id: i64, applied: bool) {
    let guard = lock_db(db);
    if applied {
        if let Err(err) = guard.mark_inbox_processed(inbox_id) {
            warn!(inbox = inbox_id, error = %err, "failed to mark inbox row processed");
        }
        return;
    }
    match guard.increment_inbox_retry(inbox_id) {
        Ok(failures) if failures >= MAX_INBOX_RETRIES => {
            warn!(
                inbox = inbox_id,
                failures, "inbox row failed too many times; discarding (poison)"
            );
            if let Err(err) = guard.mark_inbox_processed(inbox_id) {
                warn!(inbox = inbox_id, error = %err, "failed to discard poison inbox row");
            }
        }
        Ok(failures) => {
            debug!(
                inbox = inbox_id,
                failures, "inbox row apply failed; left unprocessed for replay"
            );
        }
        Err(err) => {
            warn!(inbox = inbox_id, error = %err, "failed to record inbox processing failure");
        }
    }
}

/// Apply a host `cron_upsert`, seeding a minimal task row first when the cron
/// arrives before its `task_upsert` (P2-19: the ordering must not drop the cron —
/// the later `task_upsert` patches the seeded row in place).
fn handle_cron_upsert(
    db: &Mutex<Db>,
    task_id: &str,
    cron_id: &str,
    data: &CronUpsertData,
    nudge: &Notify,
) -> bool {
    if !seed_task_if_missing(&lock_db(db), task_id) {
        return false;
    }
    apply_trigger(
        || apply_cron_upsert(db, task_id, cron_id, data),
        "cron_upsert",
        nudge,
    )
}

/// The deterministic thread id for an absent-`threadId` `message` (P0-3): derived
/// from the inbox row that carries the message, so every delivery of that row —
/// original or reconnect replay — resolves to the SAME thread instead of minting a
/// fresh UUID each time (N reconnects must not race N+1 threads for one inbox
/// row). The `task_id@` prefix is kept so the id shape matches host-minted ones.
fn minted_thread_id(task_id: &str, inbox_id: i64) -> String {
    format!("{task_id}@in-{inbox_id}")
}

/// Seed a minimal `tasks` row when `task_id` is unknown (satisfies the
/// `→ tasks(id)` FK for frames that may arrive before their `task_upsert`; the
/// later `task_upsert` patches the seeded row in place). Returns `false` only on
/// a DB error.
fn seed_task_if_missing(db: &Db, task_id: &str) -> bool {
    match db.get_task_row(task_id) {
        Ok(Some(_)) => true,
        Ok(None) => {
            let now_iso = to_iso(now());
            let task = TaskRow {
                id: task_id.to_owned(),
                name: None,
                kind: None,
                status: "active".to_owned(),
                created_at: now_iso.clone(),
                updated_at: now_iso,
            };
            match db.insert_task_row(&task) {
                Ok(()) => true,
                Err(err) => {
                    warn!(task = %task_id, error = %err, "failed to seed task row");
                    false
                }
            }
        }
        Err(err) => {
            warn!(task = %task_id, error = %err, "task lookup failed");
            false
        }
    }
}

/// Ensure the chat thread `tid` exists and is runnable for a `message` (host-v2
/// §M4 threading). A host-supplied `threadId` CONTINUES that thread; an absent
/// one arrives here as the [`minted_thread_id`] derived from the message's inbox
/// row, so the call is idempotent across replays (P0-3). A resting
/// (`active`/`succeeded`/`failed`/`cancelled`) thread is re-set `pending` for the
/// new turn, KEEPING its `engine_session_id` so the engine conversation resumes;
/// an in-flight (or already `pending`) thread is left as-is — the message waits in
/// the inbox and [`repend_queued_messages`] re-pends the thread once it settles
/// (P0-4). An unknown `tid` creates a fresh thread with no carried
/// `engine_session_id`, so the outbound `thread_result`/`thread_message` carry it
/// back for the host to record as `current_thread_id`. Returns `true` when this
/// call made the thread `pending` (re-pended or created).
fn ensure_chat_thread(db: &Mutex<Db>, task_id: &str, tid: &str, max_attempts: i64) -> bool {
    let guard = lock_db(db);
    match guard.get_thread(tid) {
        Ok(Some(existing)) => {
            if matches!(
                existing.status.as_str(),
                "active" | "succeeded" | "failed" | "cancelled"
            ) {
                let mut turn = existing;
                "pending".clone_into(&mut turn.status);
                turn.attempt = 0;
                turn.failure_reason = None;
                turn.started_at = None;
                turn.finished_at = None;
                turn.duration_ms = None;
                turn.result = None;
                turn.error = None;
                // A new turn snapshots its OWN message: clear the prior snapshot so
                // the executor re-resolves this turn's `message.text` at attempt 0
                // (D10). Retries within the turn then reuse the fresh snapshot.
                turn.prompt = String::new();
                match guard.update_thread(&turn) {
                    Ok(()) => true,
                    Err(err) => {
                        warn!(thread = %tid, error = %err, "failed to re-pend chat thread");
                        false
                    }
                }
            } else {
                debug!(thread = %tid, status = %existing.status, "chat message for an in-flight thread; queued");
                false
            }
        }
        Ok(None) => {
            // A chat message may arrive before its `task_upsert`.
            seed_task_if_missing(&guard, task_id);
            let row = ThreadRow {
                id: tid.to_owned(),
                task_id: task_id.to_owned(),
                cron_id: None,
                source: "chat".to_owned(),
                // No snapshot yet: the executor resolves and snapshots this turn's
                // inbox `message.text` into threads.prompt at attempt 0 (D10).
                prompt: String::new(),
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
                created_at: to_iso(now()),
            };
            match guard.insert_thread(&row) {
                Ok(()) => true,
                Err(err) => {
                    warn!(thread = %tid, error = %err, "failed to create chat thread");
                    false
                }
            }
        }
        Err(err) => {
            warn!(thread = %tid, error = %err, "chat thread lookup failed");
            false
        }
    }
}

/// Apply a host `task_upsert` to the v2 `tasks` entity: insert when new, patch the
/// supplied fields in place when known. Returns `false` on a DB failure so the
/// router leaves the inbox row unprocessed for replay.
fn apply_task_def(db: &Mutex<Db>, task_id: &str, data: &TaskUpsertData) -> bool {
    let guard = lock_db(db);
    let now_iso = to_iso(now());
    match guard.get_task_row(task_id) {
        Ok(Some(mut row)) => {
            if let Some(name) = &data.name {
                row.name = Some(name.clone());
            }
            if let Some(kind) = &data.kind {
                row.kind = Some(kind.clone());
            }
            if let Some(status) = &data.status {
                row.status.clone_from(status);
            }
            row.updated_at = now_iso;
            if let Err(err) = guard.update_task_row(&row) {
                warn!(task = %task_id, error = %err, "failed to update task");
                return false;
            }
            persist_task_entries(&guard, task_id, data)
        }
        Ok(None) => {
            let row = TaskRow {
                id: task_id.to_owned(),
                name: data.name.clone(),
                kind: data.kind.clone(),
                status: data.status.clone().unwrap_or_else(|| "active".to_owned()),
                created_at: now_iso.clone(),
                updated_at: now_iso,
            };
            if let Err(err) = guard.insert_task_row(&row) {
                warn!(task = %task_id, error = %err, "failed to insert task");
                return false;
            }
            persist_task_entries(&guard, task_id, data)
        }
        Err(err) => {
            warn!(task = %task_id, error = %err, "task lookup failed");
            false
        }
    }
}

/// Persist a `task_upsert`'s provisioning entries onto the `tasks` row (PLAN-007
/// §5). Present → serialized to the `entries` JSON column for the per-thread
/// materialize ([`ThreadExecutor::with_provisioning`]); absent (`None`) → left
/// untouched, so a `task_upsert` that omits `entries` never wipes a prior set.
/// Returns `false` on a serialize/DB failure.
fn persist_task_entries(db: &Db, task_id: &str, data: &TaskUpsertData) -> bool {
    let Some(entries) = &data.entries else {
        return true;
    };
    match serde_json::to_string(entries) {
        Ok(json) => match db.set_task_entries(task_id, Some(&json)) {
            Ok(()) => true,
            Err(err) => {
                warn!(task = %task_id, error = %err, "failed to persist task entries");
                false
            }
        },
        Err(err) => {
            warn!(task = %task_id, error = %err, "failed to serialize task entries");
            false
        }
    }
}

/// Apply a host `task_delete`: there is no hard delete, so the task is marked
/// `cancelled` (its triggers stop being seeded by their own `cron_delete`). An
/// unknown task is a no-op success (nothing to replay); only a DB failure
/// returns `false`.
fn delete_task(db: &Mutex<Db>, task_id: &str) -> bool {
    let guard = lock_db(db);
    match guard.get_task_row(task_id) {
        Ok(Some(mut row)) => {
            "cancelled".clone_into(&mut row.status);
            row.updated_at = to_iso(now());
            match guard.update_task_row(&row) {
                Ok(()) => true,
                Err(err) => {
                    warn!(task = %task_id, error = %err, "failed to cancel task");
                    false
                }
            }
        }
        Ok(None) => {
            debug!(task = %task_id, "task_delete for an unknown task; ignoring");
            true
        }
        Err(err) => {
            warn!(task = %task_id, error = %err, "task lookup failed");
            false
        }
    }
}

/// Apply a trigger-definition mutation and nudge the scheduler when it changed.
/// Returns `false` only when the apply FAILED (an unchanged trigger is a
/// success), so the router can leave the inbox row unprocessed for replay.
fn apply_trigger(
    op: impl FnOnce() -> Result<bool, crate::db::DbError>,
    label: &str,
    nudge: &Notify,
) -> bool {
    match op() {
        Ok(true) => {
            debug!(op = label, "trigger updated; nudging scheduler");
            nudge.notify_one();
            true
        }
        Ok(false) => {
            debug!(op = label, "trigger unchanged");
            true
        }
        Err(err) => {
            warn!(op = label, error = %err, "failed to apply trigger");
            false
        }
    }
}

/// The top-level keys of a `config_update` payload, for a non-leaky log line (the
/// values may carry secrets, so only the key set is logged).
fn config_keys(data: &serde_json::Value) -> Vec<String> {
    data.as_object()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}

/// Apply a live `config_update` end-to-end (audit P2-19). The payload is parsed
/// ONCE as a [`ConfigOverlay`], then applied on both planes:
///
/// 1. **engine/config** ([`apply_engine_config`]) — merge the overlay onto the
///    live [`Config`] and rebuild the shared engine settings, so a rotated
///    `anthropicApiKey` reaches the next engine run without a restart.
/// 2. **provisioning** ([`apply_provisioning_update`]) — the version-gated global
///    reconcile (PLAN-007 §6).
///
/// A payload that does not parse as an overlay is dropped, not retried: the
/// failure is deterministic (a replay re-fails identically) and the host
/// re-pushes config on connect, so dropping follows the same dead-letter policy
/// the transport applies to malformed known-type frames. Non-fatal.
async fn apply_config_update(context: &ServeContext, data: &serde_json::Value) {
    let overlay: ConfigOverlay = match serde_json::from_value(data.clone()) {
        Ok(overlay) => overlay,
        Err(err) => {
            warn!(error = %err, "config_update did not parse as a config overlay; dropping (dead-letter)");
            return;
        }
    };
    apply_engine_config(context, overlay.clone());
    apply_provisioning_update(context, &overlay).await;
}

/// Merge a `config_update` overlay onto the live [`Config`] (the "WS update"
/// layer — last in the config.rs precedence chain) and rebuild the shared
/// [`ClaudeSettings`] from the merged result, so the NEXT engine spawn sees the
/// updated credentials/model/tools (see [`ReloadableEngine`] for the
/// apply-on-next-run decision). Fields the engine does not read at spawn time
/// (heartbeat cadence, paths) are merged too but only take effect where they
/// are re-read — the transport and DB keep their startup values until restart.
fn apply_engine_config(context: &ServeContext, overlay: ConfigOverlay) {
    let rebuilt = {
        let mut config = lock(&context.config);
        config.apply(overlay);
        engine_settings(&config, &context.engine_config_dir)
    };
    *lock(&context.engine_settings) = rebuilt;
    debug!("config_update merged onto live config; engine settings apply on the next run");
}

/// Apply the provisioning side of a live `config_update` (PLAN-007 §6) under the SAME
/// version gate as the startup reconcile. Re-reconcile the global scope ONLY when the
/// overlay carries BOTH a `version` and an
/// `entries` set AND that version advances past the persisted applied version. The
/// host re-pushes the FULL desired set, so one [`Provisioner::reconcile`] converges
/// add / repoint / remove (incl. the authoritative-empty case — a versioned empty set
/// removes all). A stale/equal version is ignored, and an update without a
/// `version`+`entries` pair never touches provisioning (so neither a routine config
/// refresh nor a non-authoritative empty set is mistaken for "remove all"). The
/// applied version is persisted only after a successful reconcile. Unlike the startup
/// path this does NOT gc — orphaned store versions are collected on the next restart's
/// startup gc, matching the documented "no GC without a restart". Non-fatal.
async fn apply_provisioning_update(context: &ServeContext, overlay: &ConfigOverlay) {
    let Some(provisioner) = &context.provisioner else {
        return;
    };
    // The version gate keys on a (version, entries) pair: a versioned desired set.
    let (Some(fetched), Some(entries)) = (overlay.version(), overlay.entries()) else {
        debug!("config_update without a version+entries pair; leaving global provisioning intact");
        return;
    };
    let applied = lock_db(&context.db)
        .get_applied_global_version()
        .unwrap_or_else(|err| {
            warn!(error = %err, "failed to read applied global config version; treating as unset");
            None
        });
    if applied.is_some_and(|a| fetched <= a) {
        debug!(
            applied = ?applied,
            fetched,
            "config_update version not advanced; ignoring (repair-only on restart)"
        );
        return;
    }
    match provisioner
        .reconcile("global", &context.engine_config_dir, entries)
        .await
    {
        Ok(()) => {
            debug!(
                count = entries.len(),
                version = fetched,
                "config_update reconciled global provisioning to new version"
            );
            if let Err(err) = lock_db(&context.db).set_applied_global_version(fetched) {
                warn!(error = %err, "failed to persist applied global config version after config_update");
            }
        }
        Err(err) => warn!(error = %err, "config_update global reconcile failed"),
    }
}

/// Poison-tolerant DB lock (Lock 6): recover the guard rather than panic. Critical
/// sections are short and never span an `.await`.
fn lock_db(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Poison-tolerant lock for the live config / engine-settings seams — same
/// policy as [`lock_db`]: short critical sections, never across an `.await`.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::attachment_source::DbAttachmentSource;
    use crate::attachments::AttachmentSource;
    use crate::db::{Db, TaskRow, ThreadRow};
    use crate::executor::PromptSource;
    use crate::protocol::v2::{
        AttachmentDescriptor, EntryKind, InboundFrameV2, MessageInData, ProvisioningEntry,
        TaskUpsertData,
    };

    use super::{
        InboxPromptSource, ServeContext, acquire_daemon_lock, apply_task_def, conclude_inbox_row,
        ensure_chat_thread, handle_cron_upsert, lock, lock_db, minted_thread_id,
        repend_queued_messages, route,
    };
    use crate::transport::Inbound;

    /// P2-18 regression: the daemon's single-instance lock is exclusive and
    /// fails fast. The lock is per open-file-description, so a second handle in
    /// THIS process conflicts exactly like a second daemon process would — that
    /// is the seam this test drives (no subprocess needed).
    #[test]
    fn daemon_lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let held = acquire_daemon_lock(dir.path()).unwrap();

        let err = acquire_daemon_lock(dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("already running"),
            "second acquire must fail fast with a clear error, got: {err}"
        );

        // Released on drop (any process exit): a new daemon can start.
        drop(held);
        acquire_daemon_lock(dir.path()).unwrap();
    }

    /// Audit P3 wiring: the daemon path builds its executor with the transport
    /// flush signal attached, so transcript frames stream mid-run in production
    /// (the P2-16 behavior the executor's own tests pin down).
    #[tokio::test]
    async fn build_execution_plane_wires_the_transport_flush() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let http = crate::transport::http::HttpClient::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cancel = Arc::new(crate::scheduler::Cancel::new());
        let flush = Arc::new(tokio::sync::Notify::new());
        let (executor, _provisioner, _settings) = super::build_execution_plane(
            &crate::config::Config::default(),
            &http,
            &db,
            dir.path(),
            &cancel,
            Arc::clone(&flush),
        )
        .await;
        assert!(executor.flush_wired());
    }

    /// Build a [`ServeContext`] over an in-memory DB for route-level tests. The
    /// default config carries no host, so the provisioner is absent and the
    /// provisioning side of `config_update` is a no-op here.
    async fn serve_context(db: Arc<Mutex<Db>>, dir: &std::path::Path) -> ServeContext {
        let http = crate::transport::http::HttpClient::new().unwrap();
        let cancel = Arc::new(crate::scheduler::Cancel::new());
        let config = crate::config::Config::default();
        let (executor, provisioner, settings) = super::build_execution_plane(
            &config,
            &http,
            &db,
            dir,
            &cancel,
            Arc::new(tokio::sync::Notify::new()),
        )
        .await;
        ServeContext {
            executor,
            db,
            nudge: Arc::new(tokio::sync::Notify::new()),
            drain: Arc::new(tokio::sync::Notify::new()),
            chat_max_attempts: 1,
            provisioner,
            engine_config_dir: dir.to_path_buf(),
            config: Mutex::new(config),
            engine_settings: settings,
        }
    }

    /// P2-19 regression: a `config_update` carrying a rotated `anthropicApiKey`
    /// takes effect WITHOUT a restart — the route merges the overlay onto the
    /// live config and rebuilds the shared engine settings the executor's engine
    /// snapshots at its next spawn — and the inbox row is settled like any other
    /// applied frame (no replay).
    #[tokio::test]
    async fn config_update_rotated_key_applies_to_the_next_engine_run() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let context = serve_context(Arc::clone(&db), dir.path()).await;
        assert!(
            !lock(&context.engine_settings)
                .env
                .iter()
                .any(|(key, _)| key == "ANTHROPIC_API_KEY"),
            "no key configured at startup"
        );

        let frame = InboundFrameV2::ConfigUpdate {
            data: serde_json::json!({ "engine": { "anthropicApiKey": "sk-rotated" } }),
            msg_id: Some(21),
        };
        let inbox_id = lock_db(&db)
            .record_inbound_correlated("config_update", &frame, None, None, Some(21))
            .unwrap()
            .expect("a fresh msg_id stores a row");

        assert!(
            route(Inbound { inbox_id, frame }, &context)
                .await
                .is_continue()
        );

        // The seam the engine snapshots at every spawn now carries the new key…
        let env = lock(&context.engine_settings).env.clone();
        assert!(
            env.iter()
                .any(|(key, value)| key == "ANTHROPIC_API_KEY" && value == "sk-rotated"),
            "the next engine run must see the rotated key: {env:?}"
        );
        // …and the row is settled (applied), never left to replay.
        assert!(lock_db(&db).list_unprocessed_inbox().unwrap().is_empty());
    }

    /// P2-19: a `config_update` whose payload does not parse as a config overlay
    /// follows the dead-letter policy — dropped and settled (the failure is
    /// deterministic and the host re-pushes config on connect), never left to
    /// head-of-line-block the inbox — and the engine settings stay untouched.
    #[tokio::test]
    async fn malformed_config_update_overlay_is_dropped_and_settled() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let dir = tempfile::tempdir().unwrap();
        let context = serve_context(Arc::clone(&db), dir.path()).await;
        let before = lock(&context.engine_settings).env.clone();

        let frame = InboundFrameV2::ConfigUpdate {
            // `engine` must be an object; a number fails the overlay parse.
            data: serde_json::json!({ "engine": 5 }),
            msg_id: Some(22),
        };
        let inbox_id = lock_db(&db)
            .record_inbound_correlated("config_update", &frame, None, None, Some(22))
            .unwrap()
            .expect("a fresh msg_id stores a row");

        assert!(
            route(Inbound { inbox_id, frame }, &context)
                .await
                .is_continue()
        );

        assert_eq!(
            lock(&context.engine_settings).env,
            before,
            "a malformed overlay must not disturb the engine settings"
        );
        assert!(
            lock_db(&db).list_unprocessed_inbox().unwrap().is_empty(),
            "the malformed update is settled, not left to replay"
        );
    }

    fn chat_thread(id: &str, task_id: &str) -> ThreadRow {
        ThreadRow {
            id: id.to_owned(),
            task_id: task_id.to_owned(),
            cron_id: None,
            source: "chat".to_owned(),
            prompt: String::new(),
            engine: "claude".to_owned(),
            engine_session_id: None,
            status: "pending".to_owned(),
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
            created_at: "2026-06-22T00:00:00Z".to_owned(),
        }
    }

    fn inbound_descriptor(name: &str) -> AttachmentDescriptor {
        AttachmentDescriptor {
            id: format!("in-{name}"),
            name: name.to_owned(),
            content_type: "text/plain".to_owned(),
            size: 4,
            sha256: "11".repeat(32),
            reference: format!("/agent/attachments/in-{name}"),
            direction: Some("inbound".to_owned()),
        }
    }

    /// Record a `message` frame carrying `attachments` for the thread, then resolve
    /// its prompt. The drainer consumes the inbox row (hiding it from a later inbox
    /// query) yet the executor's source still surfaces the descriptors — the
    /// consume-before-materialize ordering is handled via the durable
    /// `thread_attachments` snapshot (P1-10b), so a retry AND a fresh source over
    /// the same db (a daemon restart) resolve the identical set.
    #[test]
    fn message_attachments_survive_prompt_consumption_for_inbound_for() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let descriptors = vec![
            inbound_descriptor("spec.txt"),
            inbound_descriptor("data.json"),
        ];
        let frame = InboundFrameV2::Message {
            task_id: "t-1".to_owned(),
            thread_id: Some("th-1".to_owned()),
            data: MessageInData {
                text: "summarize the inputs".to_owned(),
                attachments: Some(descriptors.clone()),
            },
            msg_id: Some(7),
        };
        lock_db(&db)
            .record_inbound_correlated("message", &frame, Some("t-1"), Some("th-1"), Some(7))
            .unwrap();
        let thread = chat_thread("th-1", "t-1");
        {
            // Seed the FK targets: the snapshot table references threads(id).
            let guard = lock_db(&db);
            guard
                .insert_task_row(&TaskRow {
                    id: "t-1".to_owned(),
                    name: None,
                    kind: None,
                    status: "active".to_owned(),
                    created_at: "2026-06-22T00:00:00Z".to_owned(),
                    updated_at: "2026-06-22T00:00:00Z".to_owned(),
                })
                .unwrap();
            guard.insert_thread(&thread).unwrap();
        }

        let prompts = InboxPromptSource {
            db: Arc::clone(&db),
        };

        // Resolving the prompt consumes the inbox message...
        assert_eq!(
            prompts.prompt_for(&thread).as_deref(),
            Some("summarize the inputs")
        );
        assert!(
            lock_db(&db)
                .list_inbox_by_type("message")
                .unwrap()
                .is_empty(),
            "message must be consumed (no replay/re-resolve)"
        );

        // ...but the executor's attachment source still resolves the descriptors,
        // on the first attempt AND on a retry (the snapshot is never popped).
        let source = DbAttachmentSource::new(Arc::clone(&db));
        assert_eq!(source.inbound_for(&thread), descriptors);
        assert_eq!(
            source.inbound_for(&thread),
            descriptors,
            "a retried run must see the same attachments"
        );

        // A daemon restart = a fresh source over the same db: identical set.
        let restarted = DbAttachmentSource::new(Arc::clone(&db));
        assert_eq!(restarted.inbound_for(&thread), descriptors);
    }

    /// A message with no attachments snapshots nothing → `inbound_for` is empty
    /// (the executor then materializes no inputs).
    #[test]
    fn message_without_attachments_yields_no_inbound() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let frame = InboundFrameV2::Message {
            task_id: "t-2".to_owned(),
            thread_id: Some("th-2".to_owned()),
            data: MessageInData {
                text: "no files attached".to_owned(),
                attachments: None,
            },
            msg_id: None,
        };
        lock_db(&db)
            .record_inbound_correlated("message", &frame, Some("t-2"), Some("th-2"), None)
            .unwrap();

        let prompts = InboxPromptSource {
            db: Arc::clone(&db),
        };
        let thread = chat_thread("th-2", "t-2");
        assert!(prompts.prompt_for(&thread).is_some());

        let source = DbAttachmentSource::new(Arc::clone(&db));
        assert!(source.inbound_for(&thread).is_empty());
    }

    /// host-v2 §M4: a `message` with an ABSENT `threadId` starts a brand-new thread
    /// per MESSAGE — two distinct inbox rows derive two distinct thread ids, each
    /// fresh (no inherited `engine_session_id`). The old `"<task>@chat"` implicit
    /// continue is gone.
    #[test]
    fn absent_thread_id_mints_fresh_thread_per_inbox_row() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        ensure_chat_thread(&db, "t-1", &minted_thread_id("t-1", 1), 1);
        ensure_chat_thread(&db, "t-1", &minted_thread_id("t-1", 2), 1);

        let threads = lock_db(&db).list_pending_threads().unwrap();
        assert_eq!(
            threads.len(),
            2,
            "each absent-threadId message mints its own fresh thread"
        );
        assert_ne!(threads[0].id, threads[1].id, "minted ids are distinct");
        assert!(threads.iter().all(|t| t.task_id == "t-1"));
        assert!(
            threads.iter().all(|t| t.engine_session_id.is_none()),
            "fresh threads carry no engine session (nothing inherited to resume)"
        );
    }

    /// P0-3 regression: replaying an unprocessed absent-`threadId` message (the
    /// reconnect replay re-delivers the SAME inbox row) derives the SAME thread id
    /// and must NOT mint a second thread — N reconnects used to race N+1 threads
    /// for one inbox row.
    #[test]
    fn replayed_absent_thread_id_message_does_not_mint_second_thread() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        let tid = minted_thread_id("t-1", 5);

        assert!(
            ensure_chat_thread(&db, "t-1", &tid, 1),
            "original delivery mints"
        );
        assert!(
            !ensure_chat_thread(&db, "t-1", &tid, 1),
            "the replay finds the minted thread and is a no-op"
        );

        let threads = lock_db(&db).list_pending_threads().unwrap();
        assert_eq!(threads.len(), 1, "replay must not mint a second thread");
        assert_eq!(threads[0].id, tid);
    }

    /// host-v2 §M4: a `message` with a PRESENT `threadId` continues that same thread —
    /// a resting thread is re-pended for the new turn while its `engine_session_id` is
    /// PRESERVED (resumed, not minted anew).
    #[test]
    fn present_thread_id_continues_same_thread() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        {
            let guard = lock_db(&db);
            // Seed the task (FK target) and a resting thread holding a resume handle.
            guard
                .insert_task_row(&TaskRow {
                    id: "t-1".to_owned(),
                    name: None,
                    kind: None,
                    status: "active".to_owned(),
                    created_at: "2026-06-22T00:00:00Z".to_owned(),
                    updated_at: "2026-06-22T00:00:00Z".to_owned(),
                })
                .unwrap();
            let mut existing = chat_thread("th-x", "t-1");
            existing.engine_session_id = Some("sess-1".to_owned());
            "succeeded".clone_into(&mut existing.status);
            guard.insert_thread(&existing).unwrap();
        }

        ensure_chat_thread(&db, "t-1", "th-x", 1);

        let row = lock_db(&db)
            .get_thread("th-x")
            .unwrap()
            .expect("the present thread is reused, not minted anew");
        assert_eq!(row.id, "th-x", "same thread id reused");
        assert_eq!(
            row.status, "pending",
            "resting thread re-pended for the new turn"
        );
        assert_eq!(
            row.engine_session_id.as_deref(),
            Some("sess-1"),
            "engine session preserved → conversation resumes"
        );
        assert_eq!(
            lock_db(&db).list_pending_threads().unwrap().len(),
            1,
            "no extra thread minted on a present threadId"
        );
    }

    /// A `task_upsert` carrying `entries` persists them as the `tasks.entries` JSON
    /// (insert-new and patch-existing); a later `task_upsert` that OMITS `entries`
    /// must leave the prior set intact, while an explicit (empty) set replaces it
    /// (PLAN-007 §5).
    #[test]
    fn task_upsert_persists_entries_and_omission_preserves_them() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        let entry = ProvisioningEntry {
            id: "claude-md".to_owned(),
            sha256: "ab".repeat(32),
            target: "CLAUDE.md".to_owned(),
            kind: EntryKind::File,
        };

        // Insert-new WITH entries → persisted as the JSON array.
        apply_task_def(
            &db,
            "t-1",
            &TaskUpsertData {
                name: Some("build".to_owned()),
                kind: None,
                status: None,
                entries: Some(vec![entry.clone()]),
            },
        );
        let json = lock_db(&db)
            .get_task_entries("t-1")
            .unwrap()
            .expect("entries persisted on insert");
        assert_eq!(
            serde_json::from_str::<Vec<ProvisioningEntry>>(&json).unwrap(),
            vec![entry],
        );

        // Patch-existing WITHOUT entries → the prior set is untouched.
        apply_task_def(
            &db,
            "t-1",
            &TaskUpsertData {
                name: Some("build-2".to_owned()),
                kind: None,
                status: None,
                entries: None,
            },
        );
        assert_eq!(
            lock_db(&db).get_task_entries("t-1").unwrap().as_deref(),
            Some(json.as_str()),
            "a task_upsert without entries must not wipe them"
        );

        // Patch-existing WITH an explicit empty set → replaced.
        apply_task_def(
            &db,
            "t-1",
            &TaskUpsertData {
                name: None,
                kind: None,
                status: None,
                entries: Some(Vec::new()),
            },
        );
        assert_eq!(
            lock_db(&db).get_task_entries("t-1").unwrap().as_deref(),
            Some("[]"),
            "an explicit empty entries set replaces the prior one"
        );
    }

    /// P2-19 regression (inbound half): a processing FAILURE leaves the inbox row
    /// unprocessed (so it replays) and bumps the failure counter; a success marks
    /// it processed. The poison policy discards the row only after
    /// [`MAX_INBOX_RETRIES`] processing failures — reconnects never count.
    #[test]
    fn processing_failure_leaves_row_unprocessed_and_poisons_after_limit() {
        use crate::transport::MAX_INBOX_RETRIES;

        let db = Mutex::new(Db::open_in_memory().unwrap());
        let id = lock_db(&db)
            .record_inbound_correlated(
                "cron_upsert",
                &serde_json::json!({ "type": "cron_upsert" }),
                Some("t-1"),
                Some("cr-1"),
                Some(11),
            )
            .unwrap()
            .unwrap();

        // One failure: still unprocessed (replayable), counter = 1.
        conclude_inbox_row(&db, id, false);
        let rows = lock_db(&db).list_unprocessed_inbox().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "a failed apply must NOT mark the row processed"
        );
        assert_eq!(
            rows[0].retry_count, 1,
            "the counter counts processing failures"
        );

        // A later success settles it.
        conclude_inbox_row(&db, id, true);
        assert!(lock_db(&db).list_unprocessed_inbox().unwrap().is_empty());

        // A row that keeps failing is discarded exactly at the limit.
        let poison = lock_db(&db)
            .record_inbound_correlated(
                "task_delete",
                &serde_json::json!({ "type": "task_delete" }),
                Some("t-2"),
                None,
                Some(12),
            )
            .unwrap()
            .unwrap();
        for _ in 0..MAX_INBOX_RETRIES - 1 {
            conclude_inbox_row(&db, poison, false);
        }
        assert_eq!(
            lock_db(&db).list_unprocessed_inbox().unwrap().len(),
            1,
            "below the limit the row stays replayable"
        );
        conclude_inbox_row(&db, poison, false);
        assert!(
            lock_db(&db).list_unprocessed_inbox().unwrap().is_empty(),
            "at the limit the poison row is discarded"
        );
    }

    /// P2-19 regression: a `cron_upsert` arriving BEFORE its `task_upsert` is not
    /// dropped — the task row is seeded and the cron applies (the later
    /// `task_upsert` patches the seeded row in place).
    #[test]
    fn cron_upsert_before_task_upsert_is_not_dropped() {
        let db = Mutex::new(Db::open_in_memory().unwrap());
        let nudge = tokio::sync::Notify::new();
        let data = crate::protocol::v2::CronUpsertData {
            kind: "schedule".to_owned(),
            prompt: "do the rounds".to_owned(),
            schedule_type: Some("interval".to_owned()),
            schedule_value: Some("60000".to_owned()),
            next_run: None,
            continuity: "isolated".to_owned(),
            status: Some("active".to_owned()),
        };

        assert!(
            handle_cron_upsert(&db, "t-new", "cr-1", &data, &nudge),
            "a cron for a not-yet-seen task applies (task seeded)"
        );
        assert!(
            lock_db(&db).get_task_row("t-new").unwrap().is_some(),
            "a minimal task row is seeded to satisfy the FK"
        );
        let cron = lock_db(&db)
            .get_cron("cr-1")
            .unwrap()
            .expect("cron persisted");
        assert_eq!(cron.prompt, "do the rounds");

        // The later task_upsert patches the seeded row rather than failing.
        assert!(apply_task_def(
            &db,
            "t-new",
            &TaskUpsertData {
                name: Some("rounds".to_owned()),
                kind: None,
                status: None,
                entries: None,
            },
        ));
        assert_eq!(
            lock_db(&db)
                .get_task_row("t-new")
                .unwrap()
                .unwrap()
                .name
                .as_deref(),
            Some("rounds")
        );
    }

    /// P0-4 regression: two quick messages to the SAME thread — the first runs
    /// immediately; the second (queued while the thread was in flight) must
    /// actually EXECUTE once the first run finishes, driven by the post-terminal
    /// inbox re-scan. Asserts the second EXECUTION (the engine receives the
    /// second prompt), not just a queued row.
    #[allow(
        clippy::too_many_lines,
        reason = "the in-test engine stub is the bulk; splitting it would obscure the scenario"
    )]
    #[tokio::test]
    async fn queued_message_executes_after_the_run_finishes() {
        use async_trait::async_trait;
        use futures_util::stream::{self, BoxStream};

        use crate::engine::{
            Engine, EngineError, EngineEvent, RunOutcome, RunStatus, Session, SessionCfg,
        };
        use crate::executor::{ThreadExecutor, WatchdogCfg};

        /// Records every prompt it is started with, succeeding each run.
        struct RecordingEngine {
            prompts: Arc<Mutex<Vec<String>>>,
        }
        struct OkSession;

        #[async_trait]
        impl Engine for RecordingEngine {
            fn name(&self) -> &'static str {
                "recording"
            }
            async fn start_session(
                &self,
                cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                self.prompts
                    .lock()
                    .unwrap()
                    .push(cfg.initial_prompt.unwrap_or_default());
                Ok(Box::new(OkSession))
            }
        }

        #[async_trait]
        impl Session for OkSession {
            async fn interrupt(&self) -> Result<(), EngineError> {
                Ok(())
            }
            fn events(&self) -> BoxStream<'_, EngineEvent> {
                Box::pin(stream::iter(vec![EngineEvent::Result(RunOutcome {
                    status: RunStatus::Success,
                    result: Some("ok".to_owned()),
                    session_id: Some("sess".to_owned()),
                    error: None,
                    error_kind: None,
                })]))
            }
        }

        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));

        // Message 1 arrives: inbox row + pending thread th-1.
        let m1 = InboundFrameV2::Message {
            task_id: "t-1".to_owned(),
            thread_id: Some("th-1".to_owned()),
            data: MessageInData {
                text: "first".to_owned(),
                attachments: None,
            },
            msg_id: Some(1),
        };
        lock_db(&db)
            .record_inbound_correlated("message", &m1, Some("t-1"), Some("th-1"), Some(1))
            .unwrap();
        assert!(ensure_chat_thread(&db, "t-1", "th-1", 1));

        // Message 2 arrives while th-1 is in flight: only queued (the original
        // P0-4 defect: nothing ever re-checked the inbox after the run).
        let m2 = InboundFrameV2::Message {
            task_id: "t-1".to_owned(),
            thread_id: Some("th-1".to_owned()),
            data: MessageInData {
                text: "second".to_owned(),
                attachments: None,
            },
            msg_id: Some(2),
        };
        lock_db(&db)
            .record_inbound_correlated("message", &m2, Some("t-1"), Some("th-1"), Some(2))
            .unwrap();
        assert!(
            !ensure_chat_thread(&db, "t-1", "th-1", 1),
            "the second quick message is queued, not a second pending turn"
        );

        let prompts_seen = Arc::new(Mutex::new(Vec::new()));
        let engine: Arc<dyn Engine> = Arc::new(RecordingEngine {
            prompts: Arc::clone(&prompts_seen),
        });
        let dir = tempfile::tempdir().unwrap();
        let executor = ThreadExecutor::new(
            engine,
            Arc::clone(&db),
            dir.path().to_path_buf(),
            WatchdogCfg {
                inactivity_timeout: None,
                ..WatchdogCfg::default()
            },
        );
        let source = InboxPromptSource {
            db: Arc::clone(&db),
        };

        // First drain: runs turn 1 only; the second message sits queued.
        executor.drain_pending(&source).await;
        assert_eq!(*prompts_seen.lock().unwrap(), vec!["first".to_owned()]);
        assert_eq!(
            lock_db(&db).get_thread("th-1").unwrap().unwrap().status,
            "active",
            "chat thread rests between turns"
        );

        // The post-terminal re-scan re-pends the thread for the queued message...
        assert_eq!(repend_queued_messages(&db, 1), 1);
        // ...and the next drain EXECUTES it (this is the P0-4 fix).
        executor.drain_pending(&source).await;
        assert_eq!(
            *prompts_seen.lock().unwrap(),
            vec!["first".to_owned(), "second".to_owned()],
            "the queued second message must actually execute"
        );
        assert!(
            lock_db(&db)
                .list_inbox_by_type("message")
                .unwrap()
                .is_empty(),
            "both messages consumed"
        );
        assert_eq!(
            repend_queued_messages(&db, 1),
            0,
            "nothing left to re-pend — the loop terminates"
        );
    }

    /// P0-2 regression, over the runner's own wiring: with the host UNREACHABLE
    /// (the transport dialing in the background, never connected) the local
    /// planes still run — the scheduler fires a due cron and the executor
    /// worker drains it through the engine — and shutdown completes cleanly.
    /// The old code blocked boot on the first dial and shut the whole daemon
    /// down once the transport's 60-retry budget was exhausted.
    #[allow(
        clippy::too_many_lines,
        reason = "the in-test engine stub is the bulk; splitting it would obscure the scenario"
    )]
    #[tokio::test]
    async fn local_planes_run_and_shut_down_cleanly_while_host_unreachable() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use async_trait::async_trait;
        use futures_util::stream::{self, BoxStream};
        use secrecy::SecretString;

        use crate::db::CronRow;
        use crate::engine::{
            Engine, EngineError, EngineEvent, RunOutcome, RunStatus, Session, SessionCfg,
        };
        use crate::executor::{ThreadExecutor, WatchdogCfg};
        use crate::scheduler::{Cancel, Scheduler};
        use crate::transport::{Transport, TransportConfig};

        /// Counts engine runs; every session immediately succeeds.
        struct CountingEngine {
            runs: AtomicUsize,
        }
        struct OkSession;

        #[async_trait]
        impl Engine for CountingEngine {
            fn name(&self) -> &'static str {
                "counting"
            }
            async fn start_session(
                &self,
                _cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                self.runs.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(OkSession))
            }
        }

        #[async_trait]
        impl Session for OkSession {
            async fn interrupt(&self) -> Result<(), EngineError> {
                Ok(())
            }
            fn events(&self) -> BoxStream<'_, EngineEvent> {
                Box::pin(stream::iter(vec![
                    EngineEvent::SessionStarted {
                        session_id: "sess-off".to_owned(),
                    },
                    EngineEvent::Result(RunOutcome {
                        status: RunStatus::Success,
                        result: Some("ran while the host was down".to_owned()),
                        session_id: Some("sess-off".to_owned()),
                        error: None,
                        error_kind: None,
                    }),
                ]))
            }
        }

        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        // A task with a cron already due: the scheduler's first tick fires it
        // (an overdue `interval` slot dispatches; only `once` has a grace cut).
        lock_db(&db)
            .insert_task_row(&TaskRow {
                id: "task-1".to_owned(),
                name: None,
                kind: None,
                status: "active".to_owned(),
                created_at: "2026-06-22T09:00:00.000Z".to_owned(),
                updated_at: "2026-06-22T09:00:00.000Z".to_owned(),
            })
            .unwrap();
        lock_db(&db)
            .insert_cron(&CronRow {
                id: "c-off".to_owned(),
                task_id: "task-1".to_owned(),
                kind: "schedule".to_owned(),
                prompt: "run the recurring job".to_owned(),
                schedule_type: Some("interval".to_owned()),
                schedule_value: Some("60000".to_owned()),
                next_run: Some("2026-06-22T09:00:00.000Z".to_owned()),
                last_fired_at: None,
                continuity: "isolated".to_owned(),
                run_count: 0,
                status: "active".to_owned(),
                dirty: false,
                created_at: "2026-06-22T09:00:00.000Z".to_owned(),
                updated_at: "2026-06-22T09:00:00.000Z".to_owned(),
            })
            .unwrap();

        // The host is UNREACHABLE: a bound-then-dropped port refuses every dial.
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let transport = Transport::open(
            format!("ws://{addr}/agent/ws/agent-off"),
            SecretString::from("tok"),
            Arc::clone(&db),
            TransportConfig::default(),
        );

        let engine = Arc::new(CountingEngine {
            runs: AtomicUsize::new(0),
        });
        let dir = tempfile::tempdir().unwrap();
        let executor = Arc::new(ThreadExecutor::new(
            Arc::clone(&engine) as Arc<dyn Engine>,
            Arc::clone(&db),
            dir.path().to_path_buf(),
            WatchdogCfg {
                inactivity_timeout: None,
                ..WatchdogCfg::default()
            },
        ));
        let prompts = Arc::new(InboxPromptSource {
            db: Arc::clone(&db),
        });

        // The runner's own planes: the real scheduler task + executor worker.
        let cancel = Arc::new(Cancel::new());
        let scheduler = Scheduler::new(Arc::clone(&db), Arc::clone(&cancel), 1, 30);
        let drain = scheduler.drain_signal();
        let scheduler_task = tokio::spawn(scheduler.run());
        let worker = tokio::spawn(super::run_worker(
            Arc::clone(&executor),
            Arc::clone(&db),
            prompts,
            drain,
            transport.flush_handle(),
            Arc::clone(&cancel),
            1,
        ));

        // The cron fires and its thread EXECUTES while the host is down.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while lock_db(&db)
            .list_threads_by_status("succeeded")
            .unwrap()
            .is_empty()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "cron thread did not execute while the host was unreachable"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            engine.runs.load(Ordering::SeqCst),
            1,
            "the engine actually ran offline"
        );
        // The result plane queued durably for the eventual reconnect replay.
        assert!(
            !lock_db(&db).list_unacked_outbox().unwrap().is_empty(),
            "result frames wait in the outbox for the host to come back"
        );

        // Shutdown completes although the transport never connected.
        cancel.cancel();
        worker.await.unwrap();
        scheduler_task.await.unwrap();
        transport.shutdown().await;
    }

    /// P1-6 regression: SIGTERM (what systemd `stop` sends) resolves the
    /// shutdown-signal future — the old code handled only SIGINT and left
    /// SIGTERM on the default handler. The OS edge is exercised in-process:
    /// tokio's listener registration replaces the default disposition, so
    /// raising SIGTERM here notifies the future instead of killing the test
    /// runner.
    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_resolves_the_shutdown_signal() {
        use std::time::Duration;

        // Install the process-wide SIGTERM handler BEFORE raising, so a lost
        // registration race cannot kill the test binary (tokio never restores
        // the default disposition once a listener has existed).
        let _armed = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing the SIGTERM listener");
        let task = tokio::spawn(super::shutdown_signal());
        // Let the spawned future register its own listeners before the raise.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let status = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("raising SIGTERM");
        assert!(status.success());

        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("shutdown_signal did not resolve on SIGTERM")
            .unwrap();
    }

    /// P1-6 regression, over the runner's own shutdown sequence: a shutdown
    /// while a run is ACTIVE trips the run's cancellation and the worker
    /// settles within the bounded drain window — the old code awaited the
    /// worker unconditionally, blocking up to running_timeout (30 min) until
    /// the supervisor SIGKILLed the daemon and orphaned the engine child.
    #[allow(
        clippy::too_many_lines,
        reason = "the in-test engine stub is the bulk; splitting it would obscure the scenario"
    )]
    #[tokio::test]
    async fn shutdown_cancels_the_active_run_within_the_drain_window() {
        use std::time::Duration;

        use async_trait::async_trait;
        use futures_util::StreamExt;
        use futures_util::stream::{self, BoxStream};

        use crate::engine::{Engine, EngineError, EngineEvent, Session, SessionCfg};
        use crate::executor::{ThreadExecutor, WatchdogCfg};
        use crate::scheduler::Cancel;

        /// Announces a session then stalls forever, ignoring the interrupt —
        /// the engine shape that used to pin shutdown until running_timeout.
        struct StallingEngine;
        struct StallingSession;

        #[async_trait]
        impl Engine for StallingEngine {
            fn name(&self) -> &'static str {
                "stalling"
            }
            async fn start_session(
                &self,
                _cfg: SessionCfg,
            ) -> Result<Box<dyn Session>, EngineError> {
                Ok(Box::new(StallingSession))
            }
        }

        #[async_trait]
        impl Session for StallingSession {
            async fn interrupt(&self) -> Result<(), EngineError> {
                Ok(())
            }
            fn events(&self) -> BoxStream<'_, EngineEvent> {
                let head = stream::iter(vec![EngineEvent::SessionStarted {
                    session_id: "sess-stall".to_owned(),
                }]);
                Box::pin(head.chain(stream::pending::<EngineEvent>()))
            }
        }

        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        lock_db(&db)
            .insert_task_row(&TaskRow {
                id: "t-1".to_owned(),
                name: None,
                kind: None,
                status: "active".to_owned(),
                created_at: "2026-06-22T09:00:00.000Z".to_owned(),
                updated_at: "2026-06-22T09:00:00.000Z".to_owned(),
            })
            .unwrap();
        // A pending thread with a prompt snapshot: the drain runs it directly.
        let mut thread = chat_thread("th-stall", "t-1");
        thread.prompt = "keep busy".to_owned();
        lock_db(&db).insert_thread(&thread).unwrap();

        let cancel = Arc::new(Cancel::new());
        let engine: Arc<dyn Engine> = Arc::new(StallingEngine);
        let dir = tempfile::tempdir().unwrap();
        let executor = Arc::new(
            ThreadExecutor::new(
                engine,
                Arc::clone(&db),
                dir.path().to_path_buf(),
                WatchdogCfg {
                    inactivity_timeout: None,
                    cancel_drain_timeout: Duration::from_millis(100),
                    ..WatchdogCfg::default()
                },
            )
            .with_shutdown(Arc::clone(&cancel)),
        );
        let prompts = Arc::new(InboxPromptSource {
            db: Arc::clone(&db),
        });
        let worker = tokio::spawn(super::run_worker(
            Arc::clone(&executor),
            Arc::clone(&db),
            prompts,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
            Arc::clone(&cancel),
            1,
        ));

        // Wait until the run is live (the engine is stalling inside it).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while lock_db(&db).get_thread("th-stall").unwrap().unwrap().status != "running" {
            assert!(
                std::time::Instant::now() < deadline,
                "the stalling run never started"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The full shutdown sequence must settle in seconds, not 30 minutes.
        tokio::time::timeout(
            Duration::from_secs(5),
            super::shutdown_worker(&cancel, &executor, worker, Duration::from_secs(10)),
        )
        .await
        .expect("shutdown did not settle within the drain window");

        let row = lock_db(&db).get_thread("th-stall").unwrap().unwrap();
        assert_eq!(row.status, "cancelled", "the active run was cancelled");
        assert!(
            row.engine_session_id.is_none(),
            "the hard-killed turn keeps no resume handle"
        );
    }
}

/// Focused coverage of the PLAN-007-5a global-provisioning startup wiring
/// (`reconcile_global_startup`) against a fake byte fetcher — the full host-driven
/// path is exercised by the e2e harness (L3-6).
#[cfg(test)]
mod provisioning_wiring_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::attachments::sha256_hex;
    use crate::db::Db;
    use crate::protocol::v2::{EntryKind, ProvisioningEntry};
    use crate::provisioning::{BlobFetcher, Provisioner, ProvisioningError};

    use super::{ENGINE_CONFIG_SUBDIR, reconcile_global_startup};

    /// In-memory blob fetcher keyed by host-proxy `reference`. Verifies the served
    /// bytes hash to the requested `sha256` (mirroring the real fetcher's contract)
    /// and counts fetches so a test can assert the version gate's repair-only branch
    /// performs no byte re-fetch.
    struct FakeFetcher {
        blobs: HashMap<String, Vec<u8>>,
        fetches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BlobFetcher for FakeFetcher {
        async fn fetch(&self, reference: &str, sha256: &str) -> Result<Vec<u8>, ProvisioningError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            let bytes =
                self.blobs
                    .get(reference)
                    .cloned()
                    .ok_or_else(|| ProvisioningError::Fetch {
                        reason: format!("no blob at {reference}"),
                    })?;
            assert_eq!(sha256_hex(&bytes), sha256, "fake blob sha mismatch");
            Ok(bytes)
        }
    }

    /// A provisioner over a fresh in-memory DB and `blobs`, returning the SHARED db
    /// handle (the version gate reads/writes `config_meta` on it) and the fetch
    /// counter so the no-refetch invariant is assertable.
    fn provisioner_with(
        ws: &std::path::Path,
        blobs: HashMap<String, Vec<u8>>,
    ) -> (Provisioner, Arc<Mutex<Db>>, Arc<AtomicUsize>) {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let fetches = Arc::new(AtomicUsize::new(0));
        let blob_fetcher: Arc<dyn BlobFetcher> = Arc::new(FakeFetcher {
            blobs,
            fetches: Arc::clone(&fetches),
        });
        let prov = Provisioner::new(ws.to_path_buf(), Arc::clone(&db), blob_fetcher);
        (prov, db, fetches)
    }

    fn file_entry(id: &str, target: &str, bytes: &[u8]) -> ProvisioningEntry {
        ProvisioningEntry {
            id: id.to_owned(),
            sha256: sha256_hex(bytes),
            target: target.to_owned(),
            kind: EntryKind::File,
        }
    }

    /// The startup sequence with a versioned empty desired set is a clean no-op: it
    /// must not panic and must leave the engine config dir empty (no spurious links).
    #[tokio::test]
    async fn startup_reconcile_with_no_entries_is_a_clean_noop() {
        let ws = tempfile::tempdir().unwrap();
        let engine_config_dir = ws.path().join(ENGINE_CONFIG_SUBDIR);
        std::fs::create_dir_all(&engine_config_dir).unwrap();
        let (prov, db, _fetches) = provisioner_with(ws.path(), HashMap::new());

        reconcile_global_startup(&prov, &db, &engine_config_dir, &[], Some(1)).await;

        assert!(
            std::fs::read_dir(&engine_config_dir)
                .unwrap()
                .next()
                .is_none(),
            "no entries → engine config dir stays empty"
        );
        assert_eq!(
            lock_applied(&db),
            Some(1),
            "a versioned reconcile records its applied version"
        );
    }

    /// The startup sequence materializes a global `file` entry as a symlink under
    /// the engine config dir that resolves to the fetched bytes.
    #[tokio::test]
    async fn startup_reconcile_materializes_global_entry_symlink() {
        let ws = tempfile::tempdir().unwrap();
        let engine_config_dir = ws.path().join(ENGINE_CONFIG_SUBDIR);
        std::fs::create_dir_all(&engine_config_dir).unwrap();

        let bytes = b"# global CLAUDE.md\n".to_vec();
        let mut blobs = HashMap::new();
        blobs.insert("/agent/attachments/clauderc".to_owned(), bytes.clone());
        let (prov, db, _fetches) = provisioner_with(ws.path(), blobs);

        let entry = file_entry("clauderc", "CLAUDE.md", &bytes);
        reconcile_global_startup(
            &prov,
            &db,
            &engine_config_dir,
            std::slice::from_ref(&entry),
            Some(1),
        )
        .await;

        let link = engine_config_dir.join("CLAUDE.md");
        assert_eq!(
            std::fs::read(&link).unwrap(),
            bytes,
            "symlink resolves to the fetched bytes"
        );
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink(),
            "target is materialized as a symlink into the store"
        );
    }

    /// AUTHORITATIVE-EMPTY (PLAN-007 §4): a VERSIONED config whose `version` ADVANCES
    /// with zero entries means the host genuinely wants none → the reconcile removes
    /// every installed global entry, and the applied version advances.
    #[tokio::test]
    async fn versioned_empty_set_is_authoritative_and_removes_all() {
        let ws = tempfile::tempdir().unwrap();
        let engine_config_dir = ws.path().join(ENGINE_CONFIG_SUBDIR);
        std::fs::create_dir_all(&engine_config_dir).unwrap();

        let bytes = b"# CLAUDE.md v1\n".to_vec();
        let mut blobs = HashMap::new();
        blobs.insert("/agent/attachments/clauderc".to_owned(), bytes.clone());
        let (prov, db, _fetches) = provisioner_with(ws.path(), blobs);

        // v1 installs one global entry.
        let entry = file_entry("clauderc", "CLAUDE.md", &bytes);
        reconcile_global_startup(
            &prov,
            &db,
            &engine_config_dir,
            std::slice::from_ref(&entry),
            Some(1),
        )
        .await;
        assert!(engine_config_dir.join("CLAUDE.md").is_symlink());
        assert_eq!(lock_applied(&db), Some(1));

        // v2 with an EMPTY set is authoritative-empty → removes the installed entry.
        reconcile_global_startup(&prov, &db, &engine_config_dir, &[], Some(2)).await;
        assert!(
            std::fs::read_dir(&engine_config_dir)
                .unwrap()
                .next()
                .is_none(),
            "an authoritative (versioned) empty set must remove every global entry"
        );
        assert_eq!(lock_applied(&db), Some(2), "applied version advanced to 2");
    }

    /// NON-AUTHORITATIVE-EMPTY (PLAN-007 §4, the restart-durability crux): an empty
    /// set with NO version (offline / not-yet-loaded) must NOT wipe — the installed
    /// global entry survives (repair-only), and the applied version is unchanged.
    #[tokio::test]
    async fn unversioned_empty_set_never_wipes_installed_global() {
        let ws = tempfile::tempdir().unwrap();
        let engine_config_dir = ws.path().join(ENGINE_CONFIG_SUBDIR);
        std::fs::create_dir_all(&engine_config_dir).unwrap();

        let bytes = b"# CLAUDE.md durable\n".to_vec();
        let mut blobs = HashMap::new();
        blobs.insert("/agent/attachments/clauderc".to_owned(), bytes.clone());
        let (prov, db, fetches) = provisioner_with(ws.path(), blobs);

        // Install at v1 (one fetch).
        let entry = file_entry("clauderc", "CLAUDE.md", &bytes);
        reconcile_global_startup(
            &prov,
            &db,
            &engine_config_dir,
            std::slice::from_ref(&entry),
            Some(1),
        )
        .await;
        let link = engine_config_dir.join("CLAUDE.md");
        assert!(link.is_symlink());
        assert_eq!(fetches.load(Ordering::SeqCst), 1, "v1 fetched once");

        // Simulate a restart whose config fetch FAILED (no version) handing an EMPTY
        // desired set: the installed entry must survive (no reconcile-to-empty).
        reconcile_global_startup(&prov, &db, &engine_config_dir, &[], None).await;
        assert!(
            link.is_symlink() && std::fs::read(&link).unwrap() == bytes,
            "a non-authoritative empty set must never wipe the installed global entry"
        );
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            1,
            "repair-only leaves the present store entry untouched (no re-fetch)"
        );
        assert_eq!(lock_applied(&db), Some(1), "applied version unchanged");
    }

    /// SAME-VERSION (restart durability): re-running at the SAME applied version
    /// neither wipes nor re-fetches — repair-only keeps the installed link, and the
    /// applied version is unchanged.
    #[tokio::test]
    async fn same_version_is_repair_only_no_refetch_no_wipe() {
        let ws = tempfile::tempdir().unwrap();
        let engine_config_dir = ws.path().join(ENGINE_CONFIG_SUBDIR);
        std::fs::create_dir_all(&engine_config_dir).unwrap();

        let bytes = b"# CLAUDE.md stable\n".to_vec();
        let mut blobs = HashMap::new();
        blobs.insert("/agent/attachments/clauderc".to_owned(), bytes.clone());
        let (prov, db, fetches) = provisioner_with(ws.path(), blobs);

        let entry = file_entry("clauderc", "CLAUDE.md", &bytes);
        reconcile_global_startup(
            &prov,
            &db,
            &engine_config_dir,
            std::slice::from_ref(&entry),
            Some(1),
        )
        .await;
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        // Re-run at the SAME version with the SAME desired set (the restart case):
        // repair-only, so the link survives and nothing is re-fetched.
        reconcile_global_startup(
            &prov,
            &db,
            &engine_config_dir,
            std::slice::from_ref(&entry),
            Some(1),
        )
        .await;
        let link = engine_config_dir.join("CLAUDE.md");
        assert!(link.is_symlink() && std::fs::read(&link).unwrap() == bytes);
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            1,
            "same version → no re-fetch"
        );
        assert_eq!(lock_applied(&db), Some(1));
    }

    /// Read the persisted applied global config version off the shared db handle.
    fn lock_applied(db: &Mutex<Db>) -> Option<i64> {
        db.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_applied_global_version()
            .unwrap()
    }
}
