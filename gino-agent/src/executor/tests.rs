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
    Engine, EngineError, EngineEvent, RunErrorKind, RunOutcome, RunStatus, Session, SessionCfg,
};
use crate::protocol::v2::{
    AttachmentDescriptor, EntryKind, ProvisioningEntry, ThreadMessageData, ThreadResultData,
    ThreadStatus,
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
    async fn start_session(&self, _cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
        Ok(Box::new(MockSession {
            script: self.script.clone(),
            stall: self.stall,
        }))
    }
}

#[async_trait]
impl Session for MockSession {
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
        engine: None,
        model: None,
        workdir: None,
        meta: None,
        created_at: to_iso(now()),
        updated_at: to_iso(now()),
    })
    .unwrap();
    Arc::new(Mutex::new(db))
}

fn set_task_status(db: &Arc<Mutex<Db>>, status: &str) {
    let guard = lock(db);
    let mut row = guard.get_task_row("task-1").unwrap().unwrap();
    status.clone_into(&mut row.status);
    guard.update_task_row(&row).unwrap();
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
    async fn start_session(&self, _cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
        Ok(Box::new(GatedSession {
            gate: Arc::clone(&self.gate),
        }))
    }
}

#[async_trait]
impl Session for GatedSession {
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

/// D1: a `pending` thread under a closed/cancelled task is NOT drained — it is
/// left `pending` (never failed, never deleted), its inbox message unconsumed —
/// and it drains as soon as the task is `active` again.
#[tokio::test]
async fn closed_task_parks_its_pending_thread_until_reactivation() {
    let db = test_db();
    let exec = build(
        &db,
        vec![started("s"), ok_result("ok", "s")],
        false,
        no_inactivity(),
    );
    lock(&db).insert_thread(&pending_thread("th-a", 1)).unwrap();
    set_task_status(&db, "closed");

    assert_eq!(exec.drain_pending(&StaticPrompt("go")).await, 0);
    let parked = lock(&db).get_thread("th-a").unwrap().unwrap();
    assert_eq!(parked.status, "pending", "a parked thread stays pending");
    assert!(parked.failure_reason.is_none());
    assert_eq!(parked.attempt, 0);
    assert!(
        parked.prompt.is_empty(),
        "a parked thread never resolves (or consumes) its prompt"
    );

    set_task_status(&db, "active");
    assert_eq!(exec.drain_pending(&StaticPrompt("go")).await, 1);
    assert_eq!(
        lock(&db).get_thread("th-a").unwrap().unwrap().status,
        "succeeded"
    );
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
        async fn start_session(&self, _cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
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
        async fn start_session(&self, _cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
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
    }

    #[async_trait]
    impl Session for SettlingSession {
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
    let handle = tokio::spawn(async move { drainer.drain_pending(&StaticPrompt("go")).await });

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
    async fn start_session(&self, cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
        let out = cfg.cwd.join("output");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join(&self.rel), &self.bytes).unwrap();
        Ok(Box::new(MockSession {
            script: vec![started("sess-att"), ok_result("done", "sess-att")],
            stall: false,
        }))
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

// -- Per-task model override (align-2026-08 D2, OD15) ---------------

/// An engine that logs the per-session model of every spawn, then replays a
/// trivial successful run. Lets a test assert what `run_thread` hands the
/// backend as the model for THIS spawn.
struct ModelProbe {
    spawns: Arc<Mutex<Vec<Option<String>>>>,
}

#[async_trait]
impl Engine for ModelProbe {
    fn name(&self) -> &'static str {
        "model-probe"
    }
    async fn start_session(&self, cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
        self.spawns.lock().unwrap().push(cfg.model.clone());
        Ok(Box::new(MockSession {
            script: vec![started("sess-model"), ok_result("done", "sess-model")],
            stall: false,
        }))
    }
}

/// Run one thread under `task-1` through a [`ModelProbe`] and report the model
/// the executor put on the session config.
async fn model_for_run(db: &Arc<Mutex<Db>>, thread_id: &str) -> Option<String> {
    let ws = tempfile::tempdir().unwrap();
    let spawns = Arc::new(Mutex::new(Vec::new()));
    let engine: Arc<dyn Engine> = Arc::new(ModelProbe {
        spawns: Arc::clone(&spawns),
    });
    let exec = ThreadExecutor::new(
        engine,
        Arc::clone(db),
        ws.path().to_path_buf(),
        no_inactivity(),
    );
    lock(db)
        .insert_thread(&pending_thread(thread_id, 1))
        .unwrap();
    let row = exec
        .run_thread(
            pending_thread(thread_id, 1),
            "go".to_owned(),
            Continuity::Isolated,
        )
        .await;
    assert_eq!(row.status, "succeeded");
    let spawns = spawns.lock().unwrap();
    assert_eq!(spawns.len(), 1, "exactly one spawn per run");
    spawns[0].clone()
}

/// D2: a thread whose task carries `model` spawns the engine with THAT model
/// (overriding `config.engine.model`, which the backend holds); a task without
/// one leaves the session config untouched so the global value stands.
#[tokio::test]
async fn task_model_overrides_the_configured_model_at_spawn() {
    let db = test_db();

    // No task model → nothing to override with; the engine keeps its own.
    assert_eq!(model_for_run(&db, "th-global").await, None);

    // A task model → carried onto the session for the next spawn.
    {
        let guard = lock(&db);
        let mut row = guard.get_task_row("task-1").unwrap().unwrap();
        row.model = Some("claude-opus-4-8".to_owned());
        guard.update_task_row(&row).unwrap();
    }
    assert_eq!(
        model_for_run(&db, "th-task-model").await.as_deref(),
        Some("claude-opus-4-8")
    );
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
    async fn start_session(&self, cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError> {
        *self.seen.lock().unwrap() = Some(cfg.cwd.clone());
        Ok(Box::new(MockSession {
            script: vec![started("sess-prov"), ok_result("done", "sess-prov")],
            stall: false,
        }))
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
    async fn fetch(&self, reference: &str, sha256: &str) -> Result<Vec<u8>, ProvisioningError> {
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
        &format!("/agent/attachments?sha256={}", sha256_hex(bytes)),
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
    let exec = ThreadExecutor::new(engine, Arc::clone(db), ws.to_path_buf(), no_inactivity())
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
        &format!("/agent/attachments?sha256={}", sha256_hex(&bytes)),
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
