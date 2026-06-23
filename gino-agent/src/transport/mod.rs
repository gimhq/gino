//! Outbound transport (PLAN-003 §2, PLAN-001 `ws.rs`).
//!
//! The runner dials *out* — to be NAT/firewall friendly the orchestrator never
//! connects to us. This module owns that outbound side:
//!
//! * [`http`] — the `reqwest` client for `POST /agent/register`,
//!   `GET /agent/config/:id`, and the `POST /agent/output/:id` fallback.
//! * [`ws`] — the reconnecting `tokio-tungstenite` client with outbox-backed
//!   at-least-once delivery, inbox dedup/replay, bounded-backoff reconnect, and a
//!   periodic heartbeat.
//!
//! [`Transport`] is the engine-agnostic facade over the WS side: it works purely
//! on [`crate::protocol::v2`] frames and the [`crate::db`] outbox/inbox, so the
//! engine and scheduler (later L3s) never touch tungstenite. [`Transport::send`]
//! persists an outbound frame to the outbox *before* transmitting it; the manager
//! task drains the outbox, dedups the inbox, and forwards inbound frames on an
//! unbounded channel drained via [`Transport::recv`].
//!
//! ## Envelope vs payload (protocol v2, agent-architecture §8/§10)
//!
//! Correlation/delivery metadata lives at the **envelope top level**
//! (`type`, `outboxId`/`msgId`, `taskId`/`threadId`/`cronId`); `data` carries the
//! type-specific business payload only. The two delivery buffers are asymmetric:
//!
//! * **outbox payload = the frame's `data` only.** The correlation ids are written
//!   to the indexed `task_id`/`thread_id` columns; the full `{type, …ids, data,
//!   outboxId}` envelope is *rebuilt on send/replay* from those columns
//!   ([`build_envelope`]). The host routes purely off the envelope ids.
//! * **inbox payload = the full received frame**, re-delivered verbatim on replay;
//!   the `type` column is the replay *classification* and the correlation columns
//!   are routing indexes.
//!
//! `outboxId`/`msgId` are delivery/ack only; `seq` (inside `data`) is the
//! per-thread continuity contract — the transport ships frames in order but never
//! reorders, and `engine_session_id` is a *value* that stays inside `data`.

pub(crate) mod http;
mod ws;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use secrecy::SecretString;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::db::{Db, DbError};
use crate::protocol::v2::{InboundFrameV2, OutboundFrameV2};

use self::ws::{WsDriver, WsError};

/// Default liveness ping cadence. The TS runner sent no heartbeat; the Rust
/// runner adds one so a silently dead half-open socket is noticed promptly.
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Public configuration / values
// ---------------------------------------------------------------------------

/// Reconnect backoff schedule. Ported from the TS runner: exponential from
/// `base`, capped at `max`, giving up after `max_retries` consecutive failures.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BackoffConfig {
    base: Duration,
    max: Duration,
    max_retries: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        // TS parity: 1 s base, 10 s ceiling, 60 attempts.
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(10),
            max_retries: 60,
        }
    }
}

#[cfg(test)]
impl BackoffConfig {
    /// Fast schedule for reconnect tests (no second-scale sleeps).
    fn for_tests() -> Self {
        Self {
            base: Duration::from_millis(10),
            max: Duration::from_millis(40),
            max_retries: 8,
        }
    }
}

/// Knobs for [`Transport::open`]. [`Default`] matches production; tests override
/// the cadence to keep wall-clock low.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransportConfig {
    pub(crate) heartbeat: Duration,
    pub(crate) backoff: BackoffConfig,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            heartbeat: DEFAULT_HEARTBEAT,
            backoff: BackoffConfig::default(),
        }
    }
}

/// An inbound frame handed to the consumer, paired with the inbox row id that
/// holds it. The consumer calls [`Transport::mark_inbox_processed`] once it has
/// durably handled the frame — until then the row replays on reconnect.
#[derive(Debug)]
pub(crate) struct Inbound {
    pub(crate) inbox_id: i64,
    pub(crate) frame: InboundFrameV2,
}

/// Errors raised while opening or driving the transport.
#[derive(Debug, Error)]
pub(crate) enum TransportError {
    /// The initial WebSocket dial failed.
    #[error("websocket connect: {0}")]
    Connect(#[from] WsError),
    /// Persisting an outbound frame to the outbox failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// An outbound frame could not be serialized.
    #[error("serialize outbound frame: {0}")]
    Serialize(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Transport facade
// ---------------------------------------------------------------------------

/// Engine-agnostic handle over the reconnecting WS connection. Cheap to hold;
/// the actual IO runs in a spawned manager task.
#[derive(Debug)]
pub(crate) struct Transport {
    db: Arc<Mutex<Db>>,
    // Written by `send` (the engine L3's producer path); only that method reads it,
    // so it is dead in the not-yet-wired binary build.
    #[allow(dead_code, reason = "read by Transport::send, wired by the engine L3")]
    immediate_tx: mpsc::UnboundedSender<String>,
    inbound_rx: mpsc::UnboundedReceiver<Inbound>,
    cancel: Arc<Cancel>,
    /// Poked to make the manager replay the unacked outbox over the live socket
    /// (the executor/scheduler enqueue frames straight to the outbox, so this is
    /// how a live send is triggered between reconnects).
    flush: Arc<Notify>,
    manager: JoinHandle<()>,
}

impl Transport {
    /// Dial `ws_url` (with `connect_token` in the `x-internal-token` header) and
    /// spawn the manager task that drains the outbox, dedups the inbox, reconnects
    /// with backoff, and heartbeats. Returns once the *first* connection is up.
    pub(crate) async fn open(
        ws_url: String,
        connect_token: SecretString,
        db: Arc<Mutex<Db>>,
        config: TransportConfig,
    ) -> Result<Self, TransportError> {
        let initial = ws::dial(&ws_url, &connect_token).await?;
        let (immediate_tx, immediate_rx) = mpsc::unbounded_channel();
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(Cancel::new());
        let flush = Arc::new(Notify::new());
        let driver = WsDriver {
            ws_url,
            connect_token,
            db: Arc::clone(&db),
            inbound_tx,
            immediate_rx,
            cancel: Arc::clone(&cancel),
            flush: Arc::clone(&flush),
            heartbeat: config.heartbeat,
            backoff: Backoff::new(config.backoff),
        };
        let manager = tokio::spawn(driver.run(initial));
        Ok(Self {
            db,
            immediate_tx,
            inbound_rx,
            cancel,
            flush,
            manager,
        })
    }

    /// A handle that, when notified, makes the manager replay the unacked outbox
    /// over the current live connection. The executor/scheduler enqueue result-plane
    /// frames straight to the durable outbox; the runner pokes this after a drain so
    /// those frames stream out promptly instead of waiting for the next reconnect.
    /// Replay is idempotent on the host (it dedups on `(thread_id, seq)`), matching
    /// the reconnect replay.
    pub(crate) fn flush_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.flush)
    }

    /// Persist a data-bearing frame to the outbox and transmit it best-effort.
    /// Returns the outbox row id for data frames; `None` for the raw control
    /// frames (`heartbeat`/`ack`/`msg_ack`), which are sent without outbox
    /// backing. A frame whose live send is dropped (disconnected) is still durably
    /// queued and replayed on the next (re)connect.
    ///
    /// The persisted payload is the frame's `data` only; its correlation ids go to
    /// the indexed `task_id`/`thread_id` columns (the `cron_state` frame's `cronId`
    /// rides the secondary column). The wire envelope is rebuilt here and on every
    /// replay via [`build_envelope`], so the bytes are identical either way.
    #[allow(
        dead_code,
        reason = "the engine/scheduler L3s are the producers of outbound frames"
    )]
    pub(crate) fn send(&self, frame: &OutboundFrameV2) -> Result<Option<i64>, TransportError> {
        if let Some(entry) = outbox_entry(frame)? {
            let id = lock_db(&self.db).enqueue_outbox_correlated(
                entry.msg_type,
                &entry.data,
                entry.task_id.as_deref(),
                entry.secondary_id.as_deref(),
            )?;
            let envelope = build_envelope(
                entry.msg_type,
                &entry.data,
                entry.task_id.as_deref(),
                entry.secondary_id.as_deref(),
                id,
            );
            // Unbounded + best-effort: a closed channel just means the manager has
            // stopped; the row stays unacked and replays on reconnect.
            let _ = self.immediate_tx.send(envelope.to_string());
            Ok(Some(id))
        } else {
            // Raw control frame (heartbeat/ack/msg_ack): no outbox backing.
            let _ = self.immediate_tx.send(serde_json::to_string(frame)?);
            Ok(None)
        }
    }

    /// Await the next inbound frame. Returns `None` once the manager task has
    /// stopped (reconnect exhausted or shutdown) — the signal for the caller to
    /// wind down.
    pub(crate) async fn recv(&mut self) -> Option<Inbound> {
        self.inbound_rx.recv().await
    }

    /// Mark an inbox row durably handled so it is not replayed on reconnect.
    pub(crate) fn mark_inbox_processed(&self, inbox_id: i64) -> Result<(), TransportError> {
        lock_db(&self.db).mark_inbox_processed(inbox_id)?;
        Ok(())
    }

    /// Signal shutdown and wait for the manager task to drain and exit.
    pub(crate) async fn shutdown(self) {
        self.cancel.cancel();
        if self.manager.await.is_err_and(|err| err.is_panic()) {
            warn!("transport manager task panicked during shutdown");
        }
    }
}

// ---------------------------------------------------------------------------
// Outbox mapping (protocol v2)
// ---------------------------------------------------------------------------

/// What [`outbox_entry`] extracts from a data-bearing v2 frame: the wire `type`,
/// the `data`-only payload to persist, and the correlation ids destined for the
/// outbox's indexed columns. `secondary_id` is the frame's `threadId` (thread
/// frames) or `cronId` (`cron_state`) — both share the single `thread_id` column;
/// [`secondary_id_key`] maps it back to the right envelope key on rebuild.
struct OutboxEntry {
    msg_type: &'static str,
    data: Value,
    task_id: Option<String>,
    secondary_id: Option<String>,
}

/// Split an outbound v2 frame into its [`OutboxEntry`], or `None` for the raw
/// control frames (`heartbeat`/`msg_ack`/`ack`) that are not outbox-backed. The
/// persisted payload is the frame's `data` only; [`build_envelope`] rebuilds the
/// full `{type, …ids, data, outboxId}` envelope on send and on every replay.
fn outbox_entry(frame: &OutboundFrameV2) -> Result<Option<OutboxEntry>, serde_json::Error> {
    Ok(match frame {
        OutboundFrameV2::ThreadResult {
            task_id,
            thread_id,
            data,
            ..
        } => Some(OutboxEntry {
            msg_type: "thread_result",
            data: serde_json::to_value(data)?,
            task_id: Some(task_id.clone()),
            secondary_id: Some(thread_id.clone()),
        }),
        OutboundFrameV2::ThreadMessage {
            task_id,
            thread_id,
            data,
            ..
        } => Some(OutboxEntry {
            msg_type: "thread_message",
            data: serde_json::to_value(data)?,
            task_id: Some(task_id.clone()),
            secondary_id: Some(thread_id.clone()),
        }),
        OutboundFrameV2::CronState {
            task_id,
            cron_id,
            data,
            ..
        } => Some(OutboxEntry {
            msg_type: "cron_state",
            data: serde_json::to_value(data)?,
            task_id: Some(task_id.clone()),
            secondary_id: Some(cron_id.clone()),
        }),
        OutboundFrameV2::Heartbeat { .. }
        | OutboundFrameV2::MsgAck { .. }
        | OutboundFrameV2::Ack { .. }
        | OutboundFrameV2::Unknown => None,
    })
}

/// Rebuild the v2 wire envelope from the outbox correlation columns + `data`.
/// Correlation ids sit at the **envelope top level** (never inside `data`); the
/// host routes/correlates off them without parsing `data`. `secondary_id` is the
/// `thread_id` column, keyed as `cronId` for a `cron_state` frame and `threadId`
/// otherwise (see [`secondary_id_key`]). Shared by [`Transport::send`] and the
/// `ws` outbox replay so live sends and replays are byte-identical.
pub(super) fn build_envelope(
    msg_type: &str,
    data: &Value,
    task_id: Option<&str>,
    secondary_id: Option<&str>,
    outbox_id: i64,
) -> Value {
    let mut envelope = Map::new();
    envelope.insert("type".to_owned(), Value::from(msg_type));
    if let Some(task_id) = task_id {
        envelope.insert("taskId".to_owned(), Value::from(task_id));
    }
    if let Some(secondary) = secondary_id {
        envelope.insert(
            secondary_id_key(msg_type).to_owned(),
            Value::from(secondary),
        );
    }
    envelope.insert("data".to_owned(), data.clone());
    envelope.insert("outboxId".to_owned(), Value::from(outbox_id));
    Value::Object(envelope)
}

/// The envelope key for the outbox's secondary correlation column: `cronId` for a
/// `cron_state` frame, `threadId` for the thread frames.
fn secondary_id_key(msg_type: &str) -> &'static str {
    if msg_type == "cron_state" {
        "cronId"
    } else {
        "threadId"
    }
}

// ---------------------------------------------------------------------------
// Shared helpers (used by the ws child module)
// ---------------------------------------------------------------------------

/// Lock the shared DB, recovering the guard from a poisoned mutex rather than
/// panicking (Lock 6: no panics in runtime paths). Critical sections are short
/// and synchronous — the guard is never held across an `.await`.
pub(super) fn lock_db(db: &Mutex<Db>) -> MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A one-shot, broadcast-style cancellation flag. Cheaper than pulling in
/// `tokio-util` for a single token, and race-free via the
/// "set flag → notify_waiters, check flag → enable → recheck" pattern.
#[derive(Debug)]
pub(super) struct Cancel {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Cancel {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Trip the flag and wake every current waiter. Idempotent.
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Resolve once [`Self::cancel`] has been called (immediately if already).
    pub(super) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let mut notified = std::pin::pin!(self.notify.notified());
        // `enable` registers interest *before* the recheck, closing the race
        // where `cancel` runs between the check and the await.
        notified.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

// ---------------------------------------------------------------------------
// Reconnect backoff
// ---------------------------------------------------------------------------

/// Stateful reconnect backoff. [`Self::next_delay`] yields the delay before the
/// next attempt, or `None` once `max_retries` is exhausted; [`Self::reset`] is
/// called after a successful (re)connect.
#[derive(Debug)]
pub(super) struct Backoff {
    config: BackoffConfig,
    attempt: u32,
}

impl Backoff {
    fn new(config: BackoffConfig) -> Self {
        Self { config, attempt: 0 }
    }

    pub(super) fn reset(&mut self) {
        self.attempt = 0;
    }

    pub(super) fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt >= self.config.max_retries {
            return None;
        }
        self.attempt += 1;
        let factor = 2u64.checked_pow(self.attempt - 1).unwrap_or(u64::MAX);
        let base_ms = u64::try_from(self.config.base.as_millis()).unwrap_or(u64::MAX);
        let max_ms = u64::try_from(self.config.max.as_millis()).unwrap_or(u64::MAX);
        let ms = base_ms.saturating_mul(factor).min(max_ms);
        Some(Duration::from_millis(ms))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use secrecy::SecretString;
    use serde_json::{Value, json};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    use super::{Backoff, BackoffConfig, Transport, TransportConfig, build_envelope, outbox_entry};
    use crate::db::Db;
    use crate::protocol::v2::{HeartbeatData, MessageKind, OutboundFrameV2, ThreadMessageData};

    // -- pure helpers ------------------------------------------------------

    #[test]
    fn backoff_schedule_matches_ts_runner() {
        let mut backoff = Backoff::new(BackoffConfig::default());
        // 1 s, 2 s, 4 s, 8 s, then clamped at the 10 s ceiling.
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(4)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(8)));
        for _ in 5..=60 {
            assert_eq!(backoff.next_delay(), Some(Duration::from_secs(10)));
        }
        // 60 attempts exhausted.
        assert_eq!(backoff.next_delay(), None);
        // reset restarts the schedule.
        backoff.reset();
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
    }

    fn sample_message_data() -> ThreadMessageData {
        ThreadMessageData {
            seq: 3,
            kind: MessageKind::AssistantText,
            role: Some("assistant".to_owned()),
            content: Some("hi".to_owned()),
            tool: None,
            input: None,
            output: None,
        }
    }

    #[test]
    fn outbox_entry_splits_data_frames_and_skips_control_frames() {
        let message = OutboundFrameV2::ThreadMessage {
            task_id: "t-1".to_owned(),
            thread_id: "c-9".to_owned(),
            data: sample_message_data(),
            outbox_id: None,
        };
        let entry = outbox_entry(&message).unwrap().unwrap();
        assert_eq!(entry.msg_type, "thread_message");
        // Persisted payload is `data` only — correlation ids ride the columns.
        assert_eq!(entry.task_id.as_deref(), Some("t-1"));
        assert_eq!(entry.secondary_id.as_deref(), Some("c-9"));
        assert_eq!(entry.data["seq"], json!(3));
        assert!(entry.data.get("taskId").is_none());
        assert!(entry.data.get("threadId").is_none());

        // A cron_state frame routes its `cronId` through the same secondary column.
        let cron = OutboundFrameV2::CronState {
            task_id: "t-1".to_owned(),
            cron_id: "cr-1".to_owned(),
            data: crate::protocol::v2::CronStateData {
                kind: "schedule".to_owned(),
                schedule_type: Some("interval".to_owned()),
                schedule_value: Some("60".to_owned()),
                continuity: Some("isolated".to_owned()),
                next_run: None,
                status: Some("active".to_owned()),
                round_count: 2,
                skipped_count: 0,
                last_status: Some("dispatched".to_owned()),
            },
            outbox_id: None,
        };
        let entry = outbox_entry(&cron).unwrap().unwrap();
        assert_eq!(entry.msg_type, "cron_state");
        assert_eq!(entry.secondary_id.as_deref(), Some("cr-1"));

        // Control frames are not outbox-backed.
        assert!(
            outbox_entry(&OutboundFrameV2::Heartbeat {
                data: HeartbeatData {
                    active_threads: None
                },
                outbox_id: None,
            })
            .unwrap()
            .is_none()
        );
        assert!(
            outbox_entry(&OutboundFrameV2::MsgAck { msg_id: 1 })
                .unwrap()
                .is_none()
        );
        assert!(
            outbox_entry(&OutboundFrameV2::Ack { id: 1 })
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn build_envelope_round_trips_thread_frame_with_ids_at_top_level() {
        // The envelope rebuilt from the outbox columns must parse straight back
        // into the typed v2 frame: ids at the top level, `data` nested, the
        // outbox row id surfacing as `outboxId`.
        let data = sample_message_data();
        let value = serde_json::to_value(&data).unwrap();
        let envelope = build_envelope("thread_message", &value, Some("t-1"), Some("c-9@1718"), 43);
        assert_eq!(envelope["type"], json!("thread_message"));
        assert_eq!(envelope["taskId"], json!("t-1"));
        assert_eq!(envelope["threadId"], json!("c-9@1718"));
        assert_eq!(envelope["outboxId"], json!(43));
        assert_eq!(envelope["data"]["seq"], json!(3));

        let parsed: OutboundFrameV2 = serde_json::from_value(envelope).unwrap();
        assert_eq!(
            parsed,
            OutboundFrameV2::ThreadMessage {
                task_id: "t-1".to_owned(),
                thread_id: "c-9@1718".to_owned(),
                data,
                outbox_id: Some(43),
            }
        );
    }

    #[test]
    fn build_envelope_keys_cron_state_secondary_as_cron_id() {
        // The `cron_state` frame's secondary column is its `cronId`, not a thread.
        let data = json!({ "kind": "schedule", "round_count": 1, "skipped_count": 0 });
        let envelope = build_envelope("cron_state", &data, Some("t-1"), Some("cr-7"), 5);
        assert_eq!(envelope["cronId"], json!("cr-7"));
        assert!(envelope.get("threadId").is_none());
    }

    #[test]
    fn heartbeat_constant_matches_serialized_frame() {
        // The manager sends a literal heartbeat string; keep it tied to the type.
        let serialized = serde_json::to_string(&OutboundFrameV2::Heartbeat {
            data: HeartbeatData {
                active_threads: None,
            },
            outbox_id: None,
        })
        .unwrap();
        assert_eq!(serialized, super::ws::HEARTBEAT_FRAME);
    }

    // -- integration against a mock WS server ------------------------------

    type ServerWs = WebSocketStream<TcpStream>;

    fn test_config() -> TransportConfig {
        TransportConfig {
            // Far longer than any test runs, so no heartbeat fires mid-test.
            heartbeat: Duration::from_secs(50),
            backoff: BackoffConfig::for_tests(),
        }
    }

    async fn next_text(ws: &mut ServerWs) -> String {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return text.as_str().to_owned(),
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_))) => {}
                other => panic!("expected a text frame, got {other:?}"),
            }
        }
    }

    async fn wait_until<F: FnMut() -> bool>(mut predicate: F) {
        for _ in 0..200 {
            if predicate() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within timeout");
    }

    // The `accept_hdr_async` callback returns tungstenite's own large
    // `Result<Response, ErrorResponse>`; the shape is fixed by the library.
    #[allow(clippy::result_large_err)]
    #[tokio::test]
    async fn outbox_replays_in_order_carries_correlation_and_ack_clears() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        {
            let guard = db.lock().unwrap();
            // Outbox payload = `data` only; correlation ids ride the columns.
            guard
                .enqueue_outbox_correlated(
                    "thread_message",
                    &json!({ "seq": 1, "kind": "assistant_text", "content": "a" }),
                    Some("t-1"),
                    Some("c-1"),
                )
                .unwrap();
            guard
                .enqueue_outbox_correlated(
                    "thread_result",
                    &json!({ "status": "succeeded", "duration_ms": 5, "final_seq": 1 }),
                    Some("t-1"),
                    Some("c-1"),
                )
                .unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured_token = Arc::new(std::sync::Mutex::new(None::<String>));
        let cap = Arc::clone(&captured_token);

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws =
                tokio_tungstenite::accept_hdr_async(tcp, move |req: &Request, resp: Response| {
                    *cap.lock().unwrap() = req
                        .headers()
                        .get("x-internal-token")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    Ok(resp)
                })
                .await
                .unwrap();
            let first = next_text(&mut ws).await;
            let second = next_text(&mut ws).await;
            ws.send(Message::text(r#"{"type":"ack","id":1}"#.to_owned()))
                .await
                .unwrap();
            ws.send(Message::text(r#"{"type":"ack","id":2}"#.to_owned()))
                .await
                .unwrap();
            (first, second)
        });

        let transport = Transport::open(
            format!("ws://{addr}/agent/ws/agent-1"),
            SecretString::from("connect-secret"),
            Arc::clone(&db),
            test_config(),
        )
        .await
        .unwrap();

        let (first, second) = server.await.unwrap();
        let first: Value = serde_json::from_str(&first).unwrap();
        let second: Value = serde_json::from_str(&second).unwrap();
        // Replayed oldest-first; each envelope is rebuilt from the columns with the
        // correlation ids at the top level and the persisted `data` nested.
        assert_eq!(first["type"], json!("thread_message"));
        assert_eq!(first["outboxId"], json!(1));
        assert_eq!(first["taskId"], json!("t-1"));
        assert_eq!(first["threadId"], json!("c-1"));
        assert_eq!(first["data"]["content"], json!("a"));
        // Correlation ids must not leak into `data`.
        assert!(first["data"].get("taskId").is_none());
        assert!(first["data"].get("threadId").is_none());
        assert_eq!(second["type"], json!("thread_result"));
        assert_eq!(second["outboxId"], json!(2));
        assert_eq!(second["threadId"], json!("c-1"));

        assert_eq!(
            captured_token.lock().unwrap().as_deref(),
            Some("connect-secret"),
            "the connect token must travel in the x-internal-token header"
        );

        // Acks clear the outbox.
        wait_until(|| db.lock().unwrap().list_unacked_outbox().unwrap().is_empty()).await;
        transport.shutdown().await;
    }

    #[tokio::test]
    async fn inbound_message_is_persisted_and_msg_acked() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::text(
                r#"{"type":"message","taskId":"t-1","threadId":"c-1","data":{"text":"hello"},"msgId":7}"#
                    .to_owned(),
            ))
            .await
            .unwrap();
            next_text(&mut ws).await // the msg_ack
        });

        let mut transport = Transport::open(
            format!("ws://{addr}/agent/ws/agent-2"),
            SecretString::from("tok"),
            Arc::clone(&db),
            test_config(),
        )
        .await
        .unwrap();

        let inbound = transport.recv().await.unwrap();
        match &inbound.frame {
            crate::protocol::v2::InboundFrameV2::Message {
                task_id,
                thread_id,
                data,
                msg_id,
            } => {
                assert_eq!(task_id, "t-1");
                assert_eq!(thread_id.as_deref(), Some("c-1"));
                assert_eq!(data.text, "hello");
                assert_eq!(*msg_id, Some(7));
            }
            other => panic!("expected a message frame, got {other:?}"),
        }
        // Persisted to the inbox (dedup source of truth) with its correlation ids
        // captured in the indexed columns, and the FULL frame as the payload.
        {
            let guard = db.lock().unwrap();
            let rows = guard.list_unprocessed_inbox().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].msg_type, "message");
            assert_eq!(rows[0].task_id.as_deref(), Some("t-1"));
            assert_eq!(rows[0].thread_id.as_deref(), Some("c-1"));
            let payload: Value = serde_json::from_str(&rows[0].payload).unwrap();
            assert_eq!(payload["type"], json!("message"));
            assert_eq!(payload["taskId"], json!("t-1"));
            assert_eq!(payload["data"]["text"], json!("hello"));
        }

        let ack: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
        assert_eq!(ack, json!({ "type": "msg_ack", "msgId": 7 }));

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn reconnect_replays_unacked_outbox() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        {
            db.lock()
                .unwrap()
                .enqueue_outbox_correlated(
                    "thread_result",
                    &json!({ "status": "succeeded", "duration_ms": 1, "final_seq": 0 }),
                    Some("t-1"),
                    Some("c-1"),
                )
                .unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            // First connection: read the replayed frame, then drop without acking.
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let first = next_text(&mut ws).await;
            drop(ws); // simulate a dropped connection

            // Second connection: the unacked row must be replayed, then we ack it.
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let second = next_text(&mut ws).await;
            ws.send(Message::text(r#"{"type":"ack","id":1}"#.to_owned()))
                .await
                .unwrap();
            (first, second)
        });

        let transport = Transport::open(
            format!("ws://{addr}/agent/ws/agent-3"),
            SecretString::from("tok"),
            Arc::clone(&db),
            test_config(),
        )
        .await
        .unwrap();

        let (first, second) = server.await.unwrap();
        let first: Value = serde_json::from_str(&first).unwrap();
        let second: Value = serde_json::from_str(&second).unwrap();
        assert_eq!(first["type"], json!("thread_result"));
        assert_eq!(first["outboxId"], json!(1));
        assert_eq!(first["threadId"], json!("c-1"));
        // The very same unacked row is replayed (byte-identical) on the fresh
        // connection — the host de-dups on its own `(thread_id, seq)` key.
        assert_eq!(second["outboxId"], json!(1));
        assert_eq!(second["threadId"], json!("c-1"));

        wait_until(|| db.lock().unwrap().list_unacked_outbox().unwrap().is_empty()).await;
        transport.shutdown().await;
    }
}
