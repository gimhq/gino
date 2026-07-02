//! Reconnecting WebSocket client (PLAN-003 §2, PLAN-001 `ws.rs`).
//!
//! A faithful Rust port of the TypeScript runner's [`ws.ts`](../../../container/gino-agent/src/ws.ts):
//! outbox-backed at-least-once delivery, inbox dedup/replay, bounded-backoff
//! reconnect, and (new here) a periodic heartbeat. The connection is **outbound
//! only** — the runner always dials the orchestrator, never the reverse.
//!
//! ## Authentication
//!
//! The per-agent connect token rides in the `x-internal-token` request header
//! (matching the host WS handler). The bootstrap *register* token is never sent
//! here — only the persisted connect token, reused verbatim across reconnects.
//!
//! ## Delivery model (protocol v2)
//!
//! * **Outbound** — every data frame is persisted to the `outbox` before it is
//!   sent (by [`super::Transport::send`]), storing the frame's `data` plus its
//!   correlation ids in the indexed columns. The driver transmits the freshly
//!   queued envelope immediately and, on every (re)connect, replays *all* unacked
//!   rows oldest-first — rebuilding each `{type, …ids, data, outboxId}` envelope
//!   from the columns via [`build_envelope`], byte-identical to the live send. The
//!   host's `ack {id}` marks a row acked. Stale immediate sends queued while
//!   disconnected are discarded on reconnect so replay does not double-transmit.
//! * **Inbound** — every frame (bar `ack`, and unknown forward-compat frames) is
//!   recorded to the `inbox` *verbatim* (the full received frame is the payload;
//!   the `type` column classifies it and the correlation ids index it), a
//!   `msg_ack` is returned so the host can advance its cursor, and the typed frame
//!   is forwarded to the consumer. The `msg_id` UNIQUE column closes the
//!   at-least-once loop (protocol.md §6): a redelivery of an already-stored
//!   `msg_id` inserts nothing and is only re-acked, so a lost `msg_ack` can never
//!   run the same frame twice. A known-type frame with a malformed body is
//!   dead-lettered (acked and dropped) rather than left to head-of-line-block the
//!   stream. Rows stay unprocessed until the consumer marks them processed
//!   ([`crate::db::Db::mark_inbox_processed`]); `close` is marked processed
//!   *before* delivery to avoid a spurious re-shutdown on crash recovery.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, warn};

use super::{Backoff, Cancel, Inbound, build_envelope, lock_db};
use crate::db::Db;
use crate::protocol::v2::{InboundFrameV2, OutboundFrameV2};

/// The auth header carrying the connect token (host WS handler reads this name).
const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";
/// Inbox poison-pill guard: discard a row after this many PROCESSING failures
/// (`retry_count` is bumped by the consumer when applying the frame fails, never
/// by a reconnect replay — network flaps must not discard a legitimately queued
/// message).
pub(crate) const MAX_INBOX_RETRIES: i64 = 10;
/// Host-supplied heartbeat cadence clamp: `heartbeatIntervalMs: 0` would panic
/// `tokio::time::interval`, and an absurdly large value would silence liveness.
const MIN_HEARTBEAT: Duration = Duration::from_secs(1);
const MAX_HEARTBEAT: Duration = Duration::from_hours(1);
/// Pre-serialized liveness ping; equal to the v2 `OutboundFrameV2::Heartbeat`
/// with no `activeThreads` summary (asserted by a unit test) so the constant
/// never drifts from the type. The runner-driven periodic heartbeat carries no
/// live-work summary; a producer wanting `activeThreads` sends its own frame.
pub(super) const HEARTBEAT_FRAME: &str = r#"{"type":"heartbeat","data":{}}"#;
/// Inbox `type` column values whose replay policy differs from the default:
/// `config_update` is dropped on replay (the host re-pushes config on connect) and
/// `close` is marked processed before delivery. Kept as constants so [`inbox_type`]
/// and the replay policy can never drift apart.
const INBOX_TYPE_CONFIG_UPDATE: &str = "config_update";
const INBOX_TYPE_CLOSE: &str = "close";

/// The client WS stream (TCP, optionally rustls-wrapped for `wss`).
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type TungsteniteError = tokio_tungstenite::tungstenite::Error;

/// Errors raised while dialing the WebSocket.
#[derive(Debug, Error)]
pub(crate) enum WsError {
    /// The URL could not be turned into a handshake request.
    #[error("invalid websocket url: {0}")]
    Request(TungsteniteError),
    /// The connect token was not a valid HTTP header value.
    #[error("connect token is not a valid header value")]
    Header,
    /// The handshake/TLS/TCP dial failed.
    #[error("dial: {0}")]
    Dial(TungsteniteError),
}

/// Dial `ws_url`, authenticating with `connect_token` in the `x-internal-token`
/// header. Used for both the initial connection and every reconnect.
pub(super) async fn dial(ws_url: &str, connect_token: &SecretString) -> Result<WsStream, WsError> {
    let mut request = ws_url.into_client_request().map_err(WsError::Request)?;
    let value =
        HeaderValue::from_str(connect_token.expose_secret()).map_err(|_| WsError::Header)?;
    request.headers_mut().insert(INTERNAL_TOKEN_HEADER, value);
    let (stream, _response) = connect_async(request).await.map_err(WsError::Dial)?;
    Ok(stream)
}

// ---------------------------------------------------------------------------
// Driver task
// ---------------------------------------------------------------------------

/// Owns the connection lifecycle. Built by [`super::Transport::open`] and run on
/// a spawned task; it never returns until cancelled or reconnect is exhausted.
#[derive(Debug)]
pub(super) struct WsDriver {
    pub(super) ws_url: String,
    pub(super) connect_token: SecretString,
    pub(super) db: Arc<Mutex<Db>>,
    pub(super) inbound_tx: mpsc::UnboundedSender<Inbound>,
    pub(super) immediate_rx: mpsc::UnboundedReceiver<String>,
    pub(super) cancel: Arc<Cancel>,
    pub(super) flush: Arc<Notify>,
    pub(super) heartbeat: Duration,
    pub(super) backoff: Backoff,
}

impl WsDriver {
    /// Drive `initial` until it drops, then reconnect-with-backoff in a loop until
    /// cancelled or the backoff is exhausted.
    pub(super) async fn run(mut self, initial: WsStream) {
        let mut current = initial;
        loop {
            // Discard immediate-send envelopes that piled up while disconnected;
            // `serve` replays the outbox, which is the durable source of truth.
            while self.immediate_rx.try_recv().is_ok() {}
            self.serve(current).await;
            if self.cancel.is_cancelled() {
                break;
            }
            match self.reconnect().await {
                Some(stream) => {
                    self.backoff.reset();
                    current = stream;
                }
                None => break,
            }
        }
        debug!("transport manager stopped");
    }

    /// Serve a single live connection: replay outbox + inbox on connect, then
    /// multiplex inbound reads, queued outbound sends, and the heartbeat until the
    /// socket drops or shutdown is requested.
    async fn serve(&mut self, stream: WsStream) {
        let (mut sink, mut read) = stream.split();

        if let Err(err) = replay_outbox(&mut sink, &self.db).await {
            warn!(error = %err, "outbox replay failed on connect; reconnecting");
            return;
        }
        replay_inbox(&self.db, &self.inbound_tx);

        let mut heartbeat = tokio::time::interval(heartbeat_period(self.heartbeat));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                () = self.cancel.cancelled() => {
                    let _ = sink.close().await;
                    return;
                }
                message = read.next() => {
                    match message {
                        Some(Ok(message)) => {
                            if !self.on_message(message, &mut sink).await {
                                return;
                            }
                        }
                        Some(Err(err)) => {
                            warn!(error = %err, "websocket read error; reconnecting");
                            return;
                        }
                        None => {
                            debug!("websocket closed by peer; reconnecting");
                            return;
                        }
                    }
                }
                Some(raw) = self.immediate_rx.recv() => {
                    if sink.send(Message::text(raw)).await.is_err() {
                        return;
                    }
                }
                () = self.flush.notified() => {
                    // A producer enqueued frames straight to the outbox; replay the
                    // unacked rows over the live socket so they ship now (the host
                    // dedups, exactly as on a reconnect replay).
                    if let Err(err) = replay_outbox(&mut sink, &self.db).await {
                        warn!(error = %err, "outbox flush failed; reconnecting");
                        return;
                    }
                }
                _ = heartbeat.tick() => {
                    if sink.send(Message::text(HEARTBEAT_FRAME.to_owned())).await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// Handle one inbound websocket message. Returns `false` when the connection
    /// should be torn down (peer close, or an unrecoverable send error).
    async fn on_message(&self, message: Message, sink: &mut WsSink) -> bool {
        match message {
            Message::Text(text) => {
                match handle_inbound(text.as_str(), sink, &self.db, &self.inbound_tx).await {
                    Ok(()) => true,
                    Err(err) => {
                        warn!(error = %err, "failed to ack an inbound frame; reconnecting");
                        false
                    }
                }
            }
            Message::Ping(payload) => sink.send(Message::Pong(payload)).await.is_ok(),
            Message::Close(_) => {
                debug!("close frame received; reconnecting");
                false
            }
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => true,
        }
    }

    /// Reconnect with bounded backoff. Returns the new stream, or `None` if
    /// cancelled or the backoff is exhausted.
    async fn reconnect(&mut self) -> Option<WsStream> {
        loop {
            let delay = self.backoff.next_delay()?;
            tokio::select! {
                () = self.cancel.cancelled() => return None,
                () = tokio::time::sleep(delay) => {}
            }
            if self.cancel.is_cancelled() {
                return None;
            }
            match dial(&self.ws_url, &self.connect_token).await {
                Ok(stream) => {
                    debug!("reconnected; replaying unacked state");
                    return Some(stream);
                }
                Err(err) => warn!(error = %err, "reconnect attempt failed"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Outbox / inbox replay
// ---------------------------------------------------------------------------

/// Transmit every unacked outbox row, oldest-first, bumping each row's retry
/// counter. DB read/write errors are logged and skipped; only a socket send
/// failure propagates (the caller reconnects).
async fn replay_outbox(sink: &mut WsSink, db: &Arc<Mutex<Db>>) -> Result<(), TungsteniteError> {
    let rows = match lock_db(db).list_unacked_outbox() {
        Ok(rows) => rows,
        Err(err) => {
            warn!(error = %err, "failed to list outbox for replay");
            return Ok(());
        }
    };
    let count = rows.len();
    for row in rows {
        let _ = lock_db(db).increment_outbox_retry(row.id);
        let Ok(data) = serde_json::from_str::<Value>(&row.payload) else {
            warn!(id = row.id, "skipping outbox row with malformed payload");
            continue;
        };
        // Rebuild the v2 envelope from the correlation columns + `data`. The
        // `thread_id` column carries the frame's `threadId` (or, for `cron_state`,
        // its `cronId`); `build_envelope` keys it correctly off the row type.
        let envelope = build_envelope(
            &row.msg_type,
            &data,
            row.task_id.as_deref(),
            row.thread_id.as_deref(),
            row.id,
        );
        sink.send(Message::text(envelope.to_string())).await?;
    }
    if count > 0 {
        debug!(count, "replayed unacked outbox rows");
    }
    Ok(())
}

/// Re-deliver every unprocessed inbox row to the consumer (dedup: rows are not
/// re-recorded). Mirrors the TS `replayInbound`: drop poison rows (rows whose
/// PROCESSING failed [`MAX_INBOX_RETRIES`] times — a replay itself never bumps
/// the counter, so reconnect flaps cannot poison a queued message), skip
/// `system_config` (the host re-pushes config), mark `system_close` processed
/// before delivery, and defer the rest until the consumer acknowledges them.
fn replay_inbox(db: &Arc<Mutex<Db>>, inbound_tx: &mpsc::UnboundedSender<Inbound>) {
    let rows = match lock_db(db).list_unprocessed_inbox() {
        Ok(rows) => rows,
        Err(err) => {
            warn!(error = %err, "failed to list inbox for replay");
            return;
        }
    };
    let mut replayed = 0usize;
    for row in rows {
        if row.retry_count >= MAX_INBOX_RETRIES {
            warn!(
                id = row.id,
                "inbox row exceeded the processing-failure limit; marking processed"
            );
            let _ = lock_db(db).mark_inbox_processed(row.id);
            continue;
        }

        let Ok(frame) = serde_json::from_str::<InboundFrameV2>(&row.payload) else {
            warn!(
                id = row.id,
                "marking inbox row with malformed payload processed"
            );
            let _ = lock_db(db).mark_inbox_processed(row.id);
            continue;
        };
        // A frame we no longer model (forward-compat) is never re-delivered.
        if matches!(frame, InboundFrameV2::Unknown) {
            let _ = lock_db(db).mark_inbox_processed(row.id);
            continue;
        }

        // `config_update`: the host re-pushes config on connect, so a stale row is
        // dropped rather than replayed.
        if row.msg_type == INBOX_TYPE_CONFIG_UPDATE {
            let _ = lock_db(db).mark_inbox_processed(row.id);
            continue;
        }
        // `close`: mark processed *before* delivery to avoid a spurious
        // re-shutdown if we crash mid-handoff.
        if row.msg_type == INBOX_TYPE_CLOSE {
            let _ = lock_db(db).mark_inbox_processed(row.id);
        }
        let _ = inbound_tx.send(Inbound {
            inbox_id: row.id,
            frame,
        });
        replayed += 1;
    }
    if replayed > 0 {
        debug!(count = replayed, "replayed unprocessed inbox rows");
    }
}

// ---------------------------------------------------------------------------
// Inbound handling
// ---------------------------------------------------------------------------

/// The socket-free decision for one received text frame, made by
/// [`classify_inbound`] and acted on by [`handle_inbound`] (which owns the sink).
#[derive(Debug)]
enum InboundDisposition {
    /// Nothing to send or deliver: an outbox `ack` (already applied), a
    /// forward-compat unknown frame, or junk with no ackable `msgId`.
    Done,
    /// Send a `msg_ack` but deliver nothing: a duplicate redelivery of an
    /// already-stored `msg_id`, or a dead-lettered malformed known-type frame.
    AckOnly { msg_id: i64 },
    /// Freshly persisted: ack durable receipt (when the frame carries a
    /// `msg_id`) and forward to the consumer. The frame is boxed to keep the
    /// enum's ack-only variants small.
    Deliver {
        frame: Box<InboundFrameV2>,
        inbox_id: i64,
        msg_id: Option<i64>,
    },
}

/// Route a freshly received text frame: ack our outbox, drop a forward-compat
/// unknown frame, or persist + msg_ack + forward an inbound frame. The full frame
/// is stored to the inbox verbatim (the host's wire bytes, parsed once as a
/// [`Value`] for storage and once as the typed frame for routing); the `msg_id`
/// UNIQUE column dedups an at-least-once redelivery, which is re-acked without a
/// second row. Parse failures are dead-lettered (acked away when a `msgId` is
/// extractable, so a permanently malformed frame cannot head-of-line-block the
/// stream) or logged and swallowed; only a `msg_ack` send failure propagates.
async fn handle_inbound(
    text: &str,
    sink: &mut WsSink,
    db: &Arc<Mutex<Db>>,
    inbound_tx: &mpsc::UnboundedSender<Inbound>,
) -> Result<(), TungsteniteError> {
    match classify_inbound(text, db) {
        InboundDisposition::Done => Ok(()),
        InboundDisposition::AckOnly { msg_id } => send_msg_ack(sink, msg_id).await,
        InboundDisposition::Deliver {
            frame,
            inbox_id,
            msg_id,
        } => {
            // Confirm durable receipt so the host can advance its cursor.
            if let Some(msg_id) = msg_id {
                send_msg_ack(sink, msg_id).await?;
            }
            deliver(*frame, inbox_id, db, inbound_tx);
            Ok(())
        }
    }
}

/// Parse, dedup, and persist one received text frame (everything except the
/// socket IO, so the at-least-once decisions are unit-testable).
fn classify_inbound(text: &str, db: &Arc<Mutex<Db>>) -> InboundDisposition {
    let frame = match serde_json::from_str::<InboundFrameV2>(text) {
        Ok(frame) => frame,
        // An unknown `type` parses into `Unknown`, so a typed-parse failure is a
        // KNOWN type with a malformed body (or unusable JSON): dead-letter it.
        Err(err) => return dead_letter(text, &err),
    };

    // The host's `ack {id}` confirms one of our outbox rows.
    if let InboundFrameV2::Ack { id } = frame {
        let _ = lock_db(db).mark_outbox_acked(id);
        return InboundDisposition::Done;
    }

    // A frame `type` this runner does not model (additive-within-major): ignore it
    // rather than persist/ack/deliver a frame we cannot act on.
    if matches!(frame, InboundFrameV2::Unknown) {
        debug!("ignoring unknown inbound v2 frame");
        return InboundDisposition::Done;
    }

    // Persist the FULL received frame (verbatim) for at-least-once before doing
    // anything else; the `type` column classifies and the columns index it.
    let raw = match serde_json::from_str::<Value>(text) {
        Ok(raw) => raw,
        Err(err) => {
            // Unreachable in practice — the typed parse above already succeeded —
            // but never persist a payload we cannot re-deliver verbatim.
            warn!(error = %err, "inbound frame is not storable json; dropping");
            return InboundDisposition::Done;
        }
    };
    let inbox_type = inbox_type(&frame);
    let (task_id, secondary_id) = correlation_of(&frame);
    let msg_id = msg_id_of(&frame);
    match lock_db(db).record_inbound_correlated(inbox_type, &raw, task_id, secondary_id, msg_id) {
        Ok(Some(inbox_id)) => InboundDisposition::Deliver {
            frame: Box::new(frame),
            inbox_id,
            msg_id,
        },
        // `msg_id` already stored: a redelivery after a lost `msg_ack`. Re-ack so
        // the host advances; the original row already drives (or drove) delivery.
        Ok(None) => match msg_id {
            Some(msg_id) => {
                debug!(msg_id, "duplicate inbound delivery; re-acking");
                InboundDisposition::AckOnly { msg_id }
            }
            // Unreachable: only a `msg_id` collision can conflict.
            None => InboundDisposition::Done,
        },
        Err(err) => {
            warn!(error = %err, "failed to persist inbound frame; dropping");
            InboundDisposition::Done
        }
    }
}

/// Dead-letter a frame that failed the typed parse: extract the top-level
/// `msgId` so the row can be acked away (the host must not resend it forever);
/// with no extractable `msgId` there is nothing to ack — drop it.
fn dead_letter(text: &str, err: &serde_json::Error) -> InboundDisposition {
    let msg_id = serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|raw| raw.get("msgId")?.as_i64());
    if let Some(msg_id) = msg_id {
        warn!(error = %err, msg_id, "dead-lettering malformed inbound frame (acked, dropped)");
        InboundDisposition::AckOnly { msg_id }
    } else {
        warn!(error = %err, "ignoring unparsable inbound frame");
        InboundDisposition::Done
    }
}

/// Send a `msg_ack {msgId}` over the live sink.
async fn send_msg_ack(sink: &mut WsSink, msg_id: i64) -> Result<(), TungsteniteError> {
    if let Ok(ack) = serde_json::to_string(&OutboundFrameV2::MsgAck { msg_id }) {
        sink.send(Message::text(ack)).await?;
    }
    Ok(())
}

/// The effective heartbeat cadence: the host-supplied interval clamped into
/// [`MIN_HEARTBEAT`, `MAX_HEARTBEAT`] so `heartbeatIntervalMs: 0` (which would
/// panic `tokio::time::interval`) or an absurd value cannot break the transport.
fn heartbeat_period(requested: Duration) -> Duration {
    requested.clamp(MIN_HEARTBEAT, MAX_HEARTBEAT)
}

/// Forward a frame to the consumer. `close` is marked processed *before* delivery
/// (avoids a spurious re-shutdown if we crash mid-handoff); everything else stays
/// unprocessed until the consumer acknowledges it.
fn deliver(
    frame: InboundFrameV2,
    inbox_id: i64,
    db: &Arc<Mutex<Db>>,
    inbound_tx: &mpsc::UnboundedSender<Inbound>,
) {
    if matches!(frame, InboundFrameV2::Close { .. }) {
        let _ = lock_db(db).mark_inbox_processed(inbox_id);
    }
    if inbound_tx.send(Inbound { inbox_id, frame }).is_err() {
        debug!("no inbound consumer; frame retained in inbox for replay");
    }
}

/// The inbox `type` column value for a v2 frame — its wire `type` verbatim. The
/// column drives the replay policy (`config_update` dropped, `close` pre-marked)
/// and lets a consumer filter via [`crate::db::Db::list_inbox_by_type`]. `ack` and
/// `unknown` are intercepted before persistence and never reach here.
fn inbox_type(frame: &InboundFrameV2) -> &'static str {
    match frame {
        InboundFrameV2::TaskUpsert { .. } => "task_upsert",
        InboundFrameV2::TaskDelete { .. } => "task_delete",
        InboundFrameV2::CronUpsert { .. } => "cron_upsert",
        InboundFrameV2::CronDelete { .. } => "cron_delete",
        InboundFrameV2::Message { .. } => "message",
        InboundFrameV2::Interrupt { .. } => "interrupt",
        InboundFrameV2::Close { .. } => INBOX_TYPE_CLOSE,
        InboundFrameV2::ConfigUpdate { .. } => INBOX_TYPE_CONFIG_UPDATE,
        InboundFrameV2::Ack { .. } => "ack",
        InboundFrameV2::Unknown => "unknown",
    }
}

/// The `(task_id, secondary_id)` correlation ids for the indexed inbox columns.
/// `secondary_id` is the frame's `threadId` or, for the cron envelopes, its
/// `cronId` (both share the single `thread_id` column; the full frame in `payload`
/// remains the source of truth on replay).
fn correlation_of(frame: &InboundFrameV2) -> (Option<&str>, Option<&str>) {
    match frame {
        InboundFrameV2::TaskUpsert { task_id, .. } | InboundFrameV2::TaskDelete { task_id, .. } => {
            (Some(task_id), None)
        }
        InboundFrameV2::CronUpsert {
            task_id, cron_id, ..
        }
        | InboundFrameV2::CronDelete {
            task_id, cron_id, ..
        } => (Some(task_id), Some(cron_id)),
        InboundFrameV2::Message {
            task_id, thread_id, ..
        } => (Some(task_id), thread_id.as_deref()),
        InboundFrameV2::Interrupt { .. }
        | InboundFrameV2::Close { .. }
        | InboundFrameV2::ConfigUpdate { .. }
        | InboundFrameV2::Ack { .. }
        | InboundFrameV2::Unknown => (None, None),
    }
}

/// The host inbox-message id to acknowledge, if the frame carries one.
fn msg_id_of(frame: &InboundFrameV2) -> Option<i64> {
    match frame {
        InboundFrameV2::TaskUpsert { msg_id, .. }
        | InboundFrameV2::TaskDelete { msg_id, .. }
        | InboundFrameV2::CronUpsert { msg_id, .. }
        | InboundFrameV2::CronDelete { msg_id, .. }
        | InboundFrameV2::Message { msg_id, .. }
        | InboundFrameV2::Interrupt { msg_id }
        | InboundFrameV2::Close { msg_id }
        | InboundFrameV2::ConfigUpdate { msg_id, .. } => *msg_id,
        InboundFrameV2::Ack { .. } | InboundFrameV2::Unknown => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use serde_json::json;

    use super::{
        HEARTBEAT_FRAME, INBOX_TYPE_CLOSE, INBOX_TYPE_CONFIG_UPDATE, InboundDisposition,
        MAX_HEARTBEAT, MIN_HEARTBEAT, classify_inbound, correlation_of, heartbeat_period,
        inbox_type, msg_id_of,
    };
    use crate::db::Db;
    use crate::protocol::v2::{InboundFrameV2, MessageInData};

    fn message(thread_id: Option<&str>) -> InboundFrameV2 {
        InboundFrameV2::Message {
            task_id: "t-1".to_owned(),
            thread_id: thread_id.map(str::to_owned),
            data: MessageInData {
                text: "hi".to_owned(),
                attachments: None,
            },
            msg_id: Some(3),
        }
    }

    #[test]
    fn inbox_type_is_the_wire_type_for_every_shape() {
        assert_eq!(inbox_type(&message(Some("c-1"))), "message");
        assert_eq!(
            inbox_type(&InboundFrameV2::ConfigUpdate {
                data: json!({}),
                msg_id: None
            }),
            INBOX_TYPE_CONFIG_UPDATE
        );
        assert_eq!(
            inbox_type(&InboundFrameV2::Close { msg_id: None }),
            INBOX_TYPE_CLOSE
        );
        assert_eq!(
            inbox_type(&InboundFrameV2::TaskUpsert {
                task_id: "t".to_owned(),
                data: crate::protocol::v2::TaskUpsertData {
                    name: None,
                    kind: None,
                    status: None,
                    entries: None,
                },
                msg_id: None,
            }),
            "task_upsert"
        );
        assert_eq!(
            inbox_type(&InboundFrameV2::TaskDelete {
                task_id: "t".to_owned(),
                msg_id: None,
            }),
            "task_delete"
        );
        assert_eq!(
            inbox_type(&InboundFrameV2::CronDelete {
                task_id: "t".to_owned(),
                cron_id: "cr".to_owned(),
                msg_id: None,
            }),
            "cron_delete"
        );
        assert_eq!(
            inbox_type(&InboundFrameV2::Interrupt { msg_id: Some(1) }),
            "interrupt"
        );
    }

    #[test]
    fn correlation_extracts_task_and_secondary_ids() {
        // A message carries task + (optional) thread.
        assert_eq!(
            correlation_of(&message(Some("c-1"))),
            (Some("t-1"), Some("c-1"))
        );
        assert_eq!(correlation_of(&message(None)), (Some("t-1"), None));
        // A cron envelope routes its `cronId` through the secondary slot.
        assert_eq!(
            correlation_of(&InboundFrameV2::CronDelete {
                task_id: "t-1".to_owned(),
                cron_id: "cr-9".to_owned(),
                msg_id: None,
            }),
            (Some("t-1"), Some("cr-9"))
        );
        // Control frames carry no correlation ids.
        assert_eq!(
            correlation_of(&InboundFrameV2::Close { msg_id: None }),
            (None, None)
        );
    }

    #[test]
    fn msg_id_extraction() {
        assert_eq!(msg_id_of(&message(Some("c-1"))), Some(3));
        assert_eq!(msg_id_of(&InboundFrameV2::Ack { id: 9 }), None);
        assert_eq!(
            msg_id_of(&InboundFrameV2::ConfigUpdate {
                data: json!({}),
                msg_id: Some(4)
            }),
            Some(4)
        );
        assert_eq!(
            msg_id_of(&InboundFrameV2::Close { msg_id: Some(5) }),
            Some(5)
        );
        assert_eq!(msg_id_of(&InboundFrameV2::Unknown), None);
    }

    #[test]
    fn heartbeat_frame_carries_empty_data_object() {
        // The constant is the v2 liveness ping with no `activeThreads` summary.
        assert_eq!(HEARTBEAT_FRAME, r#"{"type":"heartbeat","data":{}}"#);
    }

    /// P1-12 regression: a host-supplied `heartbeatIntervalMs: 0` (or an absurd
    /// value) is clamped, never handed to `tokio::time::interval` (which panics
    /// on zero).
    #[test]
    fn heartbeat_period_clamps_zero_and_absurd_values() {
        assert_eq!(heartbeat_period(Duration::ZERO), MIN_HEARTBEAT);
        assert_eq!(
            heartbeat_period(Duration::from_hours(24 * 365)),
            MAX_HEARTBEAT
        );
        // A sane value passes through unchanged.
        assert_eq!(
            heartbeat_period(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    fn test_db() -> Arc<Mutex<Db>> {
        Arc::new(Mutex::new(Db::open_in_memory().unwrap()))
    }

    const MESSAGE_FRAME: &str =
        r#"{"type":"message","taskId":"t-1","threadId":"c-1","data":{"text":"hi"},"msgId":7}"#;

    /// P0-3 regression: delivering the same `msgId` twice stores exactly ONE
    /// inbox row and delivers ONE frame; the duplicate is only re-acked (so a
    /// lost `msg_ack` cannot run the same prompt twice).
    #[test]
    fn duplicate_msg_id_is_reacked_not_restored() {
        let db = test_db();

        let first = classify_inbound(MESSAGE_FRAME, &db);
        assert!(
            matches!(
                first,
                InboundDisposition::Deliver {
                    msg_id: Some(7),
                    ..
                }
            ),
            "first delivery persists and delivers: {first:?}"
        );

        let dup = classify_inbound(MESSAGE_FRAME, &db);
        assert!(
            matches!(dup, InboundDisposition::AckOnly { msg_id: 7 }),
            "redelivery is re-acked without delivery: {dup:?}"
        );
        assert_eq!(
            db.lock().unwrap().list_unprocessed_inbox().unwrap().len(),
            1,
            "exactly one inbox row for the deduped delivery"
        );
    }

    /// P1-12 regression: a KNOWN-type frame with a malformed body is acked away
    /// (dead-lettered) rather than left unpersisted+unacked, where the host
    /// would resend it forever and head-of-line-block the stream.
    #[test]
    fn malformed_known_type_frame_is_dead_lettered_with_ack() {
        let db = test_db();
        // `data.text` must be a string; a number fails the typed parse.
        let malformed = r#"{"type":"message","taskId":"t-1","data":{"text":12345},"msgId":9}"#;
        let disposition = classify_inbound(malformed, &db);
        assert!(
            matches!(disposition, InboundDisposition::AckOnly { msg_id: 9 }),
            "malformed known-type frame is acked per the dead-letter policy: {disposition:?}"
        );
        assert!(
            db.lock()
                .unwrap()
                .list_unprocessed_inbox()
                .unwrap()
                .is_empty(),
            "a dead-lettered frame is not queued for processing"
        );

        // A well-formed frame right after flows normally — the stream is not blocked.
        assert!(matches!(
            classify_inbound(MESSAGE_FRAME, &db),
            InboundDisposition::Deliver { .. }
        ));
    }

    /// Junk with no extractable `msgId` (or non-JSON) has nothing to ack: dropped.
    #[test]
    fn unusable_junk_is_dropped_without_ack() {
        let db = test_db();
        assert!(matches!(
            classify_inbound("not json at all", &db),
            InboundDisposition::Done
        ));
        assert!(matches!(
            classify_inbound(r#"{"type":"message","data":{"text":1}}"#, &db),
            InboundDisposition::Done
        ));
        assert!(
            db.lock()
                .unwrap()
                .list_unprocessed_inbox()
                .unwrap()
                .is_empty()
        );
    }

    /// A forward-compat unknown `type` is ignored (not persisted, not acked).
    #[test]
    fn unknown_type_is_ignored() {
        let db = test_db();
        let disposition =
            classify_inbound(r#"{"type":"brand_new_frame","data":{},"msgId":3}"#, &db);
        assert!(matches!(disposition, InboundDisposition::Done));
        assert!(
            db.lock()
                .unwrap()
                .list_unprocessed_inbox()
                .unwrap()
                .is_empty()
        );
    }
}
