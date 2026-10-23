//! Reconnecting WebSocket client.
//!
//! A Rust port of the TypeScript runner's [`ws.ts`](../../../container/gino-agent/src/ws.ts):
//! outbox-backed at-least-once delivery, inbox dedup/replay, capped-backoff
//! reconnect (the delay is bounded, the attempts never are), and (new
//! here) a periodic heartbeat plus read-liveness detection of half-open sockets.
//! The connection is **outbound only** — the runner always dials the
//! orchestrator, never the reverse.
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
//!   sent (producers enqueue via [`crate::db::Db::enqueue_outbox_correlated`] and
//!   poke the flush handle), storing the frame's `data` plus its
//!   correlation ids in the indexed columns. On every (re)connect the driver
//!   replays *all* unacked rows oldest-first — rebuilding each `{type, …ids,
//!   data, outboxId}` envelope from the columns via [`build_envelope`],
//!   byte-identical to the live send. Between reconnects a flush poke transmits
//!   only the rows **not yet sent on this connection** (a per-connection
//!   watermark: flushing under ack lag must not retransmit the whole
//!   backlog every time); rows whose acks stall for a full ack window are
//!   retransmitted in bulk. The host's `ack {id}` marks a row acked.
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
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, warn};

use super::{Backoff, Cancel, Inbound, build_envelope, lock_db};
use crate::db::Db;
use crate::protocol::v2::{InboundFrameV2, OutboundFrameV2};
use crate::remote_ops::{self, RemoteOps};

/// The auth header carrying the connect token (host WS handler reads this name).
const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";
/// Inbox poison-pill guard: discard a row after this many PROCESSING failures
/// (`retry_count` is bumped by the consumer when applying the frame fails, never
/// by a reconnect replay — network flaps must not discard a legitimately queued
/// message).
pub(crate) const MAX_INBOX_RETRIES: i64 = 10;
/// Outbox poison-pill guard (audit P3), mirroring [`MAX_INBOX_RETRIES`]: a row
/// whose payload no longer parses can never be rebuilt into an envelope, so
/// after this many replay attempts it is dead-lettered
/// ([`crate::db::Db::mark_outbox_dead`]) instead of replayed forever. Healthy
/// rows never reach the guard — only the malformed-payload branch counts, so
/// ack lag can never kill a deliverable frame.
pub(crate) const MAX_OUTBOX_RETRIES: i64 = 10;
/// Host-supplied heartbeat cadence clamp: `heartbeatIntervalMs: 0` would panic
/// `tokio::time::interval`, and an absurdly large value would silence liveness.
const MIN_HEARTBEAT: Duration = Duration::from_secs(1);
const MAX_HEARTBEAT: Duration = Duration::from_hours(1);
/// Per-attempt dial bound: a black-holed TCP connect (or a peer that
/// accepts but never completes the WS handshake) must not stall a reconnect
/// attempt indefinitely — the backoff loop moves on to the next attempt.
pub(super) const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// Pre-serialized liveness ping; equal to the v2 `OutboundFrameV2::Heartbeat`
/// with no `activeThreads` summary (asserted by a unit test) so the constant
/// never drifts from the type. The runner-driven periodic heartbeat carries no
/// live-work summary; a producer wanting `activeThreads` sends its own frame.
pub(super) const HEARTBEAT_FRAME: &str = r#"{"type":"heartbeat","data":{}}"#;
/// Inbox `type` column values whose replay policy differs from the default:
/// `config_update` is dropped on replay (the host re-pushes config on connect),
/// `interrupt` is retired on replay (live-only / at-most-once, protocol §5), and
/// `close` is marked processed before delivery. Kept as constants so [`inbox_type`]
/// and the replay policy can never drift apart.
const INBOX_TYPE_CONFIG_UPDATE: &str = "config_update";
const INBOX_TYPE_INTERRUPT: &str = "interrupt";
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
    /// The dial did not complete within the per-attempt bound.
    #[error("dial timed out after {0:?}")]
    Timeout(Duration),
}

/// Dial `ws_url`, authenticating with `connect_token` in the `x-internal-token`
/// header, bounded by `timeout` per attempt (a black-holed connect must
/// not stall the reconnect loop). Used for both the initial connection and
/// every reconnect, with [`DIAL_TIMEOUT`].
pub(super) async fn dial(
    ws_url: &str,
    connect_token: &SecretString,
    timeout: Duration,
) -> Result<WsStream, WsError> {
    let mut request = ws_url.into_client_request().map_err(WsError::Request)?;
    let value =
        HeaderValue::from_str(connect_token.expose_secret()).map_err(|_| WsError::Header)?;
    request.headers_mut().insert(INTERNAL_TOKEN_HEADER, value);
    let (stream, _response) = tokio::time::timeout(timeout, connect_async(request))
        .await
        .map_err(|_elapsed| WsError::Timeout(timeout))?
        .map_err(WsError::Dial)?;
    Ok(stream)
}

// ---------------------------------------------------------------------------
// Driver task
// ---------------------------------------------------------------------------

/// Owns the connection lifecycle. Built by [`super::Transport::open`] and run on
/// a spawned task; it never returns until cancelled — reconnect never gives up.
#[derive(Debug)]
pub(super) struct WsDriver {
    pub(super) ws_url: String,
    pub(super) connect_token: SecretString,
    pub(super) db: Arc<Mutex<Db>>,
    pub(super) inbound_tx: mpsc::UnboundedSender<Inbound>,
    pub(super) cancel: Arc<Cancel>,
    pub(super) flush: Arc<Notify>,
    pub(super) heartbeat: Duration,
    pub(super) read_timeout: Duration,
    pub(super) ack_timeout: Duration,
    pub(super) backoff: Backoff,
    pub(super) remote_ops: Arc<RemoteOps>,
}

impl WsDriver {
    /// Drive `initial` until it drops, then reconnect-with-backoff in a loop
    /// until cancelled.
    pub(super) async fn run(mut self, initial: WsStream) {
        let mut current = initial;
        loop {
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
    /// multiplex inbound reads, queued outbound sends, the heartbeat, the
    /// ack-progress check, and the read-liveness deadline until the socket drops
    /// or shutdown is requested.
    async fn serve(&mut self, stream: WsStream) {
        let (live_tx, mut live_rx) = mpsc::channel(64);
        let mut ptys = remote_ops::pty::Sessions::default();
        self.serve_connection(stream, &mut ptys, &live_tx, &mut live_rx)
            .await;
        drop(live_rx);
        ptys.shutdown().await;
    }

    async fn serve_connection(
        &mut self,
        stream: WsStream,
        ptys: &mut remote_ops::pty::Sessions,
        live_tx: &mpsc::Sender<String>,
        live_rx: &mut mpsc::Receiver<String>,
    ) {
        let (mut sink, mut read) = stream.split();

        // Full replay on (re)connect — the one moment every unacked row is
        // retransmitted. The returned watermark makes later flushes incremental.
        let mut sent_watermark = match replay_outbox(&mut sink, &self.db, 0).await {
            Ok(watermark) => watermark,
            Err(err) => {
                warn!(error = %err, "outbox replay failed on connect; reconnecting");
                return;
            }
        };
        replay_inbox(&self.db, &self.inbound_tx);

        let heartbeat_period = clamp_period(self.heartbeat);
        let mut heartbeat = tokio::time::interval(heartbeat_period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await; // consume the immediate first tick

        // Ack-progress check: if the oldest transmitted-but-unacked row
        // is unchanged across two consecutive ticks, retransmit the backlog.
        let mut ack_check = tokio::time::interval(clamp_period(self.ack_timeout));
        ack_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ack_check.tick().await;
        let mut stalled_oldest: Option<i64> = None;

        // Read-liveness: every heartbeat also sends a WS ping, so a live
        // peer produces at least pong traffic; a window with nothing received at
        // all means a half-open socket — tear down instead of waiting for the
        // ~15-minute TCP retransmit timeout.
        let read_window = read_window(self.read_timeout, heartbeat_period);
        let mut last_rx = tokio::time::Instant::now();

        loop {
            tokio::select! {
                () = self.cancel.cancelled() => {
                    let _ = sink.close().await;
                    return;
                }
                message = read.next() => {
                    last_rx = tokio::time::Instant::now();
                    match message {
                        Some(Ok(message)) => {
                            if !self.on_message(message, &mut sink, ptys, live_tx).await {
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
                () = self.flush.notified() => {
                    // A producer enqueued frames straight to the outbox; ship the
                    // rows not yet sent on this connection (incremental — already
                    // transmitted rows wait for an ack, a stall, or a reconnect).
                    match replay_outbox(&mut sink, &self.db, sent_watermark).await {
                        Ok(watermark) => sent_watermark = watermark,
                        Err(err) => {
                            warn!(error = %err, "outbox flush failed; reconnecting");
                            return;
                        }
                    }
                }
                Some(frame) = live_rx.recv() => {
                    if !matches!(tokio::time::timeout(Duration::from_secs(5), sink.send(Message::text(frame))).await, Ok(Ok(()))) {
                        return;
                    }
                }
                _ = heartbeat.tick() => {
                    if sink.send(Message::text(HEARTBEAT_FRAME.to_owned())).await.is_err() {
                        return;
                    }
                    // Solicit a pong so read-liveness sees traffic from a peer
                    // that has nothing else to send.
                    if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                        return;
                    }
                }
                _ = ack_check.tick() => {
                    if ack_stalled(&self.db, sent_watermark, &mut stalled_oldest) {
                        debug!("no ack progress within the ack window; retransmitting unacked outbox");
                        match replay_outbox(&mut sink, &self.db, 0).await {
                            Ok(watermark) => sent_watermark = watermark,
                            Err(err) => {
                                warn!(error = %err, "outbox retransmit failed; reconnecting");
                                return;
                            }
                        }
                    }
                }
                () = tokio::time::sleep_until(last_rx + read_window) => {
                    warn!("nothing received within the read-liveness window; treating the connection as dead");
                    return;
                }
            }
        }
    }

    /// Handle one inbound websocket message. Returns `false` when the connection
    /// should be torn down (peer close, or an unrecoverable send error).
    async fn on_message(
        &self,
        message: Message,
        sink: &mut WsSink,
        ptys: &mut remote_ops::pty::Sessions,
        live_tx: &mpsc::Sender<String>,
    ) -> bool {
        match message {
            Message::Text(text) => {
                if let Some(frame) = remote_ops::pty::parse(text.as_str()) {
                    ptys.handle(frame, &self.remote_ops, live_tx);
                    return true;
                }
                // Live-only: answered off the socket task, bypassing the inbox.
                if let Some(request) = remote_ops::parse_request(text.as_str()) {
                    let remote_ops = Arc::clone(&self.remote_ops);
                    let live_tx = live_tx.clone();
                    tokio::spawn(async move {
                        let _ = live_tx.send(remote_ops.respond(request).await).await;
                    });
                    return true;
                }
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

    /// Reconnect with capped backoff (the delay is bounded, the attempts are
    /// not). Returns the new stream, or `None` once cancelled.
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
            match dial(&self.ws_url, &self.connect_token, DIAL_TIMEOUT).await {
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

/// Transmit the unacked outbox rows with id above `after_id`, oldest-first,
/// bumping each transmitted row's retry counter. `after_id = 0` is the full
/// replay (reconnect / ack-stall retransmit); a flush passes the connection's
/// send watermark so only fresh rows ship. Returns the new watermark —
/// the highest row id considered. DB read/write errors are logged and skipped;
/// only a socket send failure propagates (the caller reconnects).
async fn replay_outbox(
    sink: &mut WsSink,
    db: &Arc<Mutex<Db>>,
    after_id: i64,
) -> Result<i64, TungsteniteError> {
    let (envelopes, watermark) = collect_outbox_replay(db, after_id);
    let count = envelopes.len();
    for envelope in envelopes {
        sink.send(Message::text(envelope)).await?;
    }
    if count > 0 {
        debug!(count, after_id, "transmitted unacked outbox rows");
    }
    Ok(watermark)
}

/// Build the envelope for every replayable unacked outbox row with id above
/// `after_id`, oldest-first, bumping each row's retry counter. A row whose
/// payload fails to parse is skipped, and — since that failure is deterministic
/// and will recur on every replay — dead-lettered once its replay count reaches
/// [`MAX_OUTBOX_RETRIES`] (the outbound poison policy, audit P3). Returns the
/// envelopes plus the new send watermark (the highest row id considered, or
/// `after_id` when none qualified). Split from [`replay_outbox`] so the per-row
/// policy is unit-testable without a live socket.
fn collect_outbox_replay(db: &Arc<Mutex<Db>>, after_id: i64) -> (Vec<String>, i64) {
    let rows = match lock_db(db).list_unacked_outbox() {
        Ok(rows) => rows,
        Err(err) => {
            warn!(error = %err, "failed to list outbox for replay");
            return (Vec::new(), after_id);
        }
    };
    let mut watermark = after_id;
    let mut envelopes = Vec::with_capacity(rows.len());
    for row in rows {
        if row.id <= after_id {
            continue;
        }
        watermark = watermark.max(row.id);
        let _ = lock_db(db).increment_outbox_retry(row.id);
        let attempts = row.retry_count + 1;
        let Ok(data) = serde_json::from_str::<Value>(&row.payload) else {
            if attempts >= MAX_OUTBOX_RETRIES {
                warn!(
                    id = row.id,
                    attempts,
                    "outbox row payload malformed after repeated replays; dead-lettering (poison)"
                );
                let _ = lock_db(db).mark_outbox_dead(row.id);
            } else {
                warn!(id = row.id, "skipping outbox row with malformed payload");
            }
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
        envelopes.push(envelope.to_string());
    }
    (envelopes, watermark)
}

/// Whether ack progress has stalled: `stalled` carries the oldest
/// transmitted-but-unacked row id observed on the previous check; seeing the
/// SAME id again means the host acked nothing for a full ack window, so the
/// caller retransmits the backlog. Any progress (a different oldest, or no
/// outstanding rows at all) just records the new observation.
fn ack_stalled(db: &Arc<Mutex<Db>>, watermark: i64, stalled: &mut Option<i64>) -> bool {
    let oldest = match lock_db(db).list_unacked_outbox() {
        // Rows are ordered by id, so the first at-or-below the watermark is the
        // oldest row already transmitted on this connection.
        Ok(rows) => rows.iter().map(|row| row.id).find(|id| *id <= watermark),
        Err(err) => {
            warn!(error = %err, "failed to inspect outbox for ack progress");
            None
        }
    };
    if oldest.is_some() && *stalled == oldest {
        *stalled = None;
        return true;
    }
    *stalled = oldest;
    false
}

/// Re-deliver every unprocessed inbox row to the consumer (dedup: rows are not
/// re-recorded). Mirrors the TS `replayInbound`: drop poison rows (rows whose
/// PROCESSING failed [`MAX_INBOX_RETRIES`] times — a replay itself never bumps
/// the counter, so reconnect flaps cannot poison a queued message), skip
/// `system_config` (the host re-pushes config) and `interrupt` (live-only,
/// protocol §5 — a stale cancel must not fire against a future turn), mark
/// `system_close` processed before delivery, and defer the rest until the
/// consumer acknowledges them.
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
        // `interrupt`: protocol §5 declares delivery LIVE-ONLY (at-most-once) —
        // a cancel expresses a NOW intent and must never fire against a future
        // turn, so a row that survived a crash/reconnect is retired, not replayed.
        if row.msg_type == INBOX_TYPE_INTERRUPT {
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

/// The effective cadence for a periodic driver timer (heartbeat / ack check):
/// the supplied interval clamped into [`MIN_HEARTBEAT`, `MAX_HEARTBEAT`] so
/// `heartbeatIntervalMs: 0` (which would panic `tokio::time::interval`) or an
/// absurd value cannot break the transport.
fn clamp_period(requested: Duration) -> Duration {
    requested.clamp(MIN_HEARTBEAT, MAX_HEARTBEAT)
}

/// The effective read-liveness window: the (clamped) configured window, but
/// never below two heartbeat periods — the pongs the heartbeat solicits are the
/// guaranteed inbound traffic, so a window shorter than their cadence would
/// declare every healthy-but-quiet connection dead.
fn read_window(requested: Duration, heartbeat: Duration) -> Duration {
    clamp_period(requested).max(heartbeat.saturating_mul(2))
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
        InboundFrameV2::Interrupt { .. } => INBOX_TYPE_INTERRUPT,
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
        | InboundFrameV2::Interrupt { msg_id, .. }
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
        MAX_HEARTBEAT, MAX_OUTBOX_RETRIES, MIN_HEARTBEAT, ack_stalled, clamp_period,
        classify_inbound, collect_outbox_replay, correlation_of, inbox_type, msg_id_of,
        read_window, replay_inbox,
    };
    use crate::db::Db;
    use crate::protocol::v2::{InboundFrameV2, MessageInData};
    use tokio::sync::mpsc;

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

    /// Outbox poison policy (audit P3): a row whose payload cannot be parsed is
    /// skipped on each replay and dead-lettered at [`MAX_OUTBOX_RETRIES`]
    /// attempts, while healthy rows keep replaying before and after.
    #[test]
    fn malformed_outbox_row_dead_letters_after_repeated_replays() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let healthy = {
            let guard = db.lock().unwrap();
            let id = guard.enqueue_outbox("heartbeat", &json!({})).unwrap();
            // A malformed payload can only enter via raw SQL — the enqueue API
            // always serializes valid JSON.
            guard
                .exec_raw("INSERT INTO outbox (type, payload) VALUES ('thread_result', 'not-json')")
                .unwrap();
            id
        };

        // Below the limit: the malformed row is skipped (never an envelope) but
        // stays queued; the healthy row replays on every full replay.
        for _ in 0..MAX_OUTBOX_RETRIES - 1 {
            let (envelopes, _) = collect_outbox_replay(&db, 0);
            assert_eq!(envelopes.len(), 1);
            assert!(envelopes[0].contains("heartbeat"));
            assert_eq!(db.lock().unwrap().list_unacked_outbox().unwrap().len(), 2);
        }

        // The attempt that reaches the limit dead-letters the poison row…
        assert_eq!(collect_outbox_replay(&db, 0).0.len(), 1);
        let unacked = db.lock().unwrap().list_unacked_outbox().unwrap();
        assert_eq!(unacked.len(), 1);
        assert_eq!(unacked[0].id, healthy);

        // …and the healthy row continues to replay afterwards.
        assert_eq!(collect_outbox_replay(&db, 0).0.len(), 1);
    }

    /// Regression: `after_id` makes replay incremental — rows at or below
    /// the send watermark are not rebuilt (so a flush cannot retransmit them),
    /// and the returned watermark advances to the highest row considered.
    #[test]
    fn collect_outbox_replay_skips_rows_at_or_below_the_watermark() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        {
            let guard = db.lock().unwrap();
            guard
                .enqueue_outbox("thread_message", &json!({"seq": 1}))
                .unwrap();
            guard
                .enqueue_outbox("thread_message", &json!({"seq": 2}))
                .unwrap();
        }

        // Full replay sends both and reports watermark 2.
        let (all, watermark) = collect_outbox_replay(&db, 0);
        assert_eq!(all.len(), 2);
        assert_eq!(watermark, 2);

        // A flush at that watermark has nothing new to send…
        let (incremental, watermark) = collect_outbox_replay(&db, watermark);
        assert!(incremental.is_empty());
        assert_eq!(watermark, 2);

        // …and a freshly enqueued row is the only thing the next flush sends.
        db.lock()
            .unwrap()
            .enqueue_outbox("thread_message", &json!({"seq": 3}))
            .unwrap();
        let (fresh, watermark) = collect_outbox_replay(&db, watermark);
        assert_eq!(fresh.len(), 1);
        assert!(fresh[0].contains("\"seq\":3"));
        assert_eq!(watermark, 3);
    }

    /// The ack-progress check trips only when the SAME oldest
    /// transmitted-but-unacked row survives two consecutive checks; acked
    /// progress or untransmitted rows never trip it.
    #[test]
    fn ack_stall_requires_the_same_oldest_row_across_two_checks() {
        let db = Arc::new(Mutex::new(Db::open_in_memory().unwrap()));
        let first = db
            .lock()
            .unwrap()
            .enqueue_outbox("thread_message", &json!({"seq": 1}))
            .unwrap();
        let second = db
            .lock()
            .unwrap()
            .enqueue_outbox("thread_message", &json!({"seq": 2}))
            .unwrap();

        // Rows beyond the watermark (never transmitted) are not "stalled".
        let mut stalled = None;
        assert!(!ack_stalled(&db, 0, &mut stalled));
        assert!(!ack_stalled(&db, 0, &mut stalled));

        // First check under the watermark only records the observation…
        assert!(!ack_stalled(&db, second, &mut stalled));
        // …the second check with zero ack progress trips the retransmit.
        assert!(ack_stalled(&db, second, &mut stalled));

        // Ack progress between checks (oldest changes) does not trip.
        assert!(!ack_stalled(&db, second, &mut stalled));
        db.lock().unwrap().mark_outbox_acked(first).unwrap();
        assert!(!ack_stalled(&db, second, &mut stalled));

        // Everything acked: nothing outstanding, never trips.
        db.lock().unwrap().mark_outbox_acked(second).unwrap();
        assert!(!ack_stalled(&db, second, &mut stalled));
        assert!(!ack_stalled(&db, second, &mut stalled));
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
                    engine: None,
                    model: None,
                    workdir: None,
                    meta: None,
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
            inbox_type(&InboundFrameV2::Interrupt {
                task_id: None,
                msg_id: Some(1)
            }),
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

    /// Regression: a host-supplied `heartbeatIntervalMs: 0` (or an absurd
    /// value) is clamped, never handed to `tokio::time::interval` (which panics
    /// on zero).
    #[test]
    fn clamp_period_clamps_zero_and_absurd_values() {
        assert_eq!(clamp_period(Duration::ZERO), MIN_HEARTBEAT);
        assert_eq!(clamp_period(Duration::from_hours(24 * 365)), MAX_HEARTBEAT);
        // A sane value passes through unchanged.
        assert_eq!(
            clamp_period(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    /// The read-liveness window is clamped and never drops below two
    /// heartbeat periods (the guaranteed pong cadence).
    #[test]
    fn read_window_never_undercuts_the_pong_cadence() {
        // Wider than two heartbeats: passes through (clamped only).
        assert_eq!(
            read_window(Duration::from_secs(90), Duration::from_secs(30)),
            Duration::from_secs(90)
        );
        // Narrower than two heartbeats: floored at 2× so pongs can arrive.
        assert_eq!(
            read_window(Duration::from_secs(90), Duration::from_hours(1)),
            Duration::from_hours(2)
        );
        // Zero is clamped before the floor applies.
        assert_eq!(
            read_window(Duration::ZERO, Duration::from_millis(100)),
            MIN_HEARTBEAT
        );
    }

    fn test_db() -> Arc<Mutex<Db>> {
        Arc::new(Mutex::new(Db::open_in_memory().unwrap()))
    }

    const MESSAGE_FRAME: &str =
        r#"{"type":"message","taskId":"t-1","threadId":"c-1","data":{"text":"hi"},"msgId":7}"#;

    /// Regression: delivering the same `msgId` twice stores exactly ONE
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

    const CONFIG_UPDATE_FRAME: &str =
        r#"{"type":"config_update","data":{"engine":{"anthropicApiKey":"sk-new"}},"msgId":11}"#;

    /// Regression: a `config_update` flows through the SAME at-least-once
    /// pipeline as every other inbound frame — persisted verbatim under its wire
    /// type, acked, and deduped by `msg_id` (a redelivery is re-acked without a
    /// second row) — instead of being silently dropped at the WS layer.
    #[test]
    fn config_update_is_persisted_acked_and_deduped_like_other_frames() {
        let db = test_db();

        let first = classify_inbound(CONFIG_UPDATE_FRAME, &db);
        assert!(
            matches!(
                first,
                InboundDisposition::Deliver {
                    msg_id: Some(11),
                    ..
                }
            ),
            "first delivery persists, acks and delivers: {first:?}"
        );
        let rows = db.lock().unwrap().list_unprocessed_inbox().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].msg_type, INBOX_TYPE_CONFIG_UPDATE);

        let dup = classify_inbound(CONFIG_UPDATE_FRAME, &db);
        assert!(
            matches!(dup, InboundDisposition::AckOnly { msg_id: 11 }),
            "redelivery is re-acked without a second row: {dup:?}"
        );
        assert_eq!(
            db.lock().unwrap().list_unprocessed_inbox().unwrap().len(),
            1
        );
    }

    /// A malformed `config_update` (missing its required `data`) follows
    /// the established dead-letter policy — acked away, never queued — so it
    /// cannot head-of-line-block the stream.
    #[test]
    fn malformed_config_update_is_dead_lettered_with_ack() {
        let db = test_db();
        let disposition = classify_inbound(r#"{"type":"config_update","msgId":12}"#, &db);
        assert!(
            matches!(disposition, InboundDisposition::AckOnly { msg_id: 12 }),
            "malformed config_update is acked per the dead-letter policy: {disposition:?}"
        );
        assert!(
            db.lock()
                .unwrap()
                .list_unprocessed_inbox()
                .unwrap()
                .is_empty(),
            "a dead-lettered frame is not queued for processing"
        );

        // A well-formed frame right after flows normally — no blocking.
        assert!(matches!(
            classify_inbound(CONFIG_UPDATE_FRAME, &db),
            InboundDisposition::Deliver { .. }
        ));
    }

    /// Regression: a KNOWN-type frame with a malformed body is acked away
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

    /// A peer that accepts TCP but never completes the WS handshake must
    /// not stall a dial attempt past the per-attempt bound.
    #[tokio::test]
    async fn dial_times_out_when_the_handshake_stalls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hold = tokio::spawn(async move {
            // Accept and hold the raw TCP connection, never answering the
            // upgrade request, so the dial can only end via its timeout.
            let _tcp = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });

        let err = super::dial(
            &format!("ws://{addr}/agent/ws/stall"),
            &secrecy::SecretString::from("tok"),
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, super::WsError::Timeout(_)),
            "expected a dial timeout, got {err:?}"
        );
        hold.abort();
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

    /// Protocol §5: `interrupt` delivery is LIVE-ONLY (at-most-once). A row that
    /// survived a crash/reconnect is retired on replay — never re-delivered, so
    /// a stale cancel cannot fire against a future turn.
    #[test]
    fn interrupt_is_retired_not_replayed() {
        let db = test_db();
        let interrupt = r#"{"type":"interrupt","taskId":"t-1","msgId":11}"#;
        assert!(matches!(
            classify_inbound(interrupt, &db),
            InboundDisposition::Deliver { .. }
        ));

        // Simulate the crash-before-processing window: the row is still
        // unprocessed when the next connect replays the inbox.
        let (tx, mut rx) = mpsc::unbounded_channel();
        replay_inbox(&db, &tx);
        assert!(rx.try_recv().is_err(), "interrupt must not be re-delivered");
        assert!(
            db.lock()
                .unwrap()
                .list_unprocessed_inbox()
                .unwrap()
                .is_empty(),
            "the stale interrupt row is retired (marked processed)"
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
