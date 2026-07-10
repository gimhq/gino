//! Wire protocol types (PLAN-003 §2, PLAN-002 §4, PLAN-001 event mapping).
//!
//! These are the serde envelopes exchanged with the orchestrator. They are
//! host-wire-compatible: field names match the existing TypeScript runner
//! (`container/gino-agent/src/types.ts`) and the host WS handler, so the Rust
//! runner is a drop-in on the same socket.
//!
//! ## The live contract is v2
//!
//! The runner speaks `protocolVersion = 2` end to end (PLAN-006 Lane B /
//! agent-architecture §10): the [`v2`] module is the live frame catalog —
//! correlation ids (`taskId`/`threadId`/`cronId`) at the **envelope top level**,
//! business fields under `data`, and the `thread_*`/`cron_*` frames. The v1
//! `output`/`idle`/`task_*` frame enums have been removed (PLAN-006 §4); only the
//! shared HTTP registration DTOs ([`RegisterRequest`] et al.) remain at this top
//! level.
//!
//! ## Secrets
//!
//! The register token and connect token are wrapped in [`Redacted`] so they are
//! never printed by `Debug`/logs, while still serializing transparently onto the
//! wire (the tokens must reach the host) and deserializing back. The durable
//! boundary wrap into `secrecy::SecretString` happens in the config/identity
//! layers that consume these DTOs.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Protocol version
// ---------------------------------------------------------------------------

/// Negotiated wire-protocol version, bumped **1 → 2** for the task/cron/thread
/// rebuild (PLAN-006 / agent-architecture §10). The agent sends it in
/// `POST /agent/register`; the host returns the version it will speak and rejects
/// a mismatch with `register_error { reason: "unsupported_protocol" }`
/// ([`RegisterError`]). Compatibility is additive-within-major: new optional
/// fields and new `type`s are non-breaking and receivers ignore unknown ones.
///
/// Sent on the wire and gated by [`register`](crate::register): it rides the
/// register request ([`RegisterRequest::protocol_version`]) and the negotiation
/// refuses a host that cannot speak it (Lane PV).
pub(crate) const PROTOCOL_VERSION: u32 = 2;

// ---------------------------------------------------------------------------
// Redaction wrapper
// ---------------------------------------------------------------------------

/// A string that serializes/deserializes transparently but is redacted in
/// `Debug` output. Used for the bootstrap tokens carried by the register DTOs so
/// they never leak through a `{:?}` of a logged request/response.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Redacted(String);

impl Redacted {
    /// Wrap a raw secret string.
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the raw secret. Callers should hand this straight to the
    /// `secrecy`-backed config/identity boundary, not to a logger.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"<redacted>\"")
    }
}

// ---------------------------------------------------------------------------
// HTTP registration (PLAN-003 §2.1)
// ---------------------------------------------------------------------------

/// `POST /agent/register` request body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RegisterRequest {
    /// Bootstrap enrollment credential (redacted in `Debug`).
    pub(crate) register_token: Redacted,
    /// Wire-protocol version this agent speaks ([`PROTOCOL_VERSION`]); the host
    /// gates enrollment on it (§1) and rejects a mismatch with
    /// `register_error { reason: "unsupported_protocol" }`.
    pub(crate) protocol_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) version: String,
    pub(crate) platform: String,
}

/// Successful `POST /agent/register` response: the host-generated `agentId`, a
/// freshly minted per-agent `connectToken`, and the assigned config overlay.
///
/// Deliberately `Deserialize`-only: this DTO carries the connect token, and the
/// agent only ever *receives* it. Not deriving `Serialize` removes a whole
/// token-leak surface (accidentally re-serializing the secret into a log, an
/// outbox payload, or a frame).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RegisterOk {
    pub(crate) agent_id: String,
    /// Per-agent WS connect token (redacted in `Debug`).
    pub(crate) connect_token: Redacted,
    /// Host-assigned config (merged by the config layer). Opaque here.
    pub(crate) config: Value,
}

/// Failure envelope: `{register_error: {reason}}`. Kept snake_case to match the
/// host wire key exactly.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RegisterErrorEnvelope {
    pub(crate) register_error: RegisterError,
}

/// The reason carried by a [`RegisterErrorEnvelope`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RegisterError {
    pub(crate) reason: String,
}

/// The two shapes a register response can take. Untagged: a body with
/// `agentId`/`connectToken`/`config` parses as [`RegisterResponse::Ok`];
/// `{register_error: …}` parses as [`RegisterResponse::Error`].
/// `Deserialize`-only, like the secret-bearing [`RegisterOk`] it contains.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub(crate) enum RegisterResponse {
    Ok(RegisterOk),
    Error(RegisterErrorEnvelope),
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        PROTOCOL_VERSION, Redacted, RegisterError, RegisterErrorEnvelope, RegisterRequest,
        RegisterResponse,
    };

    #[test]
    fn redacted_hides_value_in_debug_but_serializes_transparently() {
        let secret = Redacted::new("super-secret");
        assert_eq!(secret.expose(), "super-secret");
        assert_eq!(format!("{secret:?}"), "\"<redacted>\"");
        assert_eq!(
            serde_json::to_value(&secret).unwrap(),
            json!("super-secret")
        );
        let back: Redacted = serde_json::from_value(json!("super-secret")).unwrap();
        assert_eq!(back, secret);
    }

    #[test]
    fn register_request_is_camel_case_and_redacts_token() {
        let req = RegisterRequest {
            register_token: Redacted::new("enroll-tok"),
            protocol_version: PROTOCOL_VERSION,
            name: Some("worker-1".to_owned()),
            capabilities: vec!["claude".to_owned()],
            version: "0.1.0".to_owned(),
            platform: "x86_64-unknown-linux-gnu".to_owned(),
        };
        assert!(!format!("{req:?}").contains("enroll-tok"));
        assert_eq!(
            serde_json::to_value(&req).unwrap(),
            json!({
                "registerToken": "enroll-tok",
                "protocolVersion": 2,
                "name": "worker-1",
                "capabilities": ["claude"],
                "version": "0.1.0",
                "platform": "x86_64-unknown-linux-gnu"
            })
        );
        let back: RegisterRequest =
            serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn register_ok_response_parses_and_redacts() {
        // Parse-only by design: RegisterOk carries the connect token and does not
        // implement Serialize (see the type's doc), so there is no re-serialize
        // round trip to assert here.
        let raw = r#"{
            "agentId": "agent-123",
            "connectToken": "connect-secret",
            "config": { "engine": { "model": "claude-sonnet-4-6" } }
        }"#;
        let resp: RegisterResponse = serde_json::from_str(raw).unwrap();
        let RegisterResponse::Ok(ok) = &resp else {
            panic!("expected Ok variant, got {resp:?}");
        };
        assert_eq!(ok.agent_id, "agent-123");
        assert_eq!(ok.connect_token.expose(), "connect-secret");
        assert_eq!(ok.config["engine"]["model"], json!("claude-sonnet-4-6"));
        assert!(!format!("{resp:?}").contains("connect-secret"));
    }

    #[test]
    fn register_error_response_round_trips() {
        let raw = r#"{ "register_error": { "reason": "invalid register token" } }"#;
        let resp: RegisterResponse = serde_json::from_str(raw).unwrap();
        let envelope = RegisterErrorEnvelope {
            register_error: RegisterError {
                reason: "invalid register token".to_owned(),
            },
        };
        assert_eq!(resp, RegisterResponse::Error(envelope.clone()));
        // The secret-free error envelope still serializes (only the enum and the
        // secret-bearing Ok variant are Deserialize-only).
        assert_eq!(
            serde_json::to_value(&envelope).unwrap(),
            json!({ "register_error": { "reason": "invalid register token" } })
        );
    }
}

// ---------------------------------------------------------------------------
// Protocol v2 frames (PLAN-006 Lane B)
// ---------------------------------------------------------------------------

/// Protocol **v2** frame types (PLAN-006 Lane B; agent-architecture §10/§12/§14).
///
/// ## Envelope vs payload
///
/// Every frame is one JSON object. Correlation/delivery metadata lives at the
/// **envelope top level** — `type`, the delivery ids `outboxId`/`msgId`, and the
/// correlation ids `taskId`/`threadId`/`cronId` — while `data` carries the
/// type-specific **business** payload only:
///
/// ```json
/// { "type": "thread_message", "outboxId": 43, "taskId": "t-1", "threadId": "c-9@1718",
///   "data": { "seq": 3, "kind": "assistant_text", "content": "…" } }
/// ```
///
/// The host routes and correlates by the envelope ids *without parsing `data`*,
/// mirroring the runner's `inbox`/`outbox` correlation columns and the host
/// read-model keys. The correlation ids are **never** repeated inside `data`.
/// `engine_session_id` is a *value* of a thread (the claude `--resume` handle),
/// not a routing key, so it lives **inside `data`** on `thread_result`.
///
/// ## Multiplexing & ordering
///
/// Many threads share one WS, so frames interleave. Each `thread_message` carries
/// a per-thread monotonic `seq` (assigned serially by the runner); the host
/// demuxes by `threadId`, orders each thread by `seq`, and writes idempotently on
/// `(threadId, seq)` (at-least-once replay can duplicate). `thread_result`'s
/// `final_seq` is the last `seq` emitted, letting the host verify it received
/// `1..=final_seq`. `outboxId` is delivery/ack only; `seq` is the per-thread
/// continuity contract.
///
/// These frames are the **live** wire contract, driven end to end by
/// `transport`/`executor`/`scheduler`/`runner`; the v1 frame enums they replaced
/// have been removed (PLAN-006 §4). Enrollment ([`RegisterRequest`] et al.) is
/// shared and unchanged.
pub(crate) mod v2 {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    // -----------------------------------------------------------------------
    // Enumerations
    // -----------------------------------------------------------------------

    /// Engine-execution lifecycle state, carried in a [`ThreadResultData`].
    /// A `thread_result` reports a terminal state (`succeeded`/`failed`/
    /// `cancelled`); the earlier states exist on the runner's `threads` row.
    #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum ThreadStatus {
        Pending,
        Dispatched,
        Running,
        Active,
        Succeeded,
        Failed,
        Cancelled,
    }

    /// Transcript-event classification for a [`ThreadMessageData`]
    /// (agent-architecture §10; the host `messages.kind` column). `thinking`/
    /// `status` are config-filterable.
    #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum MessageKind {
        UserInput,
        AssistantText,
        Thinking,
        ToolUse,
        ToolResult,
        Status,
        Error,
    }

    // -----------------------------------------------------------------------
    // Shared payloads
    // -----------------------------------------------------------------------

    /// An out-of-band attachment **descriptor** (agent-architecture §9). Only the
    /// descriptor rides the wire; bytes move on the separate attachment channel
    /// (host-proxy / presigned), never inside a frame.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct AttachmentDescriptor {
        pub(crate) id: String,
        pub(crate) name: String,
        pub(crate) content_type: String,
        pub(crate) size: i64,
        pub(crate) sha256: String,
        /// Backend-specific locator (host-proxy path or presigned URL). `ref` is a
        /// Rust keyword, so the field is named `reference` on this side and renamed
        /// back to `ref` on the wire.
        #[serde(rename = "ref")]
        pub(crate) reference: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) direction: Option<String>,
    }

    /// Shape of a [`ProvisioningEntry`]'s materialized artifact (PLAN-007 §1): a
    /// single `file` (e.g. an instruction file `CLAUDE.md`/`AGENTS.md`) or a `dir`
    /// bundle (e.g. a skill = `SKILL.md` + files). Wire-parses `"file"`/`"dir"`; an
    /// unknown kind string fails to deserialize.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum EntryKind {
        File,
        Dir,
    }

    /// A versioned, content-addressed **provisioning entry** (PLAN-007 §1): one
    /// instruction file or skill bundle to materialize into a scope root. Engine-
    /// agnostic — the runner only knows `target`/`kind`; the host chooses the
    /// meta-driven `target` per engine (claude→`CLAUDE.md`, codex→`AGENTS.md`, …).
    /// Bytes are fetched out-of-band via `/agent/attachments` by `id`/`sha256`;
    /// only this descriptor rides the wire. Shared by both scopes: the global
    /// config layer (`config.entries`) and the task definition
    /// (`task_upsert.data.entries`).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct ProvisioningEntry {
        /// Logical name (= store `<name>`); groups an artifact's versions.
        pub(crate) id: String,
        /// Full hash — the version identifier and the download-integrity check.
        pub(crate) sha256: String,
        /// Relative, meta-driven landing path under the scope root, e.g.
        /// `CLAUDE.md`, `AGENTS.md`, `.claude/skills/<name>`, `skills/<name>`.
        pub(crate) target: String,
        /// Whether the artifact is a single `file` or a `dir` bundle.
        pub(crate) kind: EntryKind,
    }

    // -----------------------------------------------------------------------
    // host → agent `data` payloads
    // -----------------------------------------------------------------------

    /// `data` of a `task_upsert` (the id is the envelope `taskId`).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct TaskUpsertData {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) status: Option<String>,
        /// Task-scope provisioning entries (instruction file + skills) materialized
        /// into `<thread.work_dir>/project/` (PLAN-007 §5). Absent → `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) entries: Option<Vec<ProvisioningEntry>>,
    }

    /// `data` of a `cron_upsert` (ids are the envelope `taskId`/`cronId`).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct CronUpsertData {
        pub(crate) kind: String,
        /// The recurring instruction the cron runs each fire (agent-architecture
        /// §10, D10:(a)). Required — the runner stores it into `crons.prompt`.
        pub(crate) prompt: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) schedule_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) schedule_value: Option<String>,
        pub(crate) continuity: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) next_run: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) status: Option<String>,
    }

    /// `data` of an inbound `message` (ids are the envelope `taskId`/`threadId?`).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct MessageInData {
        pub(crate) text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) attachments: Option<Vec<AttachmentDescriptor>>,
    }

    // -----------------------------------------------------------------------
    // agent → host `data` payloads
    // -----------------------------------------------------------------------

    /// `data` of a `thread_result` — the terminal outcome of an execution.
    /// `engine_session_id` is the thread's resume handle (a value, not a routing
    /// id); `final_seq` is the last `seq` emitted on the thread.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct ThreadResultData {
        pub(crate) status: ThreadStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) result: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) error: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) failure_reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) engine_session_id: Option<String>,
        pub(crate) duration_ms: i64,
        pub(crate) final_seq: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) attachments: Option<Vec<AttachmentDescriptor>>,
    }

    /// `data` of a `thread_message` — one ordered transcript event. `seq` is
    /// per-thread monotonic (assigned by the runner).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct ThreadMessageData {
        pub(crate) seq: i64,
        pub(crate) kind: MessageKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) role: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) tool: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) input: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) output: Option<Value>,
    }

    /// `data` of a `cron_state` — the local scheduling-ledger summary for a cron.
    /// Round/skip counts are runner-local (D2); the host derives dispatched counts
    /// from its own mirror by `cron_id`.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub(crate) struct CronStateData {
        pub(crate) kind: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) schedule_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) schedule_value: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) continuity: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) next_run: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) status: Option<String>,
        pub(crate) round_count: i64,
        pub(crate) skipped_count: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) last_status: Option<String>,
    }

    /// `data` of a `heartbeat` — liveness, optionally summarizing live work.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct HeartbeatData {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) active_threads: Option<i64>,
    }

    // -----------------------------------------------------------------------
    // Frame enums
    // -----------------------------------------------------------------------

    /// Inbound frames (host → agent), protocol v2. Internally tagged on `type`;
    /// correlation ids (`taskId`/`cronId`/`threadId`) and the delivery `msgId`
    /// ride the envelope, business fields sit under `data`. An unknown `type`
    /// folds into [`InboundFrameV2::Unknown`] so a newer host's frames are
    /// ignored, not fatal.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(
        tag = "type",
        rename_all = "snake_case",
        rename_all_fields = "camelCase"
    )]
    pub(crate) enum InboundFrameV2 {
        /// Seed or edit a task definition.
        TaskUpsert {
            task_id: String,
            data: TaskUpsertData,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Remove a task (identity is the envelope `taskId`; no `data`).
        TaskDelete {
            task_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Seed or edit a cron/trigger under a task.
        CronUpsert {
            task_id: String,
            cron_id: String,
            data: CronUpsertData,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Remove a cron (identity is the envelope `taskId`/`cronId`; no `data`).
        CronDelete {
            task_id: String,
            cron_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// A user/system message for a task (optionally targeting a live thread).
        Message {
            task_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            thread_id: Option<String>,
            data: MessageInData,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Graceful cancel of the current turn (no kill). An absent `taskId`
        /// interrupts whatever is in flight; a present one scopes the cancel
        /// to that task's run (additive: old hosts omit it).
        Interrupt {
            #[serde(skip_serializing_if = "Option::is_none")]
            task_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Host requests runner/session shutdown.
        Close {
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Live config change (opaque overlay; merged by the config layer).
        ConfigUpdate {
            data: Value,
            #[serde(skip_serializing_if = "Option::is_none")]
            msg_id: Option<i64>,
        },
        /// Host acknowledges receipt of the outbox row `id`.
        Ack { id: i64 },
        /// Forward-compat catch-all: an unknown `type` deserializes here so a
        /// receiver ignores frames added by a newer peer (additive-within-major).
        #[serde(other)]
        Unknown,
    }

    /// Outbound frames (agent → host), protocol v2. Outbox-backed frames carry an
    /// optional `outboxId`; the acks are sent raw. An unknown `type` folds into
    /// [`OutboundFrameV2::Unknown`].
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(
        tag = "type",
        rename_all = "snake_case",
        rename_all_fields = "camelCase"
    )]
    pub(crate) enum OutboundFrameV2 {
        /// Terminal outcome of a thread execution (mirror → host `thread_runs`).
        ThreadResult {
            task_id: String,
            thread_id: String,
            data: ThreadResultData,
            #[serde(skip_serializing_if = "Option::is_none")]
            outbox_id: Option<i64>,
        },
        /// One ordered transcript event (mirror → host `messages`).
        ThreadMessage {
            task_id: String,
            thread_id: String,
            data: ThreadMessageData,
            #[serde(skip_serializing_if = "Option::is_none")]
            outbox_id: Option<i64>,
        },
        /// Local scheduling-ledger summary for a cron (mirror → host `crons`).
        CronState {
            task_id: String,
            cron_id: String,
            data: CronStateData,
            #[serde(skip_serializing_if = "Option::is_none")]
            outbox_id: Option<i64>,
        },
        /// Liveness ping (interval from `config.heartbeatIntervalMs`).
        Heartbeat {
            data: HeartbeatData,
            #[serde(skip_serializing_if = "Option::is_none")]
            outbox_id: Option<i64>,
        },
        /// Runner durably stored an inbound message `msgId`.
        MsgAck { msg_id: i64 },
        /// Runner acknowledges receipt of an inbound frame `id`.
        Ack { id: i64 },
        /// Forward-compat catch-all (see [`InboundFrameV2::Unknown`]).
        #[serde(other)]
        Unknown,
    }

    #[cfg(test)]
    mod tests {
        use serde_json::json;

        use super::{
            AttachmentDescriptor, CronStateData, CronUpsertData, EntryKind, HeartbeatData,
            InboundFrameV2, MessageInData, MessageKind, OutboundFrameV2, ProvisioningEntry,
            TaskUpsertData, ThreadMessageData, ThreadResultData, ThreadStatus,
        };

        fn round_trip_inbound(frame: &InboundFrameV2) {
            let json = serde_json::to_string(frame).unwrap();
            let back: InboundFrameV2 = serde_json::from_str(&json).unwrap();
            assert_eq!(*frame, back, "inbound v2 round-trip mismatch via {json}");
        }

        fn round_trip_outbound(frame: &OutboundFrameV2) {
            let json = serde_json::to_string(frame).unwrap();
            let back: OutboundFrameV2 = serde_json::from_str(&json).unwrap();
            assert_eq!(*frame, back, "outbound v2 round-trip mismatch via {json}");
        }

        fn sample_attachment() -> AttachmentDescriptor {
            AttachmentDescriptor {
                id: "a-1".to_owned(),
                name: "in.txt".to_owned(),
                content_type: "text/plain".to_owned(),
                size: 12,
                sha256: "deadbeef".to_owned(),
                reference: "/agent/attachments/a-1".to_owned(),
                direction: Some("inbound".to_owned()),
            }
        }

        #[test]
        fn protocol_version_is_two() {
            assert_eq!(crate::protocol::PROTOCOL_VERSION, 2);
        }

        #[test]
        fn thread_message_splits_envelope_and_data() {
            let frame = OutboundFrameV2::ThreadMessage {
                task_id: "t-1".to_owned(),
                thread_id: "c-9@1718".to_owned(),
                data: ThreadMessageData {
                    seq: 3,
                    kind: MessageKind::AssistantText,
                    role: Some("assistant".to_owned()),
                    content: Some("hi".to_owned()),
                    tool: None,
                    input: None,
                    output: None,
                },
                outbox_id: Some(43),
            };
            assert_eq!(
                serde_json::to_value(&frame).unwrap(),
                json!({
                    "type": "thread_message",
                    "taskId": "t-1",
                    "threadId": "c-9@1718",
                    "outboxId": 43,
                    "data": {
                        "seq": 3,
                        "kind": "assistant_text",
                        "role": "assistant",
                        "content": "hi"
                    }
                })
            );
            // Correlation ids never leak into `data`.
            let value = serde_json::to_value(&frame).unwrap();
            assert!(value["data"].get("taskId").is_none());
            assert!(value["data"].get("threadId").is_none());
            round_trip_outbound(&frame);
        }

        #[test]
        fn thread_message_tool_event_round_trips() {
            let frame = OutboundFrameV2::ThreadMessage {
                task_id: "t-1".to_owned(),
                thread_id: "th-2".to_owned(),
                data: ThreadMessageData {
                    seq: 7,
                    kind: MessageKind::ToolUse,
                    role: None,
                    content: None,
                    tool: Some("bash".to_owned()),
                    input: Some(json!({ "cmd": "ls" })),
                    output: Some(json!({ "code": 0 })),
                },
                outbox_id: None,
            };
            round_trip_outbound(&frame);
        }

        #[test]
        fn thread_result_keeps_business_fields_in_data() {
            let frame = OutboundFrameV2::ThreadResult {
                task_id: "t-1".to_owned(),
                thread_id: "c-9@1718".to_owned(),
                data: ThreadResultData {
                    status: ThreadStatus::Succeeded,
                    result: Some("done".to_owned()),
                    error: None,
                    failure_reason: None,
                    engine_session_id: Some("sess-7".to_owned()),
                    duration_ms: 1500,
                    final_seq: 9,
                    attachments: Some(vec![sample_attachment()]),
                },
                outbox_id: Some(1),
            };
            let value = serde_json::to_value(&frame).unwrap();
            assert_eq!(value["type"], json!("thread_result"));
            assert_eq!(value["taskId"], json!("t-1"));
            assert_eq!(value["threadId"], json!("c-9@1718"));
            // engine_session_id, duration_ms, final_seq are business fields in data.
            assert_eq!(value["data"]["engine_session_id"], json!("sess-7"));
            assert_eq!(value["data"]["duration_ms"], json!(1500));
            assert_eq!(value["data"]["final_seq"], json!(9));
            // Attachment descriptor renames `reference` → `ref` on the wire.
            assert_eq!(
                value["data"]["attachments"][0]["ref"],
                json!("/agent/attachments/a-1")
            );
            round_trip_outbound(&frame);
        }

        #[test]
        fn thread_result_failure_carries_reason_and_drops_session() {
            let frame = OutboundFrameV2::ThreadResult {
                task_id: "t-1".to_owned(),
                thread_id: "c-9@1718".to_owned(),
                data: ThreadResultData {
                    status: ThreadStatus::Failed,
                    result: None,
                    error: Some("boom".to_owned()),
                    failure_reason: Some("agent.invalid_request".to_owned()),
                    engine_session_id: None,
                    duration_ms: 42,
                    final_seq: 2,
                    attachments: None,
                },
                outbox_id: None,
            };
            let value = serde_json::to_value(&frame).unwrap();
            assert_eq!(value["data"]["status"], json!("failed"));
            assert_eq!(
                value["data"]["failure_reason"],
                json!("agent.invalid_request")
            );
            // resume-unsafe failure → no engine_session_id persisted on the wire.
            assert!(value["data"].get("engine_session_id").is_none());
            assert!(value.get("outboxId").is_none());
            round_trip_outbound(&frame);
        }

        #[test]
        fn cron_state_ids_in_envelope_ledger_in_data() {
            let frame = OutboundFrameV2::CronState {
                task_id: "t-1".to_owned(),
                cron_id: "cr-1".to_owned(),
                data: CronStateData {
                    kind: "schedule".to_owned(),
                    schedule_type: Some("interval".to_owned()),
                    schedule_value: Some("300".to_owned()),
                    continuity: Some("isolated".to_owned()),
                    next_run: Some("2026-06-22T15:00:00Z".to_owned()),
                    status: Some("active".to_owned()),
                    round_count: 5,
                    skipped_count: 1,
                    last_status: Some("dispatched".to_owned()),
                },
                outbox_id: Some(2),
            };
            let value = serde_json::to_value(&frame).unwrap();
            assert_eq!(value["type"], json!("cron_state"));
            assert_eq!(value["taskId"], json!("t-1"));
            assert_eq!(value["cronId"], json!("cr-1"));
            assert_eq!(value["data"]["round_count"], json!(5));
            assert_eq!(value["data"]["skipped_count"], json!(1));
            assert_eq!(value["data"]["last_status"], json!("dispatched"));
            assert!(value["data"].get("cronId").is_none());
            round_trip_outbound(&frame);
        }

        #[test]
        fn heartbeat_and_acks_round_trip() {
            assert_eq!(
                serde_json::to_value(OutboundFrameV2::Heartbeat {
                    data: HeartbeatData {
                        active_threads: Some(2),
                    },
                    outbox_id: None,
                })
                .unwrap(),
                json!({ "type": "heartbeat", "data": { "activeThreads": 2 } })
            );
            assert_eq!(
                serde_json::to_value(OutboundFrameV2::MsgAck { msg_id: 5 }).unwrap(),
                json!({ "type": "msg_ack", "msgId": 5 })
            );
            assert_eq!(
                serde_json::to_value(OutboundFrameV2::Ack { id: 11 }).unwrap(),
                json!({ "type": "ack", "id": 11 })
            );
            for frame in [
                OutboundFrameV2::Heartbeat {
                    data: HeartbeatData {
                        active_threads: None,
                    },
                    outbox_id: Some(1),
                },
                OutboundFrameV2::MsgAck { msg_id: 5 },
                OutboundFrameV2::Ack { id: 11 },
            ] {
                round_trip_outbound(&frame);
            }
        }

        #[test]
        fn inbound_message_carries_ids_in_envelope() {
            let frame = InboundFrameV2::Message {
                task_id: "t-1".to_owned(),
                thread_id: Some("c-9@1718".to_owned()),
                data: MessageInData {
                    text: "hi".to_owned(),
                    attachments: Some(vec![sample_attachment()]),
                },
                msg_id: Some(2),
            };
            assert_eq!(
                serde_json::to_value(&frame).unwrap(),
                json!({
                    "type": "message",
                    "taskId": "t-1",
                    "threadId": "c-9@1718",
                    "msgId": 2,
                    "data": {
                        "text": "hi",
                        "attachments": [{
                            "id": "a-1",
                            "name": "in.txt",
                            "content_type": "text/plain",
                            "size": 12,
                            "sha256": "deadbeef",
                            "ref": "/agent/attachments/a-1",
                            "direction": "inbound"
                        }]
                    }
                })
            );
            round_trip_inbound(&frame);
        }

        #[test]
        fn inbound_message_omits_absent_thread_id() {
            let frame = InboundFrameV2::Message {
                task_id: "t-1".to_owned(),
                thread_id: None,
                data: MessageInData {
                    text: "hi".to_owned(),
                    attachments: None,
                },
                msg_id: None,
            };
            assert_eq!(
                serde_json::to_value(&frame).unwrap(),
                json!({ "type": "message", "taskId": "t-1", "data": { "text": "hi" } })
            );
            round_trip_inbound(&frame);
        }

        #[test]
        fn inbound_task_frames_round_trip() {
            let upsert = InboundFrameV2::TaskUpsert {
                task_id: "t-1".to_owned(),
                data: TaskUpsertData {
                    name: Some("nightly".to_owned()),
                    kind: Some("job".to_owned()),
                    status: Some("active".to_owned()),
                    entries: None,
                },
                msg_id: Some(1),
            };
            assert_eq!(
                serde_json::to_value(&upsert).unwrap(),
                json!({
                    "type": "task_upsert",
                    "taskId": "t-1",
                    "msgId": 1,
                    "data": { "name": "nightly", "kind": "job", "status": "active" }
                })
            );
            let delete = InboundFrameV2::TaskDelete {
                task_id: "t-1".to_owned(),
                msg_id: None,
            };
            assert_eq!(
                serde_json::to_value(&delete).unwrap(),
                json!({ "type": "task_delete", "taskId": "t-1" })
            );
            round_trip_inbound(&upsert);
            round_trip_inbound(&delete);
        }

        #[test]
        fn task_upsert_data_parses_provisioning_entries() {
            // A `task_upsert` data payload with `entries` deserializes into
            // `Some(vec![..])` and round-trips through serde unchanged.
            let raw = r#"{
                "name": "build",
                "entries": [
                    { "id": "clauderc", "sha256": "aa", "target": "CLAUDE.md", "kind": "file" },
                    { "id": "demo", "sha256": "bb", "target": ".claude/skills/demo", "kind": "dir" }
                ]
            }"#;
            let data: TaskUpsertData = serde_json::from_str(raw).unwrap();
            assert_eq!(
                data.entries,
                Some(vec![
                    ProvisioningEntry {
                        id: "clauderc".to_owned(),
                        sha256: "aa".to_owned(),
                        target: "CLAUDE.md".to_owned(),
                        kind: EntryKind::File,
                    },
                    ProvisioningEntry {
                        id: "demo".to_owned(),
                        sha256: "bb".to_owned(),
                        target: ".claude/skills/demo".to_owned(),
                        kind: EntryKind::Dir,
                    },
                ])
            );
            let back: TaskUpsertData =
                serde_json::from_str(&serde_json::to_string(&data).unwrap()).unwrap();
            assert_eq!(back, data);
        }

        #[test]
        fn task_upsert_data_without_entries_is_none() {
            // No `entries` key → `None`, and the omitted field is not serialized back.
            let data: TaskUpsertData = serde_json::from_str(r#"{ "name": "build" }"#).unwrap();
            assert_eq!(data.entries, None);
            assert!(
                serde_json::to_value(&data)
                    .unwrap()
                    .get("entries")
                    .is_none()
            );
        }

        #[test]
        fn provisioning_entry_rejects_unknown_kind() {
            let raw = r#"{ "id": "x", "sha256": "aa", "target": "x", "kind": "symlink" }"#;
            assert!(serde_json::from_str::<ProvisioningEntry>(raw).is_err());
        }

        #[test]
        fn inbound_cron_frames_round_trip() {
            let upsert = InboundFrameV2::CronUpsert {
                task_id: "t-1".to_owned(),
                cron_id: "cr-1".to_owned(),
                data: CronUpsertData {
                    kind: "schedule".to_owned(),
                    prompt: "send the daily digest".to_owned(),
                    schedule_type: Some("cron".to_owned()),
                    schedule_value: Some("0 9 * * *".to_owned()),
                    continuity: "isolated".to_owned(),
                    next_run: Some("2026-06-23T09:00:00Z".to_owned()),
                    status: Some("active".to_owned()),
                },
                msg_id: None,
            };
            assert_eq!(
                serde_json::to_value(&upsert).unwrap(),
                json!({
                    "type": "cron_upsert",
                    "taskId": "t-1",
                    "cronId": "cr-1",
                    "data": {
                        "kind": "schedule",
                        "prompt": "send the daily digest",
                        "schedule_type": "cron",
                        "schedule_value": "0 9 * * *",
                        "continuity": "isolated",
                        "next_run": "2026-06-23T09:00:00Z",
                        "status": "active"
                    }
                })
            );
            let delete = InboundFrameV2::CronDelete {
                task_id: "t-1".to_owned(),
                cron_id: "cr-1".to_owned(),
                msg_id: Some(8),
            };
            assert_eq!(
                serde_json::to_value(&delete).unwrap(),
                json!({ "type": "cron_delete", "taskId": "t-1", "cronId": "cr-1", "msgId": 8 })
            );
            round_trip_inbound(&upsert);
            round_trip_inbound(&delete);
        }

        #[test]
        fn inbound_control_frames_round_trip() {
            let config = InboundFrameV2::ConfigUpdate {
                data: json!({ "engine": { "model": "claude-opus-4-8" } }),
                msg_id: Some(3),
            };
            assert_eq!(
                serde_json::to_value(&config).unwrap(),
                json!({
                    "type": "config_update",
                    "msgId": 3,
                    "data": { "engine": { "model": "claude-opus-4-8" } }
                })
            );
            assert_eq!(
                serde_json::to_value(InboundFrameV2::Interrupt {
                    task_id: None,
                    msg_id: None
                })
                .unwrap(),
                json!({ "type": "interrupt" })
            );
            assert_eq!(
                serde_json::to_value(InboundFrameV2::Ack { id: 9 }).unwrap(),
                json!({ "type": "ack", "id": 9 })
            );
            for frame in [
                InboundFrameV2::Interrupt {
                    task_id: None,
                    msg_id: None,
                },
                InboundFrameV2::Interrupt {
                    task_id: Some("t-1".to_owned()),
                    msg_id: Some(6),
                },
                InboundFrameV2::Close { msg_id: Some(4) },
                InboundFrameV2::Close { msg_id: None },
                config,
                InboundFrameV2::Ack { id: 9 },
            ] {
                round_trip_inbound(&frame);
            }
        }

        #[test]
        fn interrupt_task_id_is_additive() {
            // Scoped form: envelope `taskId` lands in `task_id`.
            let scoped: InboundFrameV2 =
                serde_json::from_str(r#"{ "type": "interrupt", "taskId": "t-1" }"#).unwrap();
            assert_eq!(
                scoped,
                InboundFrameV2::Interrupt {
                    task_id: Some("t-1".to_owned()),
                    msg_id: None,
                }
            );
            // Legacy form: an interrupt without `taskId` must still parse.
            let global: InboundFrameV2 =
                serde_json::from_str(r#"{ "type": "interrupt" }"#).unwrap();
            assert_eq!(
                global,
                InboundFrameV2::Interrupt {
                    task_id: None,
                    msg_id: None,
                }
            );
        }

        #[test]
        fn unknown_type_is_tolerated() {
            let raw = r#"{ "type": "some_future_frame", "taskId": "t", "data": { "x": 1 } }"#;
            let inbound: InboundFrameV2 = serde_json::from_str(raw).unwrap();
            assert_eq!(inbound, InboundFrameV2::Unknown);
            let outbound: OutboundFrameV2 = serde_json::from_str(raw).unwrap();
            assert_eq!(outbound, OutboundFrameV2::Unknown);
        }

        #[test]
        fn unknown_fields_are_ignored() {
            // Extra envelope and extra `data` fields must not break a known frame.
            let raw = r#"{
                "type": "thread_message",
                "taskId": "t-1",
                "threadId": "th-1",
                "outboxId": 5,
                "futureEnvelopeField": true,
                "data": { "seq": 1, "kind": "status", "futureDataField": 42 }
            }"#;
            let frame: OutboundFrameV2 = serde_json::from_str(raw).unwrap();
            let OutboundFrameV2::ThreadMessage { task_id, data, .. } = frame else {
                panic!("expected thread_message, parsed something else");
            };
            assert_eq!(task_id, "t-1");
            assert_eq!(data.seq, 1);
            assert_eq!(data.kind, MessageKind::Status);
        }
    }
}
