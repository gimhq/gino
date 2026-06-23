//! The engine abstraction (PLAN-003 §5).
//!
//! An [`Engine`] is a backend that runs agent work; a [`Session`] is one
//! long-lived conversation driven by that backend. The runner's transport,
//! scheduler and persistence are all engine-agnostic — they speak only the types
//! in this module, never a backend's wire format — so a new backend is added as a
//! fresh [`Engine`] impl without touching anything else.
//!
//! Two shapes of work map onto the two entry points:
//!
//! * [`Engine::start_session`] opens a **long-lived group session** — a process
//!   that stays alive across many turns, taking follow-up [`Input`]s via
//!   [`Session::send`] and surfacing [`EngineEvent`]s on [`Session::events`].
//! * [`Engine::run_once`] runs an **isolated one-shot**: a fresh session, a single
//!   [`Payload`], one [`RunOutcome`], then exit.
//!
//! ## Object safety
//!
//! Both traits are object-safe so the runner can hold a `Box<dyn Engine>` /
//! `Box<dyn Session>` selected at runtime by `Task.engine`. Native `async fn` in
//! traits is *not* `dyn`-compatible, so the async methods are desugared to boxed
//! futures by [`mod@async_trait`]; [`Session::events`] returns a boxed
//! [`futures_util::stream::BoxStream`] for the same reason. The
//! `object_safe_*` compile-time tests below pin this guarantee.
//!
//! This module is engine-agnostic: the Claude backend lives entirely under
//! [`claude`].

pub(crate) mod claude;

use std::path::PathBuf;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use thiserror::Error;

/// Execution mode for a unit of work (PLAN-003 §4 `context.mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A turn on the long-lived group session.
    Group,
    /// An isolated, one-shot run.
    Isolated,
}

/// Status of a completed engine turn. Engine-agnostic mirror of the host
/// `output` status (`success`/`error`); the wire mapping lives in the transport
/// layer, not here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RunStatus {
    /// The turn completed normally.
    Success,
    /// The turn ended in an error (engine error, max turns, …).
    Error,
}

/// Severity of an [`EngineEvent::Log`] line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogLevel {
    /// Verbose, diagnostic detail.
    Debug,
    /// Ordinary progress information.
    Info,
    /// A recoverable concern (e.g. an API retry).
    Warn,
    /// An error the engine surfaced but recovered from.
    Error,
}

/// Configuration for a long-lived [`Session`] (PLAN-003 §5 `SessionCfg`).
#[derive(Clone, Debug, Default)]
pub(crate) struct SessionCfg {
    /// Working directory the engine runs in. Empty means the current directory.
    pub(crate) cwd: PathBuf,
    /// Resume an existing backend session id, or `None` to start fresh.
    pub(crate) resume: Option<String>,
    /// Resume at a specific message id within the resumed session.
    pub(crate) resume_at: Option<String>,
    /// An optional first user message sent right after the handshake.
    pub(crate) initial_prompt: Option<String>,
    /// The task this session runs for, exported to the engine as `GINO_TASK_ID`
    /// so in-engine tooling (e.g. the `gino-agent cron` CLI) can self-reference.
    pub(crate) task_id: Option<String>,
    /// The thread this session runs for, exported as `GINO_THREAD_ID`.
    pub(crate) thread_id: Option<String>,
}

/// Input for an isolated [`Engine::run_once`] (PLAN-003 §5 `Payload`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Payload {
    /// The single user prompt to run.
    pub(crate) prompt: String,
    /// Working directory the engine runs in. Empty means the current directory.
    pub(crate) cwd: PathBuf,
}

/// A follow-up user message for a live [`Session`] (PLAN-003 §5 `Input`).
#[derive(Clone, Debug)]
pub(crate) struct Input {
    /// The message text to deliver to the running session.
    pub(crate) text: String,
}

impl Input {
    /// Wrap a string as a session input.
    pub(crate) fn text(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

/// The result of a completed turn (PLAN-003 §5 `RunOutcome`). Carries the
/// host-output triple `{status, result, newSessionId}` plus an optional error
/// detail; the transport maps this onto an `output` frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunOutcome {
    /// Whether the turn succeeded or errored.
    pub(crate) status: RunStatus,
    /// The assistant's final text, if any.
    pub(crate) result: Option<String>,
    /// The backend session id this turn ran under, for resume/persistence.
    pub(crate) session_id: Option<String>,
    /// Error detail when `status` is [`RunStatus::Error`].
    pub(crate) error: Option<String>,
}

impl RunOutcome {
    /// An error outcome carrying just a message — used when the engine fails
    /// before (or instead of) producing a backend `result`.
    pub(crate) fn errored(message: impl Into<String>) -> Self {
        Self {
            status: RunStatus::Error,
            result: None,
            session_id: None,
            error: Some(message.into()),
        }
    }
}

/// An event emitted while a [`Session`] runs (PLAN-003 §5 `EngineEvent`). The
/// runner forwards these to the orchestrator (mapping `Result` → `output`, the
/// rest → logs) and persists the session id from `SessionStarted`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EngineEvent {
    /// The backend reported its session id (Claude `system/init`). The consumer
    /// persists this for resume.
    SessionStarted {
        /// The backend session identifier.
        session_id: String,
    },
    /// Streaming assistant output text.
    Output {
        /// A chunk of assistant text.
        text: String,
    },
    /// The turn completed (Claude `result`).
    Result(RunOutcome),
    /// Token accounting for the turn, when the backend reports it.
    Usage {
        /// Prompt/input tokens consumed.
        input_tokens: u64,
        /// Completion/output tokens produced.
        output_tokens: u64,
    },
    /// A diagnostic line (API retries, hook activity, raw assistant traffic).
    Log {
        /// Severity of the line.
        level: LogLevel,
        /// The human-readable message.
        message: String,
    },
}

/// Errors raised by an [`Engine`] or [`Session`].
#[derive(Debug, Error)]
pub(crate) enum EngineError {
    /// The backend process could not be spawned.
    #[error("spawning engine process: {0}")]
    Spawn(#[source] std::io::Error),
    /// An I/O error while talking to the backend process.
    #[error("engine i/o: {0}")]
    Io(#[source] std::io::Error),
    /// A control or input message could not be serialized.
    #[error("serialize engine message: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The backend's stdin/stdout pipe was not available after spawn.
    #[error("engine process is missing its {0} pipe")]
    MissingPipe(&'static str),
    /// The writer task's channel closed, so the session can no longer be driven.
    #[error("engine session is closed")]
    Closed,
}

/// A backend that runs agent work. One implementation per backend; object-safe so
/// the runner can dispatch on `Task.engine` at runtime.
#[async_trait]
pub(crate) trait Engine: Send + Sync {
    /// The backend's stable name (e.g. `"claude"`).
    fn name(&self) -> &'static str;

    /// Open a long-lived group session. The returned [`Session`] stays alive
    /// across turns until dropped or interrupted.
    async fn start_session(&self, cfg: SessionCfg) -> Result<Box<dyn Session>, EngineError>;

    /// Run an isolated one-shot: a fresh session, a single payload, one outcome.
    async fn run_once(&self, payload: Payload) -> Result<RunOutcome, EngineError>;
}

/// A live conversation driven by an [`Engine`]. Object-safe.
#[async_trait]
pub(crate) trait Session: Send + Sync {
    /// Deliver a follow-up user message to the running session.
    async fn send(&self, input: Input) -> Result<(), EngineError>;

    /// Gracefully cancel the current turn without killing the session (the
    /// process stays alive for the next turn).
    async fn interrupt(&self) -> Result<(), EngineError>;

    /// The stream of [`EngineEvent`]s this session produces. The stream ends when
    /// the backend exits. Intended to be consumed once; a second call yields an
    /// empty stream.
    fn events(&self) -> BoxStream<'_, EngineEvent>;
}

#[cfg(test)]
mod tests {
    use super::{Engine, EngineEvent, LogLevel, RunOutcome, RunStatus, Session};

    // Compile-time proof that both traits are object-safe (`dyn`-compatible):
    // if either method were not, these signatures would fail to compile.
    #[allow(dead_code)]
    fn object_safe_engine(_engine: &dyn Engine) {}
    #[allow(dead_code)]
    fn object_safe_session(_session: Box<dyn Session>) {}

    #[test]
    fn errored_outcome_is_an_error_with_message() {
        let outcome = RunOutcome::errored("boom");
        assert_eq!(outcome.status, RunStatus::Error);
        assert_eq!(outcome.error.as_deref(), Some("boom"));
        assert!(outcome.result.is_none());
        assert!(outcome.session_id.is_none());
    }

    #[test]
    fn engine_event_equality_distinguishes_variants() {
        let a = EngineEvent::SessionStarted {
            session_id: "s".to_owned(),
        };
        let b = EngineEvent::Log {
            level: LogLevel::Warn,
            message: "s".to_owned(),
        };
        assert_ne!(a, b);
        assert_eq!(
            a,
            EngineEvent::SessionStarted {
                session_id: "s".to_owned()
            }
        );
    }
}
