//! `codex exec --json` JSONL line parsing → [`EngineEvent`].
//!
//! Each stdout line is one `ThreadEvent`, tagged by a top-level `type`. This
//! module maps the ones the runner needs onto engine-agnostic [`EngineEvent`]s:
//!
//! | JSONL `type`      | events                                              |
//! |-------------------|-----------------------------------------------------|
//! | `thread.started`  | [`EngineEvent::SessionStarted`] (carries `thread_id`)|
//! | `turn.completed`  | [`EngineEvent::Result`] — success                   |
//! | `turn.failed`     | [`EngineEvent::Result`] — error                     |
//! | `error`           | [`EngineEvent::Result`] — error (stream-level)      |
//! | anything else     | dropped (empty `Vec`)                               |
//!
//! DECISION: THE MAPPER IS STATEFUL. Codex spreads one turn's outcome across
//! several lines: `thread.started` carries the thread id (the resume handle) and
//! nothing else, while the terminal `turn.completed` repeats neither the id nor
//! the assistant's final text. The runner's [`RunOutcome`] needs both in one
//! place, so the mapper remembers the id and the last assistant message as the
//! lines go by and folds them into the terminal event.
//!
//! Parsing is defensive throughout, mirroring the claude mapper: an unknown
//! `type` and a missing or wrongly-typed field are DROPPED, never turned into a
//! failure. A malformed line must not derail a turn that is otherwise fine.
//!
//! FOLLOW-UP (mapper L3): the item-level events — `item.started`,
//! `item.updated` and `item.completed` — are not mapped yet. They carry the
//! assistant text, reasoning and tool calls, so that work also fills
//! [`EventMapper::last_message`], which is what `turn.completed` reports as the
//! run's result. Until then a successful turn reports `result: None`.

use serde_json::Value;

use crate::engine::{EngineEvent, RunErrorKind, RunOutcome, RunStatus};

/// The fallback error text for a failed turn whose payload carries nothing
/// readable — an error [`RunOutcome`] must never surface an empty reason.
const UNKNOWN_FAILURE: &str = "codex reported a failed turn";

/// Folds a codex JSONL stream into [`EngineEvent`]s, carrying the cross-line
/// state (§the module note) the terminal events need.
#[derive(Debug, Default)]
pub(super) struct EventMapper {
    /// The thread id from `thread.started`, reported on every [`RunOutcome`] as
    /// the session id the next turn resumes from.
    thread_id: Option<String>,
    /// The assistant's most recent final text, reported as a successful turn's
    /// result. Filled in by the item-level mapping (see the FOLLOW-UP note).
    last_message: Option<String>,
}

impl EventMapper {
    /// A mapper for one child process (one turn).
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Map one parsed JSONL `ThreadEvent` to the events it carries, in order.
    /// An empty `Vec` means "carries nothing" — an unknown `type` is DROPPED
    /// defensively, never a parse failure, mirroring the claude mapper.
    pub(super) fn map_line(&mut self, value: &Value) -> Vec<EngineEvent> {
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "thread.started" => self.thread_started(value).into_iter().collect(),
            "turn.completed" => vec![EngineEvent::Result(RunOutcome {
                status: RunStatus::Success,
                result: self.last_message.clone(),
                session_id: self.thread_id.clone(),
                error: None,
                error_kind: None,
            })],
            // `turn.failed` nests the reason under `error`; the stream-level
            // `error` event puts its text at the top level as `message`.
            "turn.failed" => vec![self.failure(value.get("error"))],
            "error" => vec![self.failure(Some(value))],
            _ => Vec::new(),
        }
    }

    /// Capture the resume handle and announce the session. A missing or
    /// non-string `thread_id` captures nothing and emits nothing — resuming a
    /// thread whose id we never learned is not possible, and inventing one would
    /// be worse than running the next turn fresh.
    fn thread_started(&mut self, value: &Value) -> Option<EngineEvent> {
        let thread_id = value.get("thread_id").and_then(Value::as_str)?.to_owned();
        self.thread_id = Some(thread_id.clone());
        Some(EngineEvent::SessionStarted {
            session_id: thread_id,
        })
    }

    /// The error [`RunOutcome`] for a failed turn or a stream-level error.
    ///
    /// DECISION: [`RunErrorKind::Execution`]. `failure::classify_outcome` maps it
    /// to `agent.unknown` — non-retryable and resume-safe, the right posture for
    /// an opaque engine failure whose cause the runner cannot inspect. No new
    /// `RunErrorKind` variant is invented for codex; the enum stays
    /// engine-agnostic.
    fn failure(&self, payload: Option<&Value>) -> EngineEvent {
        EngineEvent::Result(RunOutcome {
            status: RunStatus::Error,
            result: None,
            session_id: self.thread_id.clone(),
            error: Some(error_message(payload)),
            error_kind: Some(RunErrorKind::Execution),
        })
    }
}

/// Read a failure payload's human-readable reason, degrading step by step rather
/// than failing: a nested `message` string, then the payload itself as a string,
/// then its whole JSON serialization, then a fixed fallback.
fn error_message(payload: Option<&Value>) -> String {
    let Some(payload) = payload else {
        return UNKNOWN_FAILURE.to_owned();
    };
    if let Some(message) = payload.get("message").and_then(Value::as_str) {
        return message.to_owned();
    }
    if let Some(text) = payload.as_str() {
        return text.to_owned();
    }
    serde_json::to_string(payload).unwrap_or_else(|_| UNKNOWN_FAILURE.to_owned())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::EventMapper;
    use crate::engine::{EngineEvent, RunErrorKind, RunStatus};

    #[test]
    fn thread_started_announces_the_session() {
        let mut mapper = EventMapper::new();
        let events = mapper.map_line(&json!({"type": "thread.started", "thread_id": "th-1"}));
        assert_eq!(
            events,
            vec![EngineEvent::SessionStarted {
                session_id: "th-1".to_owned()
            }]
        );
    }

    #[test]
    fn turn_completed_carries_the_captured_thread_id() {
        let mut mapper = EventMapper::new();
        mapper.map_line(&json!({"type": "thread.started", "thread_id": "th-1"}));
        let events = mapper.map_line(&json!({"type": "turn.completed"}));
        let [EngineEvent::Result(outcome)] = events.as_slice() else {
            panic!("expected exactly one Result event, got {events:?}");
        };
        assert_eq!(outcome.status, RunStatus::Success);
        assert_eq!(outcome.session_id.as_deref(), Some("th-1"));
        assert!(outcome.error.is_none());
        assert!(outcome.error_kind.is_none());
    }

    #[test]
    fn turn_failed_extracts_the_nested_message() {
        let mut mapper = EventMapper::new();
        mapper.map_line(&json!({"type": "thread.started", "thread_id": "th-1"}));
        let events = mapper.map_line(&json!({"type": "turn.failed", "error": {"message": "boom"}}));
        let [EngineEvent::Result(outcome)] = events.as_slice() else {
            panic!("expected exactly one Result event, got {events:?}");
        };
        assert_eq!(outcome.status, RunStatus::Error);
        assert_eq!(outcome.error.as_deref(), Some("boom"));
        assert_eq!(outcome.error_kind, Some(RunErrorKind::Execution));
        assert_eq!(outcome.session_id.as_deref(), Some("th-1"));
        assert!(outcome.result.is_none());
    }

    #[test]
    fn turn_failed_degrades_through_string_then_json_then_fallback() {
        let mut mapper = EventMapper::new();
        let string_error = mapper.map_line(&json!({"type": "turn.failed", "error": "plain"}));
        let [EngineEvent::Result(outcome)] = string_error.as_slice() else {
            panic!("expected exactly one Result event, got {string_error:?}");
        };
        assert_eq!(outcome.error.as_deref(), Some("plain"));

        let json_error = mapper.map_line(&json!({"type": "turn.failed", "error": {"code": 7}}));
        let [EngineEvent::Result(outcome)] = json_error.as_slice() else {
            panic!("expected exactly one Result event, got {json_error:?}");
        };
        assert_eq!(outcome.error.as_deref(), Some(r#"{"code":7}"#));

        let bare = mapper.map_line(&json!({"type": "turn.failed"}));
        let [EngineEvent::Result(outcome)] = bare.as_slice() else {
            panic!("expected exactly one Result event, got {bare:?}");
        };
        assert_eq!(
            outcome.error.as_deref(),
            Some("codex reported a failed turn")
        );
    }

    #[test]
    fn stream_level_error_reads_its_top_level_message() {
        let mut mapper = EventMapper::new();
        let events = mapper.map_line(&json!({"type": "error", "message": "stream died"}));
        let [EngineEvent::Result(outcome)] = events.as_slice() else {
            panic!("expected exactly one Result event, got {events:?}");
        };
        assert_eq!(outcome.status, RunStatus::Error);
        assert_eq!(outcome.error.as_deref(), Some("stream died"));
        assert_eq!(outcome.error_kind, Some(RunErrorKind::Execution));
        assert!(outcome.session_id.is_none());
    }

    #[test]
    fn unknown_and_malformed_lines_are_dropped() {
        let mut mapper = EventMapper::new();
        // An unmapped type (including the item-level ones a later L3 fills in).
        assert!(
            mapper
                .map_line(&json!({"type": "item.completed"}))
                .is_empty()
        );
        assert!(mapper.map_line(&json!({"type": "turn.started"})).is_empty());
        assert!(mapper.map_line(&json!({"nope": true})).is_empty());
        // A `thread.started` with no usable id announces nothing and captures
        // nothing, so a later terminal event still has no session id.
        assert!(
            mapper
                .map_line(&json!({"type": "thread.started"}))
                .is_empty()
        );
        assert!(
            mapper
                .map_line(&json!({"type": "thread.started", "thread_id": 7}))
                .is_empty()
        );
        let events = mapper.map_line(&json!({"type": "turn.completed"}));
        let [EngineEvent::Result(outcome)] = events.as_slice() else {
            panic!("expected exactly one Result event, got {events:?}");
        };
        assert!(outcome.session_id.is_none());
    }
}
