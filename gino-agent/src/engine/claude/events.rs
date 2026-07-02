//! stream-json line parsing → [`EngineEvent`] (PLAN-001 §"event mapping").
//!
//! Every non-control line on the child's stdout is a stream-json message whose
//! shape mirrors the Claude Agent SDK. This module maps the messages the runner
//! cares about onto engine-agnostic [`EngineEvent`]s:
//!
//! | stdout message              | event                          |
//! |-----------------------------|--------------------------------|
//! | `system` / `init`           | [`EngineEvent::SessionStarted`] (carries `session_id`) |
//! | `assistant`                 | [`EngineEvent::Output`] (text) |
//! | `result`                    | [`EngineEvent::Result`] (turn complete; error subtypes → [`RunStatus::Error`]) |
//! | `system` / `api_retry`,`hook_*` | [`EngineEvent::Log`]       |
//! | `user` (echo), unknown      | dropped (`None`)               |
//!
//! Parsing is defensive — missing fields degrade to defaults so a single odd line
//! never derails the stream. The consumer persists the session id from
//! `SessionStarted` and maps `Result` onto the host `output` frame.

use serde_json::Value;

use crate::engine::{EngineEvent, LogLevel, RunErrorKind, RunOutcome, RunStatus};

/// Map a parsed stdout stream-json line to an [`EngineEvent`], or `None` for
/// lines the runner ignores (echoed `user` turns, unrecognised types). The caller
/// must have already excluded `control_request`/`control_response` lines.
pub(crate) fn map_line(value: &Value) -> Option<EngineEvent> {
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "system" => map_system(value),
        "assistant" => Some(map_assistant(value)),
        "result" => Some(EngineEvent::Result(map_result(value))),
        _ => None,
    }
}

/// `system` messages: `init` carries the session id; everything else
/// (`api_retry`, `hook_*`, …) is a log line.
fn map_system(value: &Value) -> Option<EngineEvent> {
    let subtype = value
        .get("subtype")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if subtype == "init" {
        let session_id = value.get("session_id").and_then(Value::as_str)?.to_owned();
        return Some(EngineEvent::SessionStarted { session_id });
    }
    let level = if subtype == "api_retry" {
        LogLevel::Warn
    } else {
        LogLevel::Debug
    };
    Some(EngineEvent::Log {
        level,
        message: format!("system/{subtype}"),
    })
}

/// `assistant` messages: concatenate the `text` blocks into an output chunk.
fn map_assistant(value: &Value) -> EngineEvent {
    let text = value
        .get("message")
        .and_then(|m| m.get("content"))
        .and_then(Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<String>()
        })
        .unwrap_or_default();
    EngineEvent::Output { text }
}

/// `result` messages: build the [`RunOutcome`]. A `success` subtype with
/// `is_error != true` is a success; anything else (e.g. `error_max_turns`, or
/// `is_error: true`) is an error carrying the message as its detail.
fn map_result(value: &Value) -> RunOutcome {
    let subtype = value
        .get("subtype")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let is_error = value
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let text = value
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let session_id = value
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::to_owned);

    if subtype == "success" && !is_error {
        RunOutcome {
            status: RunStatus::Success,
            result: text,
            session_id,
            error: None,
            error_kind: None,
        }
    } else {
        // The error detail is the result text when present, else the subtype.
        let error = text
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| (!subtype.is_empty()).then(|| subtype.to_owned()));
        RunOutcome {
            status: RunStatus::Error,
            result: text,
            session_id,
            error,
            error_kind: error_kind_of(subtype),
        }
    }
}

/// The structured error cause for a non-success `result.subtype`. Exact-match
/// only over the subtypes the CLI emits; anything unrecognised (including a
/// `success` subtype flagged `is_error`) carries no kind and is left to the
/// classifier's conservative fallback.
fn error_kind_of(subtype: &str) -> Option<RunErrorKind> {
    match subtype {
        "error_max_turns" => Some(RunErrorKind::MaxTurns),
        "error_during_execution" => Some(RunErrorKind::Execution),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::map_line;
    use crate::engine::{EngineEvent, LogLevel, RunErrorKind, RunStatus};

    #[test]
    fn init_becomes_session_started() {
        let value = json!({
            "type": "system", "subtype": "init",
            "session_id": "sess-abc", "model": "claude-opus-4-8", "tools": []
        });
        assert_eq!(
            map_line(&value),
            Some(EngineEvent::SessionStarted {
                session_id: "sess-abc".to_owned()
            })
        );
    }

    #[test]
    fn init_without_session_id_is_dropped() {
        let value = json!({ "type": "system", "subtype": "init" });
        assert_eq!(map_line(&value), None);
    }

    #[test]
    fn api_retry_is_a_warn_log() {
        let value = json!({ "type": "system", "subtype": "api_retry", "attempt": 2 });
        assert_eq!(
            map_line(&value),
            Some(EngineEvent::Log {
                level: LogLevel::Warn,
                message: "system/api_retry".to_owned()
            })
        );
    }

    #[test]
    fn hook_events_are_debug_logs() {
        let value = json!({ "type": "system", "subtype": "hook_started" });
        let Some(EngineEvent::Log { level, message }) = map_line(&value) else {
            panic!("expected a log event");
        };
        assert_eq!(level, LogLevel::Debug);
        assert_eq!(message, "system/hook_started");
    }

    #[test]
    fn assistant_text_blocks_are_concatenated() {
        let value = json!({
            "type": "assistant",
            "message": { "role": "assistant", "content": [
                { "type": "text", "text": "Hello " },
                { "type": "tool_use", "name": "Bash", "input": {} },
                { "type": "text", "text": "world" }
            ]}
        });
        assert_eq!(
            map_line(&value),
            Some(EngineEvent::Output {
                text: "Hello world".to_owned()
            })
        );
    }

    #[test]
    fn successful_result_carries_text_and_session() {
        let value = json!({
            "type": "result", "subtype": "success", "is_error": false,
            "result": "all done", "session_id": "sess-abc"
        });
        let Some(EngineEvent::Result(outcome)) = map_line(&value) else {
            panic!("expected a result event");
        };
        assert_eq!(outcome.status, RunStatus::Success);
        assert_eq!(outcome.result.as_deref(), Some("all done"));
        assert_eq!(outcome.session_id.as_deref(), Some("sess-abc"));
        assert!(outcome.error.is_none());
        assert!(outcome.error_kind.is_none());
    }

    #[test]
    fn success_prose_never_carries_an_error_kind() {
        // The audit's false positives: error-looking prose ("took 1500ms",
        // "400") in a successful result must not surface a structured cause.
        let value = json!({
            "type": "result", "subtype": "success", "is_error": false,
            "result": "the request took 1500ms and returned 400 rows"
        });
        let Some(EngineEvent::Result(outcome)) = map_line(&value) else {
            panic!("expected a result event");
        };
        assert_eq!(outcome.status, RunStatus::Success);
        assert!(outcome.error_kind.is_none());
    }

    #[test]
    fn error_subtypes_map_to_structured_kinds() {
        let cases = [
            ("error_max_turns", Some(RunErrorKind::MaxTurns)),
            ("error_during_execution", Some(RunErrorKind::Execution)),
            ("error_never_heard_of", None),
        ];
        for (subtype, kind) in cases {
            let value = json!({ "type": "result", "subtype": subtype, "is_error": true });
            let Some(EngineEvent::Result(outcome)) = map_line(&value) else {
                panic!("expected a result event for {subtype}");
            };
            assert_eq!(outcome.status, RunStatus::Error, "{subtype}");
            assert_eq!(outcome.error_kind, kind, "{subtype}");
        }
    }

    #[test]
    fn error_subtype_result_is_an_error_outcome() {
        let value = json!({
            "type": "result", "subtype": "error_max_turns",
            "is_error": true, "session_id": "sess-abc"
        });
        let Some(EngineEvent::Result(outcome)) = map_line(&value) else {
            panic!("expected a result event");
        };
        assert_eq!(outcome.status, RunStatus::Error);
        assert_eq!(outcome.error.as_deref(), Some("error_max_turns"));
        assert_eq!(outcome.session_id.as_deref(), Some("sess-abc"));
        assert_eq!(outcome.error_kind, Some(RunErrorKind::MaxTurns));
    }

    #[test]
    fn is_error_flag_overrides_success_subtype() {
        let value = json!({
            "type": "result", "subtype": "success", "is_error": true,
            "result": "tool failure"
        });
        let Some(EngineEvent::Result(outcome)) = map_line(&value) else {
            panic!("expected a result event");
        };
        assert_eq!(outcome.status, RunStatus::Error);
        assert_eq!(outcome.error.as_deref(), Some("tool failure"));
        assert!(
            outcome.error_kind.is_none(),
            "a success subtype flagged is_error has no structured cause"
        );
    }

    #[test]
    fn echoed_user_and_unknown_lines_are_dropped() {
        assert_eq!(map_line(&json!({ "type": "user", "message": {} })), None);
        assert_eq!(map_line(&json!({ "type": "stream_event" })), None);
    }
}
