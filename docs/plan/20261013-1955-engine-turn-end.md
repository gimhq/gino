# 20261013-1955-engine-turn-end Engine turn-end detection for claude and codex

- **status**: completed
- **createdAt**: 2026-10-13 19:55
- **approvedAt**: 2026-10-13 19:55
- **relatedTask**: 20261013-1955-engine-turn-end

## Context

Real turns were captured with the agent's own argv and handshake from `claude 2.1.296` and
`codex-cli 0.162.1`, and compared with `gino-agent/src/engine/{claude,codex}/events.rs`.

1. Claude emits more than one `result` per prompt. The `Agent` tool now launches subagents
   asynchronously: the first `result` (`result_index: 0`) carries interim text such as "the
   subagent is still running", then the CLI re-emits `system/init`, runs another turn on the
   subagent's report and emits a second `result` (`origin.kind: "task-notification"` or
   `"peer"`) with the real answer. `executor::drive` returns on the first
   `EngineEvent::Result` and drops the session, which kills the child, so the run records the
   interim text and the answer is lost. Neither `queued_turn_count` (0 on every result) nor
   `background_tasks_changed` (already empty before the first result) predicts the second turn.
2. With `CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS=1` the CLI emits
   `system/session_state_changed`: `running` before `init`, and `idle` once, after the last
   `result`. After an `interrupt` the error `result` is followed by `idle` within 2 ms.
3. `codex exec --json` emits `{"type":"error","message":"Reconnecting... ..."}` for every
   stream retry and keeps the turn running. The mapper turns the first one into an error
   `Result`, so a transient network drop fails the run and kills a child that was recovering.

The rest of both mappings matches the wire: codex item kinds and fields, claude
`parent_tool_use_id` attribution, the control handshake and the auto-answers. Empty claude
`thinking` blocks (the CLI now sends a signature and no text) are already suppressed by
`executor::transcript_event`.

## Proposal

1. `claude/mod.rs`: export `CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS=1` to the child.
2. `claude/events.rs`: add a stateful `EventMapper` over the existing `map_line`. Once a
   `session_state_changed` line has been seen, a `result` is held (the latest replaces the
   previous one) and released when the state becomes `idle`. Before any such line a `result`
   is forwarded at once, which keeps older CLIs working. `finish()` releases a held result at
   EOF. `session_state_changed` itself produces no event.
3. `claude/mod.rs`: the reader task owns the mapper and calls `finish()` on EOF.
4. `codex/events.rs`: a top-level `error` becomes a warn `Log`; only `turn.failed` and
   `turn.completed` end the turn. A stream that ends with neither is still reported by the
   executor as a process failure.
5. Update the mapping tables in `docs/agent-architecture.md`.

## Risks

- A CLI that reports `running` but never `idle` would hold its result until the child exits or
  the watchdog fires. The observed CLI reports `idle` after every result, including an
  interrupted one.
- A reconnect notice is now a status frame rather than a failure; a turn that never recovers
  ends on codex's own `turn.failed` or on the inactivity watchdog.

## Scope

- `gino-agent/src/engine/claude/events.rs`, `gino-agent/src/engine/claude/mod.rs`
- `gino-agent/src/engine/codex/events.rs`
- `docs/agent-architecture.md`, `docs/changelog.md`

Out of scope: filtering the `system/<subtype>` status frames (`thinking_tokens`, `status`,
`task_*`), `rate_limit_event`, and result usage or cost fields.

## Alternatives

- Wait a fixed grace period after each `result`: slows every run and still races a slow
  subagent.
- Use `result.origin` or `queued_turn_count`: neither is set on the first result.
