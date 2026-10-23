# 20261013-1955-engine-turn-end Engine turn-end detection for claude and codex

- **status**: completed
- **priority**: P1
- **owner**: station/engine-turn-end-20261013-1955
- **createdAt**: 2026-10-13 19:55

## Description

Both engine backends end a run on an event that the current CLIs do not treat as the end of
the turn (verified against `claude 2.1.296` and `codex-cli 0.162.1`).

Acceptance criteria:

- A claude turn that dispatches a subagent ends on the result of the turn that follows the
  subagent's report, not on the first `result`.
- A claude CLI that does not report session state still ends on its first `result`.
- A claude child that exits after a `result` but before reporting idle still yields that result.
- A codex stream-level `error` (a reconnect notice) does not end the run; `turn.failed` and
  `turn.completed` still do.
- Each fix has a regression test; the agent gates pass.

## ActiveForm

Fixing engine turn-end detection

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261013-1955-engine-turn-end.md`. The user asked on 2026-10-13 for the
agent's protocol handling to be compared against the current CLIs and fixed, which satisfies
the approval gate.

- complete: Regression tests added; the session test fails without the env export and passes with it. Agent gates passed (fmt, clippy, 436 unit + 21 integration + 1 doctest). Not run end to end against a live host.
