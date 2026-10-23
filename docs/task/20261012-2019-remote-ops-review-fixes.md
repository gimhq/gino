# 20261012-2019-remote-ops-review-fixes Fix remote operations review findings

- **status**: completed
- **priority**: P1
- **owner**: station/review-fixes-20261012-2019
- **createdAt**: 2026-10-12 20:19

## Description

Fix the four findings confirmed by the review of the engine-free file and terminal operations.

Acceptance criteria:

- `put` over an existing file keeps its permission bits and writes through a symlink.
- A terminal whose shell exits while a descendant still holds the pty reports its exit
  promptly, with the exit code and no `slow_consumer` error.
- A terminal ended because its input queue overflowed reports `input_overflow`, and malformed
  input reports `invalid`; neither looks like an ordinary close.
- Deleting a staged transfer leaves no empty directory in the host blob store.
- Each fix has a regression test; the agent and host gates pass.

## ActiveForm

Fixing remote operations review findings

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261012-2019-remote-ops-review-fixes.md`. The user asked for the fixes
directly on 2026-10-12 after reading the review, which satisfies the approval gate.

- complete: Regression tests failed before and pass after; agent gates (fmt, clippy, 431 unit + 21 integration + 1 doctest), host typecheck and check, and a real host/agent run passed. Root lint still fails only in two unchanged scripts.
