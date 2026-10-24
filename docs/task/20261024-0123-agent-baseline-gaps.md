# 20261024-0123-agent-baseline-gaps Close the pma-rust baseline gaps in gino-agent

- **status**: completed
- **priority**: P2
- **owner**: station/agent-baseline-20261024-0123
- **createdAt**: 2026-10-24 01:23

## Description

Gaps against the pma-rust acceptance checklist:

- No `[profile.release]` / `[profile.dist]`.
- No panic hook emitting a structured `tracing::error!`; core dumps not suppressed in-process.
- CI lacks typos, an MSRV check and nextest.

Acceptance criteria: the profiles exist and the release workflow builds with `dist`; `main`
installs the panic hook and sets `RLIMIT_CORE` to 0; CI runs typos, `cargo hack check
--rust-version` and `cargo nextest` (plus `cargo test --doc`); all gates pass.

## ActiveForm

Closing the agent baseline gaps

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261024-0123-agent-baseline.md`.

- complete: All agent gates and the docker e2e passed.
