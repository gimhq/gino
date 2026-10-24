# 20261024-0058-engine-sandbox Filesystem and network restrictions for engine runs

- **status**: in_progress
- **priority**: P2
- **owner**: station/engine-sandbox-20261024-0058
- **createdAt**: 2026-10-24 00:58

## Description

Engine children (claude, codex) run with every permission bypassed. gino-agent will run
natively on the host and launch each engine run through bubblewrap, with filesystem and
network restrictions and per-task isolation, configured through the existing config overlay.

Acceptance criteria: with the sandbox enabled, a run sees only system paths, its own thread
work dir and per-thread engine home; it cannot see other threads, the agent DB or other runs'
processes; direct egress fails and only `allowedDomains` are reachable through the agent's
proxy. With it disabled, behavior is unchanged. When enabled and bubblewrap is unusable, runs
fail closed with a clear error. Agent fmt, clippy and tests pass.

## ActiveForm

Adding an engine sandbox

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: [20261024-0058-engine-sandbox](../plan/20261024-0058-engine-sandbox.md)
