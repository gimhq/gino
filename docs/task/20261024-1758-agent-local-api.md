# 20261024-1758-agent-local-api Local agent API socket for in-engine tooling

- **status**: in_progress
- **priority**: P1
- **owner**: station/agent-local-api-20261024-1758
- **createdAt**: 2026-10-24 17:58

## Description

The in-engine `gino` CLI opens the daemon's SQLite database directly, which the engine sandbox
deliberately hides, so the `gino` skill does not work in sandboxed (and future VM) runs. Add one
daemon-served local API over a Unix socket with per-run tokens scoped to the run's task; the CLI
becomes a client of it and nothing outside the daemon opens the database. Specify the contract in
docs/agent-architecture.md.

Acceptance criteria: every `gino` subcommand works through the socket with unchanged output; a
run can only see and change its own task; a sandboxed engine can use `gino cron` successfully;
agent gates and sandbox-it pass.

## ActiveForm

Adding the local agent API

## Dependencies

- **blocked by**: (none)
- **blocks**: 20261024-0130-engine-vm-isolation

## Notes

Plan: [20261024-1758-agent-local-api](../plan/20261024-1758-agent-local-api.md)
