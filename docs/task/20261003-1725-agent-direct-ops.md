# 20261003-1725-agent-direct-ops Agent file and environment operations without an engine

- **status**: in_progress
- **priority**: P1
- **owner**: station/session-20261003
- **createdAt**: 2026-10-03 17:25

## Description

Let the host operate on an agent's files and environment directly, without sending a prompt to an
engine: list, read, write and remove files, and run commands, inside a confined root.

Acceptance criteria:

- The host can list, stat, read, write, create, rename and remove files on a connected agent.
- The host can run a command on a connected agent and receive its exit code and output.
- Both are off unless the agent's operator enables them, are confined to a root directory, and are
  reachable on the host only with the admin token.
- No engine process is started by any of these operations.
- An agent or host that does not implement the new frames ignores them; `protocolVersion` stays 2.

## ActiveForm

Adding engine-free file and environment operations to the agent

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261003-1726-agent-direct-ops.md`.
