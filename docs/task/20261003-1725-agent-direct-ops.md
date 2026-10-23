# 20261003-1725-agent-direct-ops Agent file and environment operations without an engine

- **status**: in_progress
- **priority**: P1
- **owner**: station/session-20261003
- **createdAt**: 2026-10-03 17:25

## Description

Let the host operate on an agent's files and environment directly, without sending a prompt to an
engine: list, read, write and remove files, run commands, and open a terminal.

Acceptance criteria (file group — first pass):

- The host can list, stat, read, write, create, rename and remove files on a connected agent.
- The group is off unless the host enables it for that agent, and is reachable on the host only
  with the admin token. Paths are not confined to a root; the container is the boundary.
- No engine process is started by any of these operations.
- An agent or host that does not implement the new frames ignores them; `protocolVersion` stays 2.

Deferred to a later pass: running a command (`exec`) and the terminal session (PTY).

## ActiveForm

Adding engine-free file and environment operations to the agent

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261003-1726-agent-direct-ops.md`.

2026-10-11: file group delivered and verified; `exec` and PTY remain. Task stays in progress.
2026-10-12: file contents moved to the attachment byte channel (`put`/`get`); inline read/write removed.
