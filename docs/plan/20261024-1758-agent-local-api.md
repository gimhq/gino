# 20261024-1758-agent-local-api Local agent API socket for in-engine tooling

- **status**: implementing
- **createdAt**: 2026-10-24 17:58
- **approvedAt**: 2026-10-24 18:32
- **relatedTask**: 20261024-1758-agent-local-api

## Context

- The in-engine `gino` CLI (`gino-agent system|cron|task|thread`, `gino-agent/src/gino_cli.rs`,
  used by `skills/gino/SKILL.md`) is a separate short-lived process that opens the daemon's SQLite
  database directly (`Db::open_existing`): reads are plain queries; `cron` mutations write `crons`
  rows that the scheduler picks up by polling, and enqueue `cron_state` into the outbox.
- That database holds every task's messages (`outbox`), prompts (`threads`), crons and the agent
  identity token. Under the engine sandbox it is deliberately not mounted, so the CLI fails with
  "agent state is not available inside the sandbox" and the `gino` skill does not work. A VM guest
  would have the same problem.
- Scoping today is advisory: `--task-id` defaults to `$GINO_TASK_ID` but any task id is accepted,
  and `system` / `task list` / unscoped `cron list` show every task.
- The scheduler already has a `Notify` nudge (`gino-agent/src/scheduler.rs`), so a daemon-side
  mutation can wake it immediately instead of waiting for the poll.
- The sandbox already binds a per-run daemon socket for the TUN handover
  (`/run/gino/ctl.sock`, `gino-agent/src/sandbox/{bwrap,init}.rs`), so binding one more socket
  into the sandbox is an established pattern. libkrun maps a guest vsock port to a host Unix
  socket (`krun_vsock_device_add_unix_port`), so a VM guest can use the same socket.

## Proposal

One local API served by the daemon; every in-engine tool uses it; nothing outside the daemon
opens the database.

### Transport and framing

- The daemon listens on `<workspace>/run/agent.sock` (directory 0700, socket 0600, removed and
  recreated at startup).
- Framing: newline-delimited JSON, one request per line, one response per line, connection may
  carry several requests.
  - Request: `{"v":1,"id":"<client id>","method":"cron.create","params":{...}}`
  - Response: `{"v":1,"id":"<same>","result":{...}}` or
    `{"v":1,"id":"<same>","error":{"code":"forbidden","message":"..."}}`
- Error codes (stable): `unauthenticated`, `forbidden`, `not_found`, `invalid_params`,
  `unsupported_version`, `unavailable`, `internal`.
- Size limit per line (64 KiB) and a per-connection request timeout.

### Authentication and scope

- Every engine run gets a fresh random token (256 bits) bound to its scope
  `{task_id, thread_id}`, registered in the daemon before spawn and revoked when the run ends.
  The engine receives `GINO_AGENT_SOCK` and `GINO_AGENT_TOKEN` (plus the existing
  `GINO_TASK_ID` / `GINO_THREAD_ID`). The first request on a connection is `auth` with the token;
  unauthenticated requests get `unauthenticated`.
- A run-scoped caller can only see and change its own task: task-scoped methods ignore or reject
  any other `task_id` (`forbidden`); listings are filtered to the bound task; `system.info`
  returns agent id, version, capabilities and the caller's own task counts — never the identity
  token, host token, other tasks or file paths.
- Operator access (a shell on the machine as the agent user, outside any engine): no token, the
  connection's peer uid must equal the daemon uid (`SO_PEERCRED`) and the caller must not be a
  sandboxed run — granted full scope. In-engine processes always present a token; a token wins
  over peer credentials.

### Methods (mirror the current CLI)

`auth`, `system.info`, `task.list`, `task.show`, `thread.list`, `thread.show`, `cron.create`,
`cron.list`, `cron.delete`, `cron.pause`, `cron.resume`. Mutations run in the daemon, write the
`crons` row, enqueue `cron_state`, and nudge the scheduler.

### Client and placement

- `gino_cli.rs` becomes a thin client: same subcommands, flags and output text, so the `gino`
  skill keeps working unchanged. It no longer opens the database; with the daemon down it fails
  with "the agent is not running".
- Unsandboxed runs: the engine uses `<workspace>/run/agent.sock` directly.
- Sandboxed runs: the socket is bound read-write into the sandbox at `/run/gino/agent.sock`.
- VM runs (later): vsock port mapped to the same socket; no protocol change.

### Spec

`docs/agent-architecture.md` gains a "Local agent API" section: transport, framing, auth, scope
rules, methods with params/results, error codes, versioning (`v` field; unknown version →
`unsupported_version`). This is the one contract every in-engine tool and the future VM guest
follows.

### Files

New `gino-agent/src/agent_api/{mod.rs,server.rs,client.rs,scope.rs}`; `gino_cli.rs` (client),
`runner.rs` (start/stop server, token registry), `executor.rs` / `engine/{claude,codex}/mod.rs`
(token + socket env), `sandbox/bwrap.rs` (bind the socket), `scheduler.rs` (expose the nudge),
`lib.rs`, `skills/gino/SKILL.md` (only wording that says "no IPC"), `docs/agent-architecture.md`.

### Tests

- Unit: framing, version and size limits, auth, scope filtering for every method, error codes,
  token revocation at run end.
- Integration: the daemon serves the socket; CLI round-trips for every subcommand with output
  identical to today; a run token cannot read or change another task; operator access via peer
  credentials; a sandboxed engine (sandbox-it) runs `gino cron create/list` successfully and
  cannot see other tasks.

## Risks

- CLI output must stay byte-compatible enough for the skill; covered by golden tests.
- The CLI no longer works when the daemon is down (today it reads the database offline). Accepted:
  every in-engine use happens while the daemon runs; operator inspection of a stopped agent uses
  sqlite3 directly.
- Unsandboxed engines run as the daemon's uid and could read another engine's token from
  `/proc/<pid>/environ`; scope is only a hard boundary under the sandbox (separate PID namespace),
  which matches the isolation model.
- A connection storm from a misbehaving engine: bounded connections per token and request
  timeouts.

## Scope

About 10 files in `gino-agent` plus the skill wording and docs, roughly 1200–1600 lines including
tests. No host or wire-protocol change.

## Alternatives

1. Mount a per-task filtered SQLite copy into the sandbox: stale reads, mutations still need a
   path back to the daemon. Rejected.
2. HTTP on loopback instead of a Unix socket: needs a port inside every sandbox netns and does not
   map onto vsock as directly. Rejected.
3. Keep the direct-DB CLI for unsandboxed runs and use the socket only in the sandbox: two code
   paths and two scope models. Rejected in favour of one contract.

## Annotations

(none)
