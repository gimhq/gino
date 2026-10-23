# 20261003-1726-agent-direct-ops Agent file and environment operations without an engine

- **status**: implementing
- **createdAt**: 2026-10-03 17:26
- **approvedAt**: 2026-10-11 01:12
- **relatedTask**: 20261003-1725-agent-direct-ops

## Context

- The frame catalog (`docs/protocol.md` §5) has no file or command frames. Host→agent types are
  `task_upsert`, `task_delete`, `cron_upsert`, `cron_delete`, `message`, `interrupt`, `close`,
  `config_update`. Files and commands are reachable only by prompting an engine.
- The protocol is additive within a major version: both sides must ignore unknown frame types
  (§1, §4.2). The agent does this in `classify_inbound` (`gino-agent/src/transport/ws.rs`,
  `InboundFrameV2::Unknown`); the host does it in the `default:` arm of
  `packages/host-core/src/protocol/inbound.ts`.
- Durable frames are persisted and replayed (agent `inbox`/`outbox`, host `outbox`). A file read or
  a command must not be replayed after a reconnect. `interrupt` already has the right shape: the
  host sends it through `enqueueLiveOnly` and refuses when the agent is offline
  (`packages/host-core/src/tasks/orchestrator.ts`). Agent→host, only `heartbeat` bypasses the
  outbox (`HEARTBEAT_FRAME` in `ws.rs`).
- The agent's working tree is `workspace_dir`; each thread runs in its own `work_dir`, with the
  engine cwd at `work_dir/project/` when provisioning is on (`gino-agent/src/executor.rs`).
- Path confinement is hand-written today (`provisioning::safe_link_path`, `paths::safe_component`).
- Registration advertises `capabilities`, which the host's dispatch reads as engine names
  (`gino-agent/src/register.rs`).
- The agent is outbound-only and Unix-only (`nix` signals).

## Proposal

Three groups of live-only frames (never persisted, acked or replayed): a file
request/response pair, a command request/response pair, and a streaming terminal (PTY) session.
All are correlated by an id at the envelope top level.

### Frames

| type | direction | envelope | data |
|---|---|---|---|
| `fs_request` | host→agent | `reqId` | `{op, path, to?, content?, encoding?, offset?, limit?, recursive?}` |
| `fs_response` | agent→host | `reqId` | `{ok, entries? \| stat? \| content? + encoding + eof, error?}` |
| `exec_request` | host→agent | `reqId` | `{argv, cwd?, env?, stdin?, timeout_ms?}` |
| `exec_response` | agent→host | `reqId` | `{ok, exit_code?, signal?, stdout, stderr, truncated, timed_out, error?}` |
| `pty_open` | host→agent | `ptyId` | `{argv?, cwd?, env?, cols?, rows?}` |
| `pty_input` | host→agent | `ptyId` | `{data}` (base64 keystrokes) |
| `pty_resize` | host→agent | `ptyId` | `{cols, rows}` |
| `pty_close` | host→agent | `ptyId` | — |
| `pty_output` | agent→host | `ptyId` | `{data}` (base64 bytes from the pty) |
| `pty_exit` | agent→host | `ptyId` | `{exit_code?, error?}` |

- `op` ∈ `list`, `stat`, `read`, `write`, `mkdir`, `remove`, `rename`.
- `encoding` ∈ `utf8`, `base64`. `read` and `write` carry at most 1 MiB per frame; larger files
  are read in ranges with `offset`/`limit` and written with `offset`. A read of non-UTF-8 bytes
  returns `base64` regardless of the requested encoding.
- `exec_request` takes an argv array and never goes through a shell. Output is captured up to a
  cap (default 1 MiB per stream) and the command is killed at `timeout_ms` (default 60 s,
  maximum 10 min).
- PTY: `pty_open` spawns `argv` (default `$SHELL`, else `/bin/sh`) on a pseudo-terminal; `pty_input`
  and `pty_output` stream bytes both ways; `pty_resize` sets the window size; `pty_close` and the
  child exiting both end the session with `pty_exit`. The `ptyId` is host-minted and scopes every
  frame of one session. This is the channel for the human-intervention case: a session outlives any
  single request and survives as long as the WebSocket is up.
- Paths are **not confined to a root** (owner decision 3): a relative `path`/`cwd` resolves against
  `workspace_dir`, an absolute one is used as given. The container is the only boundary.
- `error` is `{code, message}` with `code` ∈ `disabled`, `not_found`, `exists`, `too_large`,
  `invalid`, `denied`, `io`.
- A request or `pty_open` sent to an offline agent fails at once; when the agent disconnects, the
  host rejects every pending request and ends every open PTY session with `agent_disconnected`.

### Enablement (owner decision 2 — host-driven)

The host decides whether an agent accepts these frames; the agent has no local switch. The agent
gains three booleans in its host-assigned config — `remoteOps: {fs, exec, pty}`, all default
`false` — delivered the same way as the other config fields: in the register-response config, the
`GET /agent/config` mirror, and live `config_update` (`gino-agent/src/config.rs` `ConfigOverlay`).
A frame whose group is not enabled is answered `disabled`. There is no agent-side env or flag; an
agent whose host never sets the flags behaves exactly as today.

### Agent (`gino-agent`)

- New module `remote_ops/` with `frames.rs` (wire types), `fs.rs`, `exec.rs`, `pty.rs`.
- Files use `std::fs` on the blocking pool. No library confinement: a relative path joins
  `workspace_dir`, an absolute path is used as given (decision 3). `..` is not rejected.
- Commands use `tokio::process` (already a dependency) with `kill_on_drop`, cwd defaulting to
  `workspace_dir`, and the agent's environment minus `GINO_*` and the credential variables, plus
  any `env` the request adds.
- PTY uses `pty-process` 0.5.3 (`async` feature; Unix-only, matching the crate). Each session owns
  a spawned child on a pty and a task that pumps pty→`pty_output` and `pty_input`→pty until
  `pty_close`, child exit, or disconnect; `kill_on_drop` reaps the child.
- `InboundFrameV2` gains the new host→agent variants (or a sibling pre-parse in `classify_inbound`):
  they are handled live, before the inbox write, and responses go straight to the socket like
  `heartbeat`. None of them touch `inbox`/`outbox`.
- A live-send path to the socket is added for agent→host frames that must not be queued
  (`fs_response`, `exec_response`, `pty_output`, `pty_exit`) — the outbox is for durable result
  frames only.

### Host

- `packages/host-core`: `protocol/frames.ts` gains the ten schemas (and the generated §8 block);
  `protocol/inbound.ts` routes the agent→host responses to a new `agents/remote.ts`, which holds
  the per-host pending-request map (fs/exec), the open-PTY-session registry, timeouts, and
  rejection/teardown on detach.
- SDK surface: `host.agents.fs(agentId, request)`, `host.agents.exec(agentId, request)`, and a PTY
  handle `host.agents.openPty(agentId, opts)` exposing `write`, `resize`, `close`, and an output
  stream plus an exit promise.
- `packages/host-hono`: `POST /admin/agents/:id/fs` and `POST /admin/agents/:id/exec`; a WebSocket
  route `GET /admin/agents/:id/pty` that bridges a browser terminal (e.g. `@xterm/xterm`) to a PTY
  session. All under the existing admin `authorize`; the PTY route needs the injected
  `upgradeWebSocket` the agent router already uses (`host-hono` `./bun` / `./node`).
- Enablement config: the host adds `remoteOps` to the agent config it serves
  (`host-core/src/host/wire/config.ts` `agentConfig`), surfaced through the admin API so an operator
  can turn each group on per agent.
- `apps/host`: rate limit on the new routes.

### Documentation

`docs/protocol.md` §2, §5 and §7; `docs/agent-architecture.md` and `docs/host-architecture.md`;
`gino-agent/README.md`; `e2e/TEST-COVERAGE.md`.

### Tests (written first)

- Agent unit: every `op`; size cap; ranged read and write; base64 round-trip of binary content;
  exec exit code, output cap, timeout kill, env scrubbing; pty echo round-trip, resize, exit on
  close and on child exit; each group answers `disabled` until the host enables it.
- Agent integration (`tests/e2e.rs` stub host): request → response over the socket; a full pty
  open/input/output/close; nothing written to `inbox` or `outbox` by any of them.
- Host unit: pending-map resolve, timeout, detach rejection, unknown `reqId`/`ptyId` ignored,
  offline agent refused; PTY registry teardown on detach.
- Host integration: the admin fs/exec routes and the pty WebSocket bridge, admin-token only.
- Docker e2e: enable the groups via config, write a file, read it back, run `ls` on it, open a pty
  and echo a line, all with the fake engine never invoked.

### Out of scope

- Durable or replayable operations — every frame here is live-only by design.
- Files larger than the 1 MiB frame cap in a single transfer — handled by ranged read/write now, and
  by the attachment byte channel in a later step if a one-shot large transfer is wanted.

## Risks

- **This is remote code execution and unrestricted filesystem access on the agent's machine by
  design.** The two owner decisions widen the blast radius versus the first draft, and this must be
  stated plainly in the docs:
  - **No agent-side defense (decision 2).** Enablement is entirely host-driven, so whoever controls
    the host controls every enabled agent's shell and filesystem. The agent cannot refuse. The only
    guards left are: default-off (the host must switch each group on per agent), admin-token-only on
    the host surface, argv without a shell, timeouts and output caps.
  - **No path confinement (decision 3).** `fs` and `exec` reach anywhere the agent's uid can,
    bounded only by the container. A leaked admin token is a root-of-the-container shell on every
    enabled agent.
  - The trust boundary stays the container — the engines already run there with full access — but
    the token that opens that boundary is now the host's admin token, so its handling (rotation,
    scope, never logged) is load-bearing. The default-off switch is what keeps a fleet that does not
    use this feature unexposed.
- The PTY WebSocket bridge is a second authenticated upgrade path on the admin surface; it must
  enforce the same admin auth as the mounted routes before the socket opens (the agent router's
  `connectTokenAuth`-at-upgrade pattern is the model).
- Large frames and fast PTY output block the single agent socket; the 1 MiB fs cap bounds file
  transfers, and PTY output should be chunked so one burst cannot starve work frames.
- `pty-process` is Unix-only — consistent with the agent, which is already Unix-only (`nix`).

## Scope

Agent: about 6 new files and 4 edited (`protocol.rs` or `transport/ws.rs` for the live frames,
`transport/mod.rs` for the live-send path, `runner.rs`, `config.rs`). Host: about 3 new files and
~7 edited across `host-core`, `host-hono` and `apps/host`. One new Rust dependency
(`pty-process`, `async` feature; `base64` is already in the lockfile). No database migration on
either side. No protocol version bump — all frames are additive and ignored by an un-upgraded peer.

## Alternatives

- **Durable frames through the existing outbox.** Rejected: a replayed `remove`, command, or
  keystroke after a reconnect is unsafe.
- **A second WebSocket on the agent dedicated to operations.** Cleaner isolation from work frames,
  but a second outbound connection and reconnect loop on the agent. Reusing the one existing socket
  with live-only frames is simpler and the PTY chunking bounds head-of-line blocking.
- **Embedded SSH/SFTP server (`russh`).** Rejected: the agent would have to listen for inbound
  connections, breaking the outbound-only model.
- **Agent-side opt-in (the first draft).** Superseded by owner decision 2 — enablement is
  host-driven. Recorded here because it was the stronger security posture; the trade-off is noted
  under Risks.

## Annotations

- 2026-10-06 — Owner decisions, which supersede the matching parts of the first draft:
  1. **`exec` and PTY are both in scope.** Reason: the engine can already perform operations, but
     when a turn needs a human to step in, the engine channel is gone; a terminal-style session is
     needed to take over. PTY moved from "out of scope" into the proposal.
  2. **The host decides enablement, not the agent.** The agent-side `GINO_REMOTE_OPS` switch and the
     registration `features` array are dropped; enablement rides the host-assigned config
     (`remoteOps: {fs, exec, pty}`, default off). Risks updated — there is no longer any agent-side
     defense.
  3. **No root confinement.** `GINO_REMOTE_OPS_ROOT` and the `cap-std` sandbox are dropped; relative
     paths resolve against `workspace_dir`, absolute paths are honored, the container is the only
     boundary. Risks updated.
- 2026-10-06 — Implementation status: a first write of the agent frame/`fs` modules was blocked by
  the Claude Code safety classifier ("create RCE surface"); no feature code is on disk. Awaiting the
  owner's chosen path (grant a Bash permission rule for these writes, or land the code by hand).
  This plan stays `draft` until implementation can proceed.
- 2026-10-11 — Second investigation pass; implementation was stopped by the safety classifier again
  before any feature code was written. Open points found, to settle before implementation:
  1. **Where the per-agent `remoteOps` flags persist.** The `agents` table has no field for them
     and `AgentRepo` has no method that updates `capabilities`. Either store them inside the
     `capabilities` JSON column (no migration, but a new port method in the memory and sqlite
     adapters) or add a column (a migration, which contradicts *Scope*). Owner decision needed.
  2. **Teardown hook.** `SessionRegistry` drops a session on four paths (`attach` supersede,
     `detach`, `closeAgent`, `closeAll`); `wire/socket.ts` `detachSocket` only observes one, and
     reports `evicted = false` after `closeAgent`. Rejecting pending requests and ending PTY
     sessions needs a hook in `SessionRegistry`, not in `detachSocket`.
  3. **Task file is stale.** `docs/task/20261003-1725-agent-direct-ops.md` still lists "confined to
     a root" and "the agent's operator enables them" as acceptance criteria; both contradict owner
     decisions 2 and 3.
  4. `paths.workspaceDir` is host-overridable in `ConfigOverlay`, so the base for relative paths
     follows the live config, not the startup value.
- 2026-10-11 — Owner decision 4: **implement the file group first; `exec` and PTY are deferred.**
  This pass delivers `fs_request`/`fs_response` end to end and the `remoteOps.fs` switch only. The
  `exec_*` and `pty_*` frames, `remoteOps.exec`/`remoteOps.pty`, the PTY WebSocket route and the
  `pty-process` dependency stay in this plan as a later pass. Open point 1 is settled for this
  pass as: the flags live in the `capabilities` JSON column (`remote_ops`), no migration.
- 2026-10-11 — File group implemented (plan stays `implementing` for the deferred `exec`/PTY pass).
  - Agent: `remote_ops/` (`mod.rs`, `fs.rs`), a live-send channel in `transport/ws.rs`,
    `remoteOps.fs` in `config.rs`, wiring in `runner.rs`. New dependency `base64` 0.23 (latest
    stable; the lockfile also keeps the transitive 0.22).
  - Host: `agents/remote.ts`, `fs_request`/`fs_response` schemas, `reqId` on the envelope,
    `SessionRegistry.onDrop`, `AgentRepo.setCapabilities` (memory + sqlite, no migration),
    `host.agents.fs` / `host.agents.setRemoteOps`, `POST /admin/agents/:id/fs` and
    `POST /admin/agents/:id/remote-ops`.
  - Verified: agent `cargo fmt --check`, `cargo clippy --all-targets`, `cargo test` (rust:1.96
    container); host typecheck and every package's `bun test`; a manual run of the real host
    against the real agent binary covering enable, mkdir, write, read, ranged read, base64, list,
    absolute path, rename, remove, disable and the offline refusal.
  - Not done: the `apps/host` rate limit (no admin route is rate limited today; left for the
    `exec`/PTY pass, where it matters most); the Docker e2e scenario is written
    (`e2e/e2e.ts` `testRemoteFs`) but the compose suite was not run; `tests/node/conformance.mts`
    was not run because it needs a `dist` build.
- 2026-10-12 — Owner decision 5: **file contents go over the attachment byte channel only.** The
  inline `read`/`write` operations, their `base64` encoding and the 1 MiB ranged transfer are
  removed (and with them the `base64` dependency); `fs_request` gains `put` and `get`, which
  carry a reference instead of bytes. The host stages a transfer in the blob store under a
  host-minted single-use name: `GET /agent/attachments/:id` serves it only to the agent it was
  minted for while the request is pending, and the staged copy is deleted when the transfer
  settles. New surface: `host.agents.putFile` / `getFile`, `PUT|GET /admin/agents/:id/files`.
  `/admin/agents/:id/fs` accepts the metadata operations only. No migration.
  Verified by the agent gates (rust:1.96), the host suites, and a manual run of the real host
  against the real agent binary (text and 3 MB binary upload, overwrite, download, absolute path,
  refusals; no staged bytes left on the host, no cache copy on the agent). Each transfer holds the
  whole file in memory on both sides, bounded by the existing upload and attachment limits.
