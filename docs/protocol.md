# Host ⇄ Agent Protocol (the contract)

This document is the **single coupling point** between the two otherwise-independent
architectures:

- **Host** (control plane) — `packages/host-*` + `apps/host`, TypeScript. Orchestration, launchers, storage, UI.
- **Agent / runner** (data plane) — `gino-agent/**`, Rust. Executes work via engines.

They share **no code**. Each implements this protocol independently and is versioned and
deployable on its own. Anything not specified here is private to one side and must not be
assumed by the other. If a change needs both sides to agree, it is a change to *this
document* (and the protocol version) — nothing else.

The model is **task / cron / thread**. It pairs with the data-plane spec
[`docs/agent-architecture.md`](agent-architecture.md) (§10 transport, §12 identifiers) and
the host spec [`docs/host-architecture.md`](host-architecture.md). The runner types live in
`gino-agent/src/protocol.rs`.

## 1. Versioning

- The protocol carries a `protocolVersion` (integer, current = **2**).
- The agent sends `protocolVersion` in `POST /agent/register`; the host returns the
  version it will speak in the register response. If they cannot agree, the host returns
  `register_error { reason: "unsupported_protocol" }` and the agent exits.
- Compatibility rule: **additive only within a major version** — new optional envelope
  fields and new envelope `type`s are non-breaking; receivers MUST ignore unknown
  `type`s and unknown fields. Removals/renames/semantics changes bump the version.

## 2. Transport

- **WebSocket is the primary, bidirectional channel** for all work and results.
- **HTTP** is used only for enrollment + bootstrap + a pre-WS output fallback + the
  attachment byte channel: `POST /agent/register`, `GET /agent/config/:agentId`,
  `POST /agent/output/:agentId`, and (host-proxy backend) `GET /agent/attachments/:id`
  (an attachment by descriptor id), `GET /agent/attachments?sha256=<hex>` (a
  config/skill artifact in the agent's desired set, content-addressed — the
  provisioning fetch), and `POST /agent/attachments` (upload: bytes ride the raw
  body; metadata rides the `x-attachment-name` / `x-attachment-sha256` headers or
  equivalent query params; the response is the flat descriptor JSON `{id, name,
  content_type, size, sha256, ref, direction}`). A descriptor's `ref` is a
  byte-channel path the agent resolves against its host base URL
  (`/agent/attachments/<id>`) — never a host-internal storage path.
- **Live-only frames** ride the same WebSocket but bypass both delivery buffers: the
  `fs_request` / `fs_response` pair (§5) is never persisted, acked or replayed. It is
  correlated by a host-minted top-level `reqId`, and a request can only be sent to an
  agent that is connected right now.
- The agent always **dials out** to the host (NAT/firewall friendly). The host never
  connects into the agent; out-of-band control is the launcher's job (see
  [`docs/host-architecture.md`](host-architecture.md)), not part of this protocol.

## 3. Enrollment — `POST /agent/register`

Unauthenticated by connect token; carries the bootstrap **register token** instead.

Request:
```json
{ "registerToken": "<secret>", "protocolVersion": 2,
  "name": "<human label?>", "capabilities": ["claude", ...],
  "version": "<agent build version?>", "platform": "<arch-os?>", "source": "docker|vm|bare?" }
```
Response (success):
```json
{ "agentId": "<host-generated>", "connectToken": "<per-agent secret>",
  "config": { "agentId": "...", "wsUrl": "/agent/ws/<agentId>",
              "configUrl": "/agent/config/<agentId>", "outputUrl": "/agent/output/<agentId>",
              "heartbeatIntervalMs": 30000, "protocolVersion": 2,
              "entries": [ { "name": "...", "sha256": "...", "target": "...", "kind": "file|dir" } ],
              "version": 0,
              "remoteOps": { "fs": false, "pty": false } } }
```

`config.entries` is the GLOBAL-scope desired provisioning set and `config.version`
the host-assigned monotonic global config version (agent-architecture §9b): the
agent reconciles the global scope only when `version` advances past its applied
version. The same two fields are delivered on `GET /agent/config/:agentId`
(which returns the config object FLAT, not nested under `config`) and on
`config_update`.
`config.remoteOps` carries the host-set switches for engine-free operations (§5); every
group is `false` until an operator enables it for that agent. It is delivered the same
three ways. The agent has no local switch and never sets it itself: a `remote_ops` value
in the register body's `capabilities` is ignored.
Response (failure): `{ "register_error": { "reason": "invalid_register_token | rate_limited | invalid_body | registration_disabled | unsupported_protocol" } }`.

Rules:
- The host generates `agentId`. The agent does not choose it.
- The **connect token** authenticates the WS + HTTP config/output/attachments (header
  `x-internal-token`). It is **never** the register token; the agent persists it locally
  (`connect.token`) and reuses it on reconnect — no re-register needed.
- The host is the authority for `config` (credentials/engine settings arrive via the
  config layer; see [`docs/host-architecture.md`](host-architecture.md)).
- `capabilities` advertises the engines the runner can **actually run** — a runtime property
  (the agent probes for each engine's binary at registration), not a build constant. The host
  matches a task's `engine` (§7 `task_upsert`) against that list when **placing** the task, so an
  engine no permitted agent advertises leaves the task unplaced rather than failing it.

## 4. WebSocket framing + delivery guarantee

`GET /agent/ws/:agentId` with header `x-internal-token: <connectToken>`.

### 4.1 Envelope vs payload (the core rule)

Every frame is one JSON object. **Correlation/delivery metadata lives at the envelope top
level; `data` carries the type-specific business payload only.**

```json
{ "type": "thread_message", "outboxId": 43, "taskId": "t-1", "threadId": "c-9@1718…",
  "data": { "seq": 3, "kind": "assistant_text", "content": "…" } }
```

- **Envelope** keys: `type`; the delivery ids `outboxId` / `msgId`; the correlation ids
  `taskId` / `threadId` / `cronId`; and `reqId` on live-only file requests or `ptyId` on live terminal sessions.
- **`data`**: business fields only — it **never** repeats the correlation ids. In
  particular `engine_session_id` is a *value* of a thread (the engine `--resume` handle),
  so it lives **inside `data`** on `thread_result` (stored, not routed-by).
- The host routes and correlates by the **envelope** ids *without parsing `data`*,
  mirroring the runner's `inbox`/`outbox` correlation columns and the host read-model keys.
- Which ids are present depends on the frame (see §5). `interrupt` MAY carry `taskId` to
  scope the cancel to one task; `close` and the acks carry no correlation ids; deletes
  carry only their identity ids and no `data`.

### 4.2 Delivery — at-least-once both ways

- Sender persists each outbound frame (outbox) before sending; unacked frames replay on
  reconnect.
- **Agent→host**: the host replies `{ "type": "ack", "id": <outboxId> }` after the durable
  mirror write; the agent clears the outbox row on that ack.
- **Host→agent**: EVERY durable host→agent frame carries `msgId` (the host outbox row id);
  the agent dedups inbound frames on `msgId` and replies
  `{ "type": "msg_ack", "msgId": <int> }` once it has durably stored the frame — that
  `msg_ack` (not `ack{id}`) is what stops host replay, for `message` and for every other
  durable type alike. Non-`message` frames also carry `outboxId` (same row id) for
  compatibility; the agent does not acknowledge `outboxId`.
- Receivers MUST be idempotent (dedupe by id; see §6) and MUST ignore unknown `type`s and
  unknown fields.

## 5. Frame catalog

`data` casing: business fields are snake_case (`engine_session_id`, `duration_ms`,
`final_seq`, `round_count`, `schedule_type`, …) except `heartbeat.activeThreads`. Envelope
ids are camelCase (`taskId`, `threadId`, `cronId`, `outboxId`, `msgId`). An attachment
descriptor is `{id, name, content_type, size, sha256, ref, direction?}` (§9 of
agent-architecture); bytes never travel in frames.

**agent → host**

| type | envelope ids | data | meaning |
|---|---|---|---|
| `thread_result` | `taskId`, `threadId` | `{status, result?, error?, failure_reason?, engine_session_id?, duration_ms, final_seq, attachments?}` | terminal outcome of one execution (mirror → `thread_runs`) |
| `thread_message` | `taskId`, `threadId` | `{seq, kind, role?, content?, tool?, input?, output?, tool_use_id?, parent_tool_use_id?}` | one ordered transcript event (mirror → `messages`) |
| `cron_state` | `taskId`, `cronId` | `{kind, schedule_type?, schedule_value?, continuity?, next_run?, status?, round_count, skipped_count, last_status?}` | local scheduling-ledger summary (mirror → `crons`) |
| `heartbeat` | — | `{activeThreads?, …}` | liveness (interval from `config.heartbeatIntervalMs`) |
| `fs_response` | `reqId` | `{ok, entries? \| stat? \| sha256 + size, error?}` | answers the `fs_request` with the same `reqId`; live-only |
| `msg_ack` | (top-level `msgId`) | — | durably stored an inbound message |
| `ack` | (top-level `id`) | — | acked a host outbound frame |

**host → agent**

| type | envelope ids | data | meaning |
|---|---|---|---|
| `task_upsert` | `taskId` | `{name?, kind?, status?, engine?, model?, workdir?, meta?, entries?}` | seed/modify a task (the global top entity); `engine`/`model`/`workdir`/`meta` are the task definition, `entries` the task-scope provisioning set (M8) |
| `task_delete` | `taskId` | — | remove a task |
| `cron_upsert` | `taskId`, `cronId` | `{kind, prompt, schedule_type?, schedule_value?, continuity, next_run?, status?}` | seed/modify a trigger under a task; `prompt` (required) is the recurring instruction the cron runs each fire |
| `cron_delete` | `taskId`, `cronId` | — | remove a trigger |
| `message` | `taskId`, `threadId?` | `{text, attachments?}` | user/system input for a task (optionally a live thread) |
| `interrupt` | `taskId?` | — | graceful cancel of the current turn (engine stays alive); with `taskId`, scoped: the agent interrupts only if that task owns the in-flight turn. Delivery is LIVE-ONLY (at-most-once): the host sends it to an online agent and never replays it on reconnect — a stale cancel must not fire against a future turn |
| `close` | — | `{reason?}` (`stop\|restart\|remove`) | runner-level wind-down and exit (agent removal / launcher stop), **not** per-task close — per-task close = `task_upsert{status:"closed"}` + scoped `interrupt` |
| `config_update` | — | `{entries, version, <config fields>}` | live config change (the §3 global overlay; `data` is required — `entries` + `version` drive the version-gated reconcile) |
| `fs_request` | `reqId` | `{op, path, to?, recursive?, ref? + sha256? + size?, upload?}` | one engine-free file operation; live-only (never queued, never replayed) |
| `ack` | (top-level `id`) | — | acked an agent outbound frame |

Every durable host→agent frame additionally carries the top-level delivery id `msgId`
(§4.2); it is omitted from the "envelope ids" column above, which lists the correlation
ids only.

Field semantics:
- `kind` (`thread_message`) is an open string; the recognized set is
  `{user_input, assistant_text, thinking, tool_use, tool_result, status, error}` —
  a receiver stores an unrecognized `kind` opaquely instead of rejecting the
  frame (additive-compatibility, §1). `thinking`/`status` are config-filterable.
- `status` (`thread_result`) is a terminal thread state ∈ `{succeeded, failed, cancelled}`.
- `failure_reason` follows the taxonomy in agent-architecture §6 (`runner.*` / `agent.*`);
  NULL on success. A resume-unsafe failure (e.g. `agent.invalid_request`) omits
  `engine_session_id` so the next turn starts fresh.
- `seq` (`thread_message`) is per-thread monotonic (assigned serially by the runner);
  `final_seq` (`thread_result`) is the last `seq` emitted on the thread (see §6).
- `tool_use_id` (`thread_message`) is the engine's id for a `tool_use` block, echoed on
  the matching `tool_result`, so a receiver can pair a call with its return value.
- `parent_tool_use_id` (`thread_message`) attributes the event to the parent tool call
  that produced it: it is PRESENT on every event produced INSIDE a sub-agent (a `Task`
  tool call) and ABSENT for the main agent's own events — absence therefore means "the
  main agent". It introduces no hierarchy in the task/thread model: `seq` stays a single
  per-thread monotonic stream across all kinds, and attribution is a per-message property
  only.
- `output` on a `tool_result` (`thread_message`) carries `{content, is_error}` —
  `content` is the engine's `tool_result` block content verbatim (any JSON value) and
  `is_error` is the block's boolean flag (`false` when the engine omits it).
- `round_count` / `skipped_count` (`cron_state`) are the runner-local `cron_logs` summary;
  the host derives dispatched counts from its own mirror by `cron_id`.
- `threadId` (`message`, host→agent) selects continuity. The HOST mints conversation
  thread ids (globally unique ULIDs, §6) and records each one as the task's
  `current_thread_id` BEFORE emitting the frame; every `message` a conforming host sends
  carries a `threadId`. On the agent: **present + known** → CONTINUE that thread (resume
  its stored `engine_session_id`); **present + unknown** → CREATE ON FIRST SIGHT — the
  agent creates a fresh thread under the host-supplied id with no carried session. Every
  conversation starts via create-on-first-sight (the first message of a task, an explicit
  new-thread request, and the fresh thread after an orphaned task is re-placed). An
  **absent** `threadId` is tolerated defensively (the field stays optional on the wire):
  the agent derives a deterministic thread id — stable across at-least-once replays — and
  returns it on the resulting `thread_result`/`thread_message` frames; there is no
  implicit per-task continue on an absent `threadId`. Cron-fired thread ids are the one
  agent-minted family the host sees (`<cronId>@<epoch>` / `<cronId>@shared`, §7): they
  arrive only on agent→host frames and are never a task's `current_thread_id`.

The agent is **authoritative for scheduling and execution**; `thread_result` / `cron_state`
are mirror-only writes on the host (read model). The host is a sink for results and a
source for config + work — never on the execution critical path.

### 5.1 Engine-free file operations (`fs_request` / `fs_response`)

The host can operate on an agent's files directly, without prompting an engine.

- `op` ∈ `list`, `stat`, `mkdir`, `remove`, `rename`, and the two transfers `put`, `get`.
- **Paths are not confined to a root.** A relative `path` (and `to`) resolves against the
  agent's workspace directory; an absolute one is used as given. The agent's container and
  uid are the only boundary.
- **File bytes never ride in a frame** — they move over the attachment byte channel (§2),
  whole file at a time:
  - `put {path, ref, sha256, size}` — the host has staged the bytes; the agent downloads
    `ref` (a `/agent/attachments/<id>` path) into a temporary file beside `path`, verifies
    the sha256, and renames it into place, so the destination is replaced atomically or not
    at all. The parent directory must exist. An existing destination keeps its permission
    bits, and a symlink is written through to its target.
  - `get {path, upload}` — the agent uploads the file with `POST /agent/attachments` under
    the name `upload` and answers `{ok, sha256, size}`; the host reads the bytes back by
    that name and digest. `path` must be a regular file.
  - The staged bytes are single-use: `ref` is served only to the agent it was minted for
    and only while the request is pending, and the host deletes them when the transfer
    settles. Only the host mints `ref` and `upload`; a caller cannot supply them.
  - A file larger than the agent's single-attachment limit is refused with `too_large`
    before any bytes move; the host additionally caps what it accepts for a `put`.
- `recursive` lets `mkdir` create parents and `remove` delete a non-empty directory.
  `remove` on a symlink removes the link, never its target.
- `list` returns `entries: [{name, type, size, mtime_ms?}]` sorted by name; `stat` returns
  `stat: {type, size, mtime_ms?}`. `type` ∈ `file`, `dir`, `symlink`, `other`.
- A failure is `{ok: false, error: {code, message}}` with `code` ∈ `disabled`, `not_found`,
  `exists`, `too_large`, `invalid`, `denied`, `io`.
- **Enablement is host-driven.** The agent answers `disabled` until its host-assigned
  config carries `remoteOps.fs = true` (§3). There is no agent-side switch.
- **Live-only.** The request carries no `msgId`/`outboxId`; neither side persists either
  frame. The host refuses a request for an offline agent at once, and fails every pending
  request when the agent's socket drops. A response that arrives for an unknown `reqId` is
  ignored. A peer that does not implement the frames ignores them (§4.2), so the host times
  the request out.

### 5.2 Interactive terminals (live-only)

Terminals share the agent socket with file and work frames. Every terminal frame carries a
host-minted `ptyId` at the envelope top level and has no delivery ID or acknowledgment.

| Direction | Type | Data |
|---|---|---|
| host → agent | `pty_open` | `{argv?, cwd?, env?, cols?, rows?}` |
| host → agent | `pty_input` | `{data}` (base64 bytes) |
| host → agent | `pty_resize` | `{cols, rows}` |
| host → agent | `pty_close` | none |
| agent → host | `pty_ready` | `{}` |
| agent → host | `pty_output` | `{data}` (base64 bytes) |
| agent → host | `pty_exit` | `{exit_code?, signal?, error?: {code, message}}` |

- `remoteOps.pty` defaults to false. Host configuration enables it independently of `fs`;
  disabling it ends existing terminals, including across a rapid disable/re-enable.
- Default argv is `[/bin/sh]`; default size is 80 columns by 24 rows. Dimensions must be
  integers in 1–1000. Relative cwd resolves against the current workspace; absolute cwd is
  honored. Existing sessions keep their original cwd. The terminal inherits only normal
  user/path/locale variables, plus `TERM=xterm-256color` and explicitly supplied `env`.
- Open options are capped at 64 KiB, with at most 64 argv items and environment entries.
  Input/output chunks contain at most 16 KiB of decoded bytes. Each agent permits eight
  sessions; input queues hold 64 chunks, the connection output queue holds 64 frames, and
  the host's unread output buffer is capped at 256 KiB per terminal. Slow consumers close.
  There is no flow control: input that overflows the queue ends the terminal with
  `input_overflow`, and a malformed input or resize frame ends it with `invalid`.
- `pty_ready` confirms successful spawn before output or input. The host defaults to a
  30-second open timeout, including when an older agent ignores the new frame types.
- Input and resize commands are processed in arrival order. Natural completion drains final
  output before `pty_exit`; a descendant that still holds the terminal does not delay it.
  Close terminates the shell and its foreground/process group and reaps the direct child.
  Independently detached processes are outside terminal ownership.
- A terminal belongs to one connection: agent disconnect, replacement, host shutdown, client
  cancellation or disabling the group ends it. Reconnect never restores it or replays bytes.
  Unknown terminal IDs and another agent's responses are ignored. Protocol version stays 2.

The Host SDK exposes `host.agents.openPty(agentId, options?, signal?)`. It resolves after
`pty_ready` to `{id, write(Uint8Array), resize(cols, rows), close(), output, exited}`. `output`
is a byte `ReadableStream`; `exited` resolves to the exit payload. The optional AbortSignal
also cancels a pending open. Canceling the output stream closes the terminal.

The optional Hono admin bridge (enabled by injecting `upgradeWebSocket`) uses:

1. `POST /admin/agents/:id/pty-ticket`, with normal admin authorization, returns
   `{success: true, data: {ticket, expiresAt}}`. Tickets expire in 30 seconds, are single-use,
   are scoped to the agent, and cannot authorize any other admin route.
2. `GET /admin/agents/:id/pty?ticket=...` upgrades the browser connection. Non-browser clients
   may use the normal admin authorization instead. A supplied Origin must match the Origin
   from the authenticated ticket request (or the request URL origin when absent). This works
   through TLS termination without trusting forwarded headers. Direct admin upgrades require
   the request URL origin. The standalone app limits tickets and upgrades to 30 requests per
   minute per IP.
3. Within five seconds, send `{type: "open", data: {...options}}`. Wait for
   `{type: "ready", data: {ptyId}}`, then send `input` / `resize` / `close` using the same
   data shapes above. The server emits `output` and `exit`. One socket owns one terminal;
   messages do not take a client-selected ptyId. The browser's pending send buffer is bounded.

Standalone paths are prefixed with `/api` (and the configured base path, when present).
Ticket responses are not cacheable; request logs omit query strings.

## 6. Multiplexing & ordering (one WS, N parallel threads)

Many threads share one connection, so their frames interleave. The receiver MUST NOT assume
arrival order or `outboxId` order across threads:

- **Demux by `threadId`.** Each thread is an independent stream.
- **Order each thread by `seq`.** `thread_message.seq` is per-thread monotonic; the
  transcript for a thread is `ORDER BY seq`, not by arrival or `outboxId`.
- **Idempotent write on `(threadId, seq)`.** At-least-once replay can duplicate a frame;
  the host de-dupes on `(threadId, seq)` (and `(taskId, threadId, seq)` as the composite
  key — `threadId` is globally unique, so it determines `taskId`).
- **Completeness via `final_seq`.** `thread_result.final_seq` is the last `seq` the thread
  emitted, letting the host verify it received `1..=final_seq`.
- `outboxId` is delivery/ack only; `seq` is the per-thread continuity contract.

Parse model: a whole job is `WHERE task_id = ?`; one conversation is
`WHERE task_id = ? AND thread_id = ? ORDER BY seq`; threads within a task are ordered by
`thread.created_at` (seq is per-thread, not per-task).

## 7. What is NOT in the protocol

- How the agent runs an engine (Claude `-p` control protocol, etc.) — agent-private.
- How/where a runner is started and out-of-band managed — host-private (launchers).
- Scheduling internals — the runner's `cron_logs` ledger, deterministic `thread_id`,
  dedup, retry, and watchdog are runner-local; only the `cron_state` summary is shared.
- The `task_id ↔ delivery channel` mapping — host-private routing.
- Storage schemas on either side — private; only the envelopes above are shared.
- Running a command or opening a terminal on the agent without an engine — not part of
  the protocol yet; only the file operations of §5.1 are.
- Attachment **bytes** — only descriptors ride frames; bytes move on the separate channel
  (§2 / agent-architecture §9).

## 8. Frame JSON Schema (generated)

<!-- GENERATED by packages/host-core/scripts/gen-frames-schema.ts from packages/host-core/src/protocol/frames.ts — DO NOT EDIT.
     Regenerate with `bun run gen:frames-schema`. -->

The machine-checkable JSON Schema for the v2 frame catalog (§5), exported from the single
zod source of truth in [`packages/host-core/src/protocol/frames.ts`](../packages/host-core/src/protocol/frames.ts). `AnyFrame` is
the union of the agent→host (`AgentFrame`) and host→agent (`HostFrame`) catalogs; both are
also exposed under `$defs`. Frames and their `data` objects are **open** — additive fields
validate (protocol §1/§4.2) — while an unknown `type` matches no member and is rejected.

```json
{
  "title": "Gino host ⇄ agent protocol v2 — frame catalog",
  "description": "Generated from packages/host-core/src/protocol/frames.ts (zod). `AnyFrame` = a valid v2 frame in either direction. Frames and `data` objects are open (additive-compatible): unknown fields are accepted, unknown `type`s are rejected. See protocol.md §4–§6.",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "anyOf": [
    {
      "oneOf": [
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "thread_result"
            },
            "taskId": {
              "type": "string"
            },
            "threadId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "status": {
                  "type": "string",
                  "enum": [
                    "succeeded",
                    "failed",
                    "cancelled"
                  ]
                },
                "result": {
                  "type": "string"
                },
                "error": {
                  "type": "string"
                },
                "failure_reason": {
                  "type": "string"
                },
                "engine_session_id": {
                  "type": "string"
                },
                "duration_ms": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "final_seq": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "attachments": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "string"
                      },
                      "name": {
                        "type": "string"
                      },
                      "content_type": {
                        "type": "string"
                      },
                      "size": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "sha256": {
                        "type": "string"
                      },
                      "ref": {
                        "type": "string"
                      },
                      "direction": {
                        "type": "string",
                        "enum": [
                          "inbound",
                          "outbound"
                        ]
                      }
                    },
                    "required": [
                      "id",
                      "name",
                      "content_type",
                      "size",
                      "sha256",
                      "ref"
                    ],
                    "additionalProperties": {}
                  }
                }
              },
              "required": [
                "status",
                "duration_ms",
                "final_seq"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "threadId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "thread_message"
            },
            "taskId": {
              "type": "string"
            },
            "threadId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "seq": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "kind": {
                  "type": "string"
                },
                "role": {
                  "type": "string"
                },
                "content": {
                  "type": "string"
                },
                "tool": {
                  "type": "string"
                },
                "input": {},
                "output": {},
                "tool_use_id": {
                  "type": "string"
                },
                "parent_tool_use_id": {
                  "type": "string"
                }
              },
              "required": [
                "seq",
                "kind"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "threadId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "cron_state"
            },
            "taskId": {
              "type": "string"
            },
            "cronId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "kind": {
                  "type": "string"
                },
                "schedule_type": {
                  "type": "string"
                },
                "schedule_value": {
                  "type": "string"
                },
                "continuity": {
                  "type": "string"
                },
                "next_run": {
                  "type": "string"
                },
                "status": {
                  "type": "string"
                },
                "round_count": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "skipped_count": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "last_status": {
                  "type": "string"
                }
              },
              "required": [
                "kind",
                "round_count",
                "skipped_count"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "cronId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "heartbeat"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "activeThreads": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "fs_response"
            },
            "reqId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "ok": {
                  "type": "boolean"
                },
                "entries": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "type": {
                        "type": "string"
                      },
                      "size": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "mtime_ms": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "name": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "type",
                      "size",
                      "name"
                    ],
                    "additionalProperties": {}
                  }
                },
                "stat": {
                  "type": "object",
                  "properties": {
                    "type": {
                      "type": "string"
                    },
                    "size": {
                      "type": "integer",
                      "minimum": -9007199254740991,
                      "maximum": 9007199254740991
                    },
                    "mtime_ms": {
                      "type": "integer",
                      "minimum": -9007199254740991,
                      "maximum": 9007199254740991
                    }
                  },
                  "required": [
                    "type",
                    "size"
                  ],
                  "additionalProperties": {}
                },
                "sha256": {
                  "type": "string"
                },
                "size": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "error": {
                  "type": "object",
                  "properties": {
                    "code": {
                      "type": "string"
                    },
                    "message": {
                      "type": "string"
                    }
                  },
                  "required": [
                    "code",
                    "message"
                  ],
                  "additionalProperties": {}
                }
              },
              "required": [
                "ok"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "reqId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_ready"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {},
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_output"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "data": {
                  "type": "string",
                  "maxLength": 21848,
                  "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"
                }
              },
              "required": [
                "data"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_exit"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "exit_code": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "signal": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "error": {
                  "type": "object",
                  "properties": {
                    "code": {
                      "type": "string"
                    },
                    "message": {
                      "type": "string"
                    }
                  },
                  "required": [
                    "code",
                    "message"
                  ],
                  "additionalProperties": {}
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "msg_ack"
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "msgId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "ack"
            },
            "id": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "id"
          ],
          "additionalProperties": {}
        }
      ]
    },
    {
      "oneOf": [
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "task_upsert"
            },
            "taskId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "name": {
                  "type": "string"
                },
                "kind": {
                  "type": "string"
                },
                "status": {
                  "type": "string"
                },
                "engine": {
                  "type": "string"
                },
                "model": {
                  "type": "string"
                },
                "workdir": {
                  "type": "string"
                },
                "meta": {
                  "type": "object",
                  "properties": {},
                  "additionalProperties": {}
                },
                "entries": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "name": {
                        "type": "string"
                      },
                      "sha256": {
                        "type": "string"
                      },
                      "target": {
                        "type": "string"
                      },
                      "kind": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "name",
                      "sha256",
                      "target",
                      "kind"
                    ],
                    "additionalProperties": {}
                  }
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "task_delete"
            },
            "taskId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "taskId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "cron_upsert"
            },
            "taskId": {
              "type": "string"
            },
            "cronId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "kind": {
                  "type": "string"
                },
                "prompt": {
                  "type": "string"
                },
                "schedule_type": {
                  "type": "string"
                },
                "schedule_value": {
                  "type": "string"
                },
                "continuity": {
                  "type": "string"
                },
                "next_run": {
                  "type": "string"
                },
                "status": {
                  "type": "string"
                }
              },
              "required": [
                "kind",
                "prompt",
                "continuity"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "cronId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "cron_delete"
            },
            "taskId": {
              "type": "string"
            },
            "cronId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "taskId",
            "cronId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "message"
            },
            "taskId": {
              "type": "string"
            },
            "threadId": {
              "type": "string"
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "text": {
                  "type": "string"
                },
                "attachments": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "string"
                      },
                      "name": {
                        "type": "string"
                      },
                      "content_type": {
                        "type": "string"
                      },
                      "size": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "sha256": {
                        "type": "string"
                      },
                      "ref": {
                        "type": "string"
                      },
                      "direction": {
                        "type": "string",
                        "enum": [
                          "inbound",
                          "outbound"
                        ]
                      }
                    },
                    "required": [
                      "id",
                      "name",
                      "content_type",
                      "size",
                      "sha256",
                      "ref"
                    ],
                    "additionalProperties": {}
                  }
                }
              },
              "required": [
                "text"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "interrupt"
            },
            "taskId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "close"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "config_update"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {},
              "additionalProperties": {}
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "fs_request"
            },
            "reqId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "op": {
                  "type": "string",
                  "enum": [
                    "list",
                    "stat",
                    "mkdir",
                    "remove",
                    "rename",
                    "put",
                    "get"
                  ]
                },
                "path": {
                  "type": "string",
                  "minLength": 1
                },
                "to": {
                  "type": "string",
                  "minLength": 1
                },
                "recursive": {
                  "type": "boolean"
                },
                "ref": {
                  "type": "string"
                },
                "sha256": {
                  "type": "string"
                },
                "size": {
                  "type": "integer",
                  "minimum": 0,
                  "maximum": 9007199254740991
                },
                "upload": {
                  "type": "string"
                }
              },
              "required": [
                "op",
                "path"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "reqId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_open"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "argv": {
                  "minItems": 1,
                  "maxItems": 64,
                  "type": "array",
                  "items": {
                    "type": "string",
                    "maxLength": 4096
                  }
                },
                "cwd": {
                  "type": "string",
                  "maxLength": 4096
                },
                "env": {
                  "type": "object",
                  "propertyNames": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 256,
                    "pattern": "^[^=\\0]+$"
                  },
                  "additionalProperties": {
                    "type": "string",
                    "maxLength": 4096
                  }
                },
                "cols": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                },
                "rows": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_input"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "data": {
                  "type": "string",
                  "maxLength": 21848,
                  "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"
                }
              },
              "required": [
                "data"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_resize"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "cols": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                },
                "rows": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                }
              },
              "required": [
                "cols",
                "rows"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_close"
            },
            "ptyId": {
              "type": "string"
            }
          },
          "required": [
            "type",
            "ptyId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "ack"
            },
            "id": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "id"
          ],
          "additionalProperties": {}
        }
      ]
    }
  ],
  "$defs": {
    "AgentFrame": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "oneOf": [
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "thread_result"
            },
            "taskId": {
              "type": "string"
            },
            "threadId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "status": {
                  "type": "string",
                  "enum": [
                    "succeeded",
                    "failed",
                    "cancelled"
                  ]
                },
                "result": {
                  "type": "string"
                },
                "error": {
                  "type": "string"
                },
                "failure_reason": {
                  "type": "string"
                },
                "engine_session_id": {
                  "type": "string"
                },
                "duration_ms": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "final_seq": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "attachments": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "string"
                      },
                      "name": {
                        "type": "string"
                      },
                      "content_type": {
                        "type": "string"
                      },
                      "size": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "sha256": {
                        "type": "string"
                      },
                      "ref": {
                        "type": "string"
                      },
                      "direction": {
                        "type": "string",
                        "enum": [
                          "inbound",
                          "outbound"
                        ]
                      }
                    },
                    "required": [
                      "id",
                      "name",
                      "content_type",
                      "size",
                      "sha256",
                      "ref"
                    ],
                    "additionalProperties": {}
                  }
                }
              },
              "required": [
                "status",
                "duration_ms",
                "final_seq"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "threadId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "thread_message"
            },
            "taskId": {
              "type": "string"
            },
            "threadId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "seq": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "kind": {
                  "type": "string"
                },
                "role": {
                  "type": "string"
                },
                "content": {
                  "type": "string"
                },
                "tool": {
                  "type": "string"
                },
                "input": {},
                "output": {},
                "tool_use_id": {
                  "type": "string"
                },
                "parent_tool_use_id": {
                  "type": "string"
                }
              },
              "required": [
                "seq",
                "kind"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "threadId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "cron_state"
            },
            "taskId": {
              "type": "string"
            },
            "cronId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "kind": {
                  "type": "string"
                },
                "schedule_type": {
                  "type": "string"
                },
                "schedule_value": {
                  "type": "string"
                },
                "continuity": {
                  "type": "string"
                },
                "next_run": {
                  "type": "string"
                },
                "status": {
                  "type": "string"
                },
                "round_count": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "skipped_count": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "last_status": {
                  "type": "string"
                }
              },
              "required": [
                "kind",
                "round_count",
                "skipped_count"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "cronId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "heartbeat"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "activeThreads": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "fs_response"
            },
            "reqId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "ok": {
                  "type": "boolean"
                },
                "entries": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "type": {
                        "type": "string"
                      },
                      "size": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "mtime_ms": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "name": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "type",
                      "size",
                      "name"
                    ],
                    "additionalProperties": {}
                  }
                },
                "stat": {
                  "type": "object",
                  "properties": {
                    "type": {
                      "type": "string"
                    },
                    "size": {
                      "type": "integer",
                      "minimum": -9007199254740991,
                      "maximum": 9007199254740991
                    },
                    "mtime_ms": {
                      "type": "integer",
                      "minimum": -9007199254740991,
                      "maximum": 9007199254740991
                    }
                  },
                  "required": [
                    "type",
                    "size"
                  ],
                  "additionalProperties": {}
                },
                "sha256": {
                  "type": "string"
                },
                "size": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "error": {
                  "type": "object",
                  "properties": {
                    "code": {
                      "type": "string"
                    },
                    "message": {
                      "type": "string"
                    }
                  },
                  "required": [
                    "code",
                    "message"
                  ],
                  "additionalProperties": {}
                }
              },
              "required": [
                "ok"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "reqId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_ready"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {},
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_output"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "data": {
                  "type": "string",
                  "maxLength": 21848,
                  "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"
                }
              },
              "required": [
                "data"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_exit"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "exit_code": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "signal": {
                  "type": "integer",
                  "minimum": -9007199254740991,
                  "maximum": 9007199254740991
                },
                "error": {
                  "type": "object",
                  "properties": {
                    "code": {
                      "type": "string"
                    },
                    "message": {
                      "type": "string"
                    }
                  },
                  "required": [
                    "code",
                    "message"
                  ],
                  "additionalProperties": {}
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "msg_ack"
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "msgId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "ack"
            },
            "id": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "id"
          ],
          "additionalProperties": {}
        }
      ]
    },
    "HostFrame": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "oneOf": [
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "task_upsert"
            },
            "taskId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "name": {
                  "type": "string"
                },
                "kind": {
                  "type": "string"
                },
                "status": {
                  "type": "string"
                },
                "engine": {
                  "type": "string"
                },
                "model": {
                  "type": "string"
                },
                "workdir": {
                  "type": "string"
                },
                "meta": {
                  "type": "object",
                  "properties": {},
                  "additionalProperties": {}
                },
                "entries": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "name": {
                        "type": "string"
                      },
                      "sha256": {
                        "type": "string"
                      },
                      "target": {
                        "type": "string"
                      },
                      "kind": {
                        "type": "string"
                      }
                    },
                    "required": [
                      "name",
                      "sha256",
                      "target",
                      "kind"
                    ],
                    "additionalProperties": {}
                  }
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "task_delete"
            },
            "taskId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "taskId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "cron_upsert"
            },
            "taskId": {
              "type": "string"
            },
            "cronId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "kind": {
                  "type": "string"
                },
                "prompt": {
                  "type": "string"
                },
                "schedule_type": {
                  "type": "string"
                },
                "schedule_value": {
                  "type": "string"
                },
                "continuity": {
                  "type": "string"
                },
                "next_run": {
                  "type": "string"
                },
                "status": {
                  "type": "string"
                }
              },
              "required": [
                "kind",
                "prompt",
                "continuity"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "cronId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "cron_delete"
            },
            "taskId": {
              "type": "string"
            },
            "cronId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "taskId",
            "cronId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "message"
            },
            "taskId": {
              "type": "string"
            },
            "threadId": {
              "type": "string"
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {
                "text": {
                  "type": "string"
                },
                "attachments": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "string"
                      },
                      "name": {
                        "type": "string"
                      },
                      "content_type": {
                        "type": "string"
                      },
                      "size": {
                        "type": "integer",
                        "minimum": -9007199254740991,
                        "maximum": 9007199254740991
                      },
                      "sha256": {
                        "type": "string"
                      },
                      "ref": {
                        "type": "string"
                      },
                      "direction": {
                        "type": "string",
                        "enum": [
                          "inbound",
                          "outbound"
                        ]
                      }
                    },
                    "required": [
                      "id",
                      "name",
                      "content_type",
                      "size",
                      "sha256",
                      "ref"
                    ],
                    "additionalProperties": {}
                  }
                }
              },
              "required": [
                "text"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "taskId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "interrupt"
            },
            "taskId": {
              "type": "string"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "close"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "config_update"
            },
            "outboxId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "msgId": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            },
            "data": {
              "type": "object",
              "properties": {},
              "additionalProperties": {}
            }
          },
          "required": [
            "type"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "fs_request"
            },
            "reqId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "op": {
                  "type": "string",
                  "enum": [
                    "list",
                    "stat",
                    "mkdir",
                    "remove",
                    "rename",
                    "put",
                    "get"
                  ]
                },
                "path": {
                  "type": "string",
                  "minLength": 1
                },
                "to": {
                  "type": "string",
                  "minLength": 1
                },
                "recursive": {
                  "type": "boolean"
                },
                "ref": {
                  "type": "string"
                },
                "sha256": {
                  "type": "string"
                },
                "size": {
                  "type": "integer",
                  "minimum": 0,
                  "maximum": 9007199254740991
                },
                "upload": {
                  "type": "string"
                }
              },
              "required": [
                "op",
                "path"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "reqId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_open"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "argv": {
                  "minItems": 1,
                  "maxItems": 64,
                  "type": "array",
                  "items": {
                    "type": "string",
                    "maxLength": 4096
                  }
                },
                "cwd": {
                  "type": "string",
                  "maxLength": 4096
                },
                "env": {
                  "type": "object",
                  "propertyNames": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 256,
                    "pattern": "^[^=\\0]+$"
                  },
                  "additionalProperties": {
                    "type": "string",
                    "maxLength": 4096
                  }
                },
                "cols": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                },
                "rows": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                }
              },
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_input"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "data": {
                  "type": "string",
                  "maxLength": 21848,
                  "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"
                }
              },
              "required": [
                "data"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_resize"
            },
            "ptyId": {
              "type": "string"
            },
            "data": {
              "type": "object",
              "properties": {
                "cols": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                },
                "rows": {
                  "type": "integer",
                  "minimum": 1,
                  "maximum": 1000
                }
              },
              "required": [
                "cols",
                "rows"
              ],
              "additionalProperties": {}
            }
          },
          "required": [
            "type",
            "ptyId",
            "data"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "pty_close"
            },
            "ptyId": {
              "type": "string"
            }
          },
          "required": [
            "type",
            "ptyId"
          ],
          "additionalProperties": {}
        },
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "const": "ack"
            },
            "id": {
              "type": "integer",
              "minimum": -9007199254740991,
              "maximum": 9007199254740991
            }
          },
          "required": [
            "type",
            "id"
          ],
          "additionalProperties": {}
        }
      ]
    }
  }
}
```
