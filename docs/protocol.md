# Host ⇄ Agent Protocol (the contract)

This document is the **single coupling point** between the two otherwise-independent
architectures:

- **Host** (control plane) — `src/**`, TypeScript. Orchestration, launchers, storage, UI.
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
  `POST /agent/output/:agentId`, and (host-proxy backend) `GET /agent/attachments/:id` +
  `POST /agent/attachments`.
- The agent always **dials out** to the host (NAT/firewall friendly). The host never
  connects into the agent; out-of-band control is the launcher's job (see
  [`docs/host-architecture.md`](host-architecture.md)), not part of this protocol.

## 3. Enrollment — `POST /agent/register`

Unauthenticated by connect token; carries the bootstrap **register token** instead.

Request:
```json
{ "registerToken": "<secret>", "protocolVersion": 2,
  "name": "<human label?>", "capabilities": ["claude", ...], "source": "docker|vm|bare" }
```
Response (success):
```json
{ "agentId": "<host-generated>", "connectToken": "<per-agent secret>",
  "config": { "agentId": "...", "wsUrl": "/agent/ws/<agentId>",
              "configUrl": "/agent/config/<agentId>", "outputUrl": "/agent/output/<agentId>",
              "heartbeatIntervalMs": 30000, "protocolVersion": 2 } }
```
Response (failure): `{ "register_error": { "reason": "invalid_register_token | rate_limited | invalid_body | registration_disabled | unsupported_protocol" } }`.

Rules:
- The host generates `agentId`. The agent does not choose it.
- The **connect token** authenticates the WS + HTTP config/output/attachments (header
  `x-internal-token`). It is **never** the register token; the agent persists it locally
  (`connect.token`) and reuses it on reconnect — no re-register needed.
- The host is the authority for `config` (credentials/engine settings arrive via the
  config layer; see [`docs/host-architecture.md`](host-architecture.md)).

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
  `taskId` / `threadId` / `cronId`.
- **`data`**: business fields only — it **never** repeats the correlation ids. In
  particular `engine_session_id` is a *value* of a thread (the engine `--resume` handle),
  so it lives **inside `data`** on `thread_result` (stored, not routed-by).
- The host routes and correlates by the **envelope** ids *without parsing `data`*,
  mirroring the runner's `inbox`/`outbox` correlation columns and the host read-model keys.
- Which ids are present depends on the frame (see §5). `interrupt` MAY carry `taskId` to
  scope the cancel to one task; `close` and the acks carry no correlation ids; deletes
  carry only their identity ids and no `data`.

### 4.2 Delivery — at-least-once both ways

- Sender persists each outbound frame (outbox) before sending; receiver replies
  `{ "type": "ack", "id": <outboxId> }`; unacked frames replay on reconnect.
- Host→agent message delivery uses `msgId` + `{ "type": "msg_ack", "msgId": <int> }` so
  the host advances its cursor only after the agent has durably stored the message.
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
| `thread_message` | `taskId`, `threadId` | `{seq, kind, role?, content?, tool?, input?, output?}` | one ordered transcript event (mirror → `messages`) |
| `cron_state` | `taskId`, `cronId` | `{kind, schedule_type?, schedule_value?, continuity?, next_run?, status?, round_count, skipped_count, last_status?}` | local scheduling-ledger summary (mirror → `crons`) |
| `heartbeat` | — | `{activeThreads?, …}` | liveness (interval from `config.heartbeatIntervalMs`) |
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
| `close` | — | — | runner-level wind-down and exit (agent removal / launcher stop), **not** per-task close — per-task close = `task_upsert{status:"closed"}` + scoped `interrupt` |
| `config_update` | — | `{<config fields>}` | live config change (opaque overlay) |
| `ack` | (top-level `id`) | — | acked an agent outbound frame |

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
- Attachment **bytes** — only descriptors ride frames; bytes move on the separate channel
  (§2 / agent-architecture §9).

## 8. Frame JSON Schema (generated)

<!-- GENERATED by scripts/gen-frames-schema.ts from src/protocol/frames.ts — DO NOT EDIT.
     Regenerate with `bun run gen:frames-schema`. -->

The machine-checkable JSON Schema for the v2 frame catalog (§5), exported from the single
zod source of truth in [`src/protocol/frames.ts`](../src/protocol/frames.ts). `AnyFrame` is
the union of the agent→host (`AgentFrame`) and host→agent (`HostFrame`) catalogs; both are
also exposed under `$defs`. Frames and their `data` objects are **open** — additive fields
validate (protocol §1/§4.2) — while an unknown `type` matches no member and is rejected.

```json
{
  "title": "Gino host ⇄ agent protocol v2 — frame catalog",
  "description": "Generated from src/protocol/frames.ts (zod). `AnyFrame` = a valid v2 frame in either direction. Frames and `data` objects are open (additive-compatible): unknown fields are accepted, unknown `type`s are rejected. See protocol.md §4–§6.",
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
                "output": {}
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
                "output": {}
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
