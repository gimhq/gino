# Architecture

Gino is a control plane (the **host**) and a data plane (the **agent**) joined by one protocol.
The two share no code; each can be versioned and deployed on its own.

```
 channels · admin · users
          │  HTTP
          ▼
 ┌──────────────────┐   WS /agent/ws/:agentId    ┌───────────────────┐
 │ host (TypeScript)│ ◀────────────────────────▶ │ agent (Rust)      │
 │ source of work,  │   HTTP /agent/{register,   │ schedules, runs,  │
 │ sink for results │   config,output,attachments}│ retries; offline- │
 └──────────────────┘                            │ tolerant          │
          │ /llm proxy                           └─────────┬─────────┘
          ▼                                                │ stdio
    LLM providers                                  claude / codex CLI
```

## Where to read

| document | covers |
|---|---|
| [`protocol.md`](protocol.md) | the wire contract: enrollment, framing, delivery guarantees, the frame catalog and its JSON Schema |
| [`host-architecture.md`](host-architecture.md) | host modules M1–M9, the workspace layout, tokens, storage |
| [`agent-architecture.md`](agent-architecture.md) | agent entities (task, cron, thread), the SQLite model, lifecycle, attachments, provisioning |
| [`host-sdk-design.md`](host-sdk-design.md) | why the host is split into embeddable packages, the storage and transport ports, open items |
| [`host-upgrade-design.md`](host-upgrade-design.md) | signed releases, hot update and rollback of the host through the lode loader |
| [`changelog.md`](changelog.md) | dated work log |
| [`audit/`](audit/) | point-in-time code audits (2026-07, 2026-08); findings are not annotated with their current status |
| [`multica/`](multica/) | notes on the Multica server, the system gino's design was compared against (in Chinese) |
| [`../e2e/README.md`](../e2e/README.md) | the cross-implementation docker test |

Per-package usage is in each package's `README.md`.

## Vocabulary

- **task** — the top-level unit of work; every cron, thread, message and attachment belongs to one.
- **cron** — a trigger under a task (schedule, webhook or API). `isolated` crons start a new thread
  per fire; `shared` crons continue one thread.
- **thread** — one engine conversation or execution under a task.
- **engine** — the CLI the agent drives (`claude`, `codex`).
- **M1–M9** — the host's module numbers, defined in `host-architecture.md` §1 and used in code
  comments.

## Invariants

1. Frames are routed by their envelope (`type`, `taskId`, `threadId`, `cronId`, ids), never by payload.
2. Delivery is at-least-once in both directions: persist before send, replay on reconnect, dedupe by
   id and by `(threadId, seq)`.
3. Only the protocol adapter on each side knows the wire format.
4. Large bytes travel on the attachment channel, not in frames.
5. The host never schedules or executes; the agent never depends on the host to keep running.
