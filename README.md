# Gino

Gino runs coding agents (Claude Code, Codex) as managed, long-lived workers. It has two
independent halves that share no code and talk over one versioned protocol:

- **Host** — the control plane (TypeScript, Bun). It owns task identity, cron definitions, agent
  enrollment, the durable transcript, config and skill provisioning, and the LLM credential proxy.
- **Agent** — the data plane (`gino-agent`, Rust). It owns scheduling, execution, concurrency and
  retry, and keeps working while the host is unreachable.

The host is a source of work and a sink for results; it is never on the execution path.

## Layout

| path | what it is |
|---|---|
| `packages/host-core` | the control plane as a library: protocol, agents, tasks, message store, config store |
| `packages/host-store-sqlite` | SQLite storage adapter (`bun:sqlite` or `better-sqlite3`) |
| `packages/host-hono` | Hono routers for `/agent`, `/admin`, `/messages` |
| `packages/host-serve` | the `/agent` data plane with no HTTP framework (`Bun.serve`, `node:http`) |
| `packages/host-channels` | outbound webhook delivery and inbound message routing |
| `packages/host-proxy` | LLM credential proxy mounted at `/llm` |
| `apps/host` | the standalone host server composed from the packages |
| `gino-agent` | the agent daemon |
| `examples/embed-node` | the host SDK embedded in a plain Node process |
| `e2e` | docker-compose test that runs the real host against the real agent |
| `skills/gino` | the skill an engine uses to drive the agent's own CLI |
| `docs` | architecture, protocol, design documents, audits |

## Quick start

Host (Bun 1.4):

```bash
bun install
cp .env.example .env    # set the register, channel and admin tokens
bun run dev             # http://localhost:3000/api, API reference at /api/docs
```

Agent (Rust 1.96):

```bash
cd gino-agent
GINO_HOST_URL=http://localhost:3000/api \
GINO_REGISTER_TOKEN=<the host's GINO_REGISTER_TOKEN> \
cargo run -- run
```

## Quality gates

```bash
bun run check                                   # host: lint, typecheck, test, build
(cd gino-agent && cargo fmt --all --check && cargo clippy --locked --all-targets && cargo test --locked)
./e2e/run.sh                                    # host and agent together, in docker
```

## Documentation

Start at [`docs/architecture.md`](docs/architecture.md). The wire contract is
[`docs/protocol.md`](docs/protocol.md).
