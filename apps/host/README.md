# @gino/host

The standalone gino host: the `@gino/host-*` packages composed into one Bun + Hono server. This
is the only place in the workspace with Bun-specific APIs, environment configuration, rate limits,
bearer policy, Prometheus metrics, the API reference UI and the lode upgrade integration.

Design: [`../../docs/host-architecture.md`](../../docs/host-architecture.md).

## Run

```bash
bun install                 # at the repository root
cp .env.example .env        # at the repository root; set the three tokens
bun run dev                 # from the root, or `bun --hot src/index.ts` here
```

The server listens on `GINO_PORT` (default 3000) and serves under `/api` unless `GINO_BASE_PATH`
is set.

| prefix | purpose | credential |
|---|---|---|
| `/agent/*` | agent data plane: register, WebSocket, config, output, attachments | register token, then the per-agent connect token |
| `/admin/*` | agents, tasks, crons, config | admin token |
| `/messages/*` | channel message I/O and webhooks | channel token |
| `/llm/*` | LLM credential proxy for the engines | per-agent connect token, or the static proxy key |
| `/health`, `/health/ready`, `/system/version` | probes | none |
| `/system/lode/*`, `/metrics` | upgrade control, Prometheus | admin token |
| `/docs`, `/openapi.json` | API reference | none |

The channel and admin tokens are separate secrets. An unset token rejects every request on its
surface; there is no fallback from one to the other.

## Configuration

Every key is `GINO_`-prefixed and validated in `src/config/schema.ts`. The annotated list is the
root [`.env.example`](../../.env.example).

## CLI

```bash
bun src/cli.ts healthcheck        # in-process probe against /api/health
bun src/cli.ts migrate --check    # list pending migrations without applying them
```

## Scripts

| script | does |
|---|---|
| `bun run check` | lint, typecheck, test, build |
| `bun run build` | bundle to `dist/index.js` |
| `bun run package` | build the lode release asset and its manifest |

Tests resolve the default store directory against the working directory, so run them from this
directory (`bun run test`) or through the root `bun run check`.
