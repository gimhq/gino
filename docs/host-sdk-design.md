# Host SDK Design — an embeddable control plane

Status: **proposal** (2026-09). Scope: turn the host (`src/**`, ~13.6k lines TypeScript)
from a standalone Bun server into a library that other systems embed, while keeping the
current standalone server as one thin consumer of that library. Companion docs:
[`host-architecture.md`](host-architecture.md) (what the host is), [`protocol.md`](protocol.md)
(the wire contract — unchanged by this design), [`host-upgrade-design.md`](host-upgrade-design.md)
(lode; becomes app-only).

## 1. Goal and non-goals

**Goal.** A consumer service (Node or Bun, any HTTP framework, its own auth and users) can:

1. instantiate the control plane in-process (`createHost(...)`) against its own SQLite file;
2. expose the agent data plane (`/agent/*` HTTP + WS) through **its** server — the wire is
   the only contract runners depend on, so it must be reproduced byte-for-byte;
3. drive tasks/messages/crons/agents/config through **typed function calls**, not by HTTP-ing
   its own admin API with a bearer token;
4. receive results as **events** (transcript rows, thread results, cron state, agent
   lifecycle) instead of polling a channel cursor;
5. opt out of everything it does not need: LLM credential proxy, channels/webhooks, docker
   launcher, OpenAPI docs, metrics, lode, CLI.

**Non-goals.** Changing the protocol; supporting non-SQLite storage (drizzle stays, the
SQLite *driver* becomes pluggable); making the runner (`gino-agent`) embeddable; keeping
HTTP-level backward compatibility for `/admin/*` inside the SDK (the standalone app keeps
it, the SDK does not promise it).

## 2. Where the code stands today (measured)

| area | lines | SDK verdict |
|---|---|---|
| `protocol/` (M2 codec, inbound, outbox, version, frames) | 1097 | **core** |
| `agents/` (registry, sessions, liveness, control, launchers, launch-reaper) | 1513 | **core** (docker launcher → plugin) |
| `tasks/` (store, dispatch, orchestrator, lifecycle) | 760 | **core** |
| `msgstore/` (transcript, thread_runs, crons, attachments, reads) | 484 | **core** |
| `configstore/` (blob store, artifacts, entries, gc, serve) | 1014 | **core** |
| `db/` (schema, drizzle on `bun:sqlite`, migrations, snapshot/lock) | 590 | **core**, driver abstracted |
| `channels/` (egress cursor tail, ingress routing, webhooks, pump) | 1105 | **optional module** |
| `edge/` (Hono routers: agent, admin, messages, llm) | 2032 | split: agent router → **adapter**; admin/messages → adapter (optional); llm → proxy package |
| `proxy/` (LLM credential proxy, OAuth, Claude→OpenAI bridge) | 1591 | **separate optional package** |
| `shared/` (errors, logger, metrics, ids, middleware, openapi, body-limit, client-ip) | 1394 | split: errors/logger-interface/ids/body-limit → core; middleware/openapi/metrics-render → adapter/app |
| `lode/`, `modules/` (docs, system routes), `routes/`, `cli.ts`, `index.ts`, `dirs.ts`, `config.ts` + `config/` | ~1650 | **app only** |
| `platform/` (barrel) | 86 | fold into core barrel |

Things that already work in the SDK's favor (do not redesign):

- **Domain functions take `db` explicitly** — `enrollAgent(db, …)`, `emitMessage(db, sender, task, input)`,
  `writeThreadMessage(db, …)`. The Hono context is read (`c.get("db"|"logger"|"config")`) only in
  `edge/**`, `shared/middleware/**` and `modules/**`. The domain layer is already a functional core.
- **Transport is behind two small interfaces**: `FrameSender { isOnline, send }` (`protocol/outbox.ts`)
  and `WsLike { send, readyState?, close, raw? }` (`agents/sessions.ts`). Nothing in the domain knows
  Hono or Bun sockets.
- **Inbound already has a hook seam**: `InboundDeps.onTranscriptWrite` — the event surface grows from
  here, it does not have to be invented.
- **`createApp({ config, db, logger })` + `createDb(":memory:")`** is how every integration test embeds
  the host in-process today. The SDK formalizes that seam; it does not create a new one.
- **Docker launcher shell-out is an injected seam** (`Bun.spawn` default, fake in tests).

Things that block embedding today (must change):

| blocker | where | why it matters |
|---|---|---|
| process-global singletons | `agents/sessions.ts` (`export const sessions`), `agents/launchers/index.ts` (launcher registry Map), `shared/lib/metrics.ts` (registry Map), `shared/middleware/logging.ts` (`seenRoutes`) | two hosts in one process (tests, multi-tenant embedders) share state; the standalone server's `app.ts` wires `sessions` by import, not by injection |
| Bun-only runtime bindings | `db/index.ts` (`bun:sqlite`, `drizzle-orm/bun-sqlite`, bun migrator), `edge/agent.ts` + `edge/llm.ts` (`hono/bun` `upgradeWebSocket`), `app.ts` (`hono/bun` `websocket`), `index.ts` (`Bun.serve`), `launchers/docker.ts` + `cli.ts` (`Bun.spawn`), `config.ts`/`lode/config.ts` (`Bun.env`, `Bun.TOML`) | a Node consumer cannot even import the package |
| env-shaped configuration | `config/schema.ts` (41 `GINO_*` keys) threaded as one `Config` object into domain code (`createStore(config.STORE_DIR)`, `config.MAX_UPLOAD_BYTES`, `REGISTER_TOKEN`, `REGISTER_ENABLED`, webhook knobs) | the domain must take a small typed options object; the env schema belongs to the app |
| auth policy baked into routes | `shared/middleware/auth.ts` (admin/channel bearer tokens, `connectTokenAuth`) | embedders have their own principal model; only the **agent** credentials (register token, connect token) are part of the protocol and belong in the SDK |
| hidden cross-module edges | `channels/webhooks.ts → proxy` (`isPrivateUpstreamHost`), `shared/lib/types.ts → channels/pump` (`WebhookPump` on `AppEnv`), `shared/middleware/auth.ts → agents/registry` | optional modules must not be reachable from core imports |
| background loops started ad hoc | `app.ts`/`index.ts` start liveness sweep, outbox GC, configstore GC, launch reaper, webhook pump, lode watcher | the embedder needs one `start()`/`stop()` with a known set of timers, all `unref`'d |
| OpenAPI decoration on every route | `describeRoute(...)` (hono-openapi) in `edge/**` | ties the agent router to `hono-openapi` + `@scalar`; the SDK adapter should mount without docs deps |

## 3. Target shape

One Bun workspace, four packages plus the existing app. Names are placeholders.

```
packages/
  host-core/        @gino/host-core   — the SDK. No HTTP framework, no Bun-only API, no env, no timers
                                        started implicitly. drizzle + sqlite driver injected.
  host-hono/        @gino/host-hono   — Hono routers + WS adapters (bun | node-ws) mounting the
                                        agent data plane, and OPTIONAL admin/messages routers.
  host-channels/    @gino/host-channels — M6 egress/ingress/webhooks/pump as an add-on over core events.
  host-proxy/       @gino/host-proxy  — M7 LLM credential proxy (independent of the agent protocol).
apps/
  host/             @gino/host        — today's standalone server: env config, Bun.serve, CLI, lode,
                                        docs/openapi, metrics endpoint, rate limits, docker launcher.
```

Dependency direction is strict: `apps/host → host-hono, host-channels, host-proxy → host-core`.
`host-core` depends only on `drizzle-orm`, `zod` (frames), `nanoid`/ulid, and `node:*`.

### 3.1 `@gino/host-core` — public surface

```ts
import { createHost } from "@gino/host-core";

const host = createHost({
  storage: { driver: sqliteDriver /* bun | better-sqlite3 | libsql */, storeDir: "./data/store" },
  logger,                                      // Logger interface (pino-compatible subset); default no-op
  agents: {
    registerToken: process.env.REGISTER_TOKEN, // the ONLY secrets the SDK owns: protocol credentials
    registrationEnabled: true,
    heartbeatIntervalMs: 30_000,
    offlineGraceMs: 90_000,
  },
  attachments: { maxUploadBytes: 64 << 20 },
  launchers: [manualLauncher, dockerLauncher({ exec })],  // manual is default; docker is opt-in
  ids: { agentId: ulid, taskId: ulid },        // injectable for deterministic tests
  clock: () => Date.now(),
});

await host.start();      // liveness sweep, outbox GC, configstore GC, launch reaper (all unref'd)
await host.stop();       // close sockets, stop timers, close db
```

Namespaces (each is a thin object over the existing domain functions, bound to the instance):

| namespace | wraps today's | representative calls |
|---|---|---|
| `host.agents` | `agents/registry`, `agents/control`, `agents/launchers` | `enroll`, `list`, `get`, `remove`, `stop`, `restart`, `inspect`, `grant`, `revoke`, `setAccess`, `launch` |
| `host.tasks` | `tasks/store`, `tasks/dispatch`, `tasks/lifecycle`, `tasks/orchestrator` | `create`, `get`, `list`, `assign`, `send(taskId, { text, attachments?, newThread? })`, `interrupt`, `close`, `setEntries` |
| `host.crons` | `tasks/orchestrator` cron emitters + `msgstore/crons` mirror | `upsert`, `delete`, `list(taskId)` |
| `host.transcript` | `msgstore` reads | `get(taskId, threadId, { after?, limit? })`, `threads(taskId)`, `run(threadId)`, `stats` |
| `host.config` | `configstore` | `putArtifact`, `setEntries(scope, …)`, `resolveFor(agent)`, `version()` |
| `host.wire` | `edge/agent.ts` logic minus Hono, `protocol/inbound`, `agents/sessions` | framework-agnostic handlers, see §3.2 |
| `host.events` | new, grown from `InboundDeps.onTranscriptWrite` | typed emitter, see §3.3 |

Rules for the surface: every method takes plain data and returns plain data (the drizzle row
types stay internal); errors are the existing `AppError` subclasses (`NotFoundError`,
`ForbiddenError`, `ValidationError`, `AppError{status,code}`) so an adapter can map them to
HTTP without the SDK knowing HTTP.

### 3.2 `host.wire` — the data plane without a framework

The agent-facing endpoints are the protocol; their *logic* lives in core as request/response
functions over plain types, and the Hono adapter is a 1:1 mount:

```ts
interface Wire {
  register(body: unknown, ctx: { clientIp?: string }): RegisterOutcome;         // {status, body}
  resolveAgent(connectToken: string): { agentId: string } | null;               // connectTokenAuth
  configFor(agentId: string): ConfigPayload;                                    // GET /agent/config/:id
  ingestHttpFrame(agentId: string, raw: string | Uint8Array): AckBody;          // POST /agent/output/:id
  attachSocket(agentId: string, ws: WsLike): void;                              // WS onOpen (supersede + replay)
  onSocketMessage(agentId: string, raw: string | ArrayBuffer): void;            // WS onMessage
  detachSocket(agentId: string, ws: WsLike): void;                              // WS onClose
  attachments: { serveById, serveBySha, ingest }                                // byte channel, returns {status, headers, body}
}
```

Rate limiting, CORS, request ids, logging middleware are **not** in core — the adapter or the
embedder applies them. `WsLike` stays as the socket abstraction; `host-hono` ships two upgrade
implementations (`hono/bun` and `@hono/node-ws`), and a raw `ws`-library adapter is a ~40-line
example, because `attachSocket/onSocketMessage/detachSocket` are the whole contract.

### 3.3 `host.events` — results without channels

```ts
host.events.on("transcript.message", ({ taskId, threadId, seq, row }) => …);   // after durable write
host.events.on("thread.result",      ({ taskId, threadId, run }) => …);
host.events.on("cron.state",         ({ taskId, cronId, cron }) => …);
host.events.on("agent.enrolled" | "agent.online" | "agent.offline" | "agent.removed", …);
host.events.on("task.placed",        ({ taskId, agentId }) => …);
host.events.on("outbox.acked",       ({ agentId, id, type }) => …);              // msg_ack / ack
```

Semantics: events fire **after** the durable write that today already triggers
`onTranscriptWrite`; they are at-least-once hints, never the record — the store remains the
source of truth (`host.transcript.get` re-reads by cursor), exactly the M5/M6 inversion in
`host-architecture.md`. `host-channels` becomes a consumer of these events (its webhook pump
subscribes instead of being handed `webhookPump` through the Hono context).

### 3.4 Storage (minimal option — see §8 for the recommended port-based design)

Keep drizzle and the SQLite schema (15 tables, 6 migrations). Change the *type* and the *driver*:

- `AppDatabase` is currently `BunSQLiteDatabase & {close, checkpoint}`. It becomes drizzle's
  `BaseSQLiteDatabase<"sync", unknown, typeof schema>` plus `{ close, checkpoint }` — the query
  API used across the 22 files that import `drizzle-orm` is identical for `bun-sqlite`,
  `better-sqlite3` and `libsql` sync drivers.
- `createDb(path)` becomes `openStorage({ driver, path, migrationLock?, snapshotOnUpgrade? })`;
  the driver object exposes `open(path) → { drizzle, exec, close }`. Bun and better-sqlite3
  drivers ship with the SDK (each ~30 lines); the caller picks one.
- Migrations run from the existing `embeddedMigrations` map through `db.run(sql.raw(step))`
  in a transaction with a `user_version`-style journal table, replacing the bun-only
  `drizzle-orm/bun-sqlite/migrator`. Behavior (idempotent, ordered) unchanged.
- The pre-migration DB snapshot and cross-process migration lock stay, as opt-in options
  (they are operational features of the standalone app, harmless for embedders).

## 4. Deletions and demotions (from the SDK, not from the repo)

| item | lines | action |
|---|---|---|
| `lode/` + `modules/system` lode routes + `index.ts` lode wiring | ~1000 | **app only** — supervisor integration is deployment, not control plane |
| `cli.ts`, `dirs.ts`, `build-info.ts`, `index.ts` (`Bun.serve`) | ~400 | **app only** |
| `config.ts` + `config/schema.ts` (env → `Config`) | 140 | **app only**; core takes `HostOptions` (§3.1). Every `config.X` read in domain code is replaced by an option field — today that is `STORE_DIR`, `MAX_UPLOAD_BYTES`, `REGISTER_TOKEN`, `REGISTER_ENABLED`, `CONFIG_GC_*`, `WEBHOOK_*`, `HEARTBEAT`; the proxy and rate-limit keys never reach core |
| `modules/docs` (scalar UI, openapi.json) + `shared/lib/openapi.ts` + every `describeRoute()` | ~350 | **adapter-optional**: the Hono adapter exposes plain routers; the app re-decorates them for docs. Removes `hono-openapi` and `@scalar/*` from the SDK dependency set |
| `shared/middleware/{rate-limit, logging, request-id, error-handler}` + `app.ts` secure headers / CORS | ~500 | **app / adapter**: the adapter ships `errorHandler` (AppError → JSON) because the wire's error envelope is part of the protocol; the rest is the embedder's stack |
| `shared/lib/metrics.ts` global registry + `GET /metrics` | 156 | **app**: core emits counters through an injected `Metrics` sink interface (`counterAdd` has three call sites in `protocol/inbound.ts`; the fourth is in the logging middleware, which leaves core anyway); the app plugs its Prometheus renderer in |
| `shared/middleware/auth.ts` admin/channel bearer auth | 93 | **app**: the embedder authorizes its own callers. Core keeps `verifyRegisterToken` / `resolveConnectToken` (protocol credentials) as pure functions |
| `edge/admin/**` | 1016 | **adapter-optional** (`host-hono` `adminRouter(host)`): a convenience for integrators who want the same HTTP control API; the SDK-native path is `host.tasks.*` / `host.agents.*` |
| `edge/messages.ts` + `channels/**` | ~1340 | **`host-channels` package**: poll/webhook delivery is one way to consume events; integrators with their own bus subscribe to `host.events` instead |
| `proxy/**` + `edge/llm.ts` | ~1780 | **`host-proxy` package**: independent of the protocol; only coupling is the connect-token lookup (an injected `resolveConnectToken`) and `channels/webhooks.ts`'s use of `isPrivateUpstreamHost`, which moves to a core `net` util so channels stop importing proxy |
| `agents/launchers/docker.ts` | ~300 | **plugin** shipped in `host-hono`-independent `host-core/launchers/docker` with the exec seam required (no `Bun.spawn` default in core; the app injects `Bun.spawn`, Node injects `child_process`) |

Nothing is deleted from the repository in this design; the standalone app keeps every feature.
Deletion candidates *after* the split (report, decide later): dead `emitClose`/`emitConfigUpdate`
task-scoped variants in `tasks/orchestrator.ts` (noted in the 2026-08 alignment audit).

## 5. Changes inside core (in dependency order)

1. **Instance context instead of singletons.** Introduce `HostContext { db, logger, sessions,
   launchers, metrics, events, options, clock, ids }` created by `createHost`. `sessions` becomes
   `new SessionRegistry()` per host (the class already exists; only the exported singleton goes).
   The launcher registry becomes a per-context map. `seenRoutes`/metrics registry leave core (§4).
   Domain functions keep their `(db, …)` signatures; the namespace objects in §3.1 bind them to the
   context, so the 673 existing tests keep passing with `db` passed explicitly.
2. **Options object.** Replace `Config` reads in `agents/`, `tasks/`, `configstore/`, `msgstore/`,
   `protocol/` with `HostOptions` fields (§3.1). `createStore(config)` → `createStore(storeDir)`.
3. **Storage driver seam** (§3.4). New `db/driver.ts` interface + two drivers; `AppDatabase` type
   generalized; migrator replaced. Verified by running the whole unit+integration suite under both
   drivers (bun test can exercise better-sqlite3 too).
4. **Wire handlers extracted from `edge/agent.ts`** into `core/wire/*` returning `{status, headers,
   body}` objects; `edge/agent.ts` shrinks to a mounting shim in `host-hono`. The WS lifecycle
   (`attach` → status online → touchLiveness → replay; `detach` → status offline only when the
   current socket was evicted) moves verbatim — its ordering is protocol-visible and audited
   (HOST-M3-02/03/08/14), so it must not be re-implemented in the adapter.
5. **Events.** Generalize `InboundDeps.onTranscriptWrite` into `HostContext.events` (a typed emitter
   over the six event families in §3.3). `channels/pump.ts` subscribes; `AppEnv.Variables.webhookPump`
   and `c.get("webhookPump")` disappear.
6. **Lifecycle.** `host.start()/stop()` own the four core loops (liveness sweep, outbox GC,
   configstore GC, launch reaper); each already returns a stop handle. Timers `unref()`; `stop()`
   force-closes sockets with the existing `1001` code and closes the db.
7. **Move the SSRF host check** (`isPrivateUpstreamHost`) from `proxy/` to `core/net`, used by both
   channels and proxy.
8. **Public barrel + `exports` map.** One `index.ts` per package with an explicit allow-list; the
   `MODULE = "…"` constants and drizzle row types stay internal. Types exported: `Host`, `HostOptions`,
   `Wire`, `WsLike`, `Launcher`, `Logger`, `Metrics`, `HostEvents`, the frame schemas from
   `protocol/frames.ts` (already the generated-schema source of truth), and the `AppError` family.

Each step is a no-behavior-change refactor verifiable by the existing suites; steps 3 and 4 are
the only ones with runtime risk and get the docker cross-implementation E2E (`e2e/run.sh`) run
after them — the 2026-08 alignment audit showed it is the only test that catches wire drift.

## 6. Rollout

| phase | deliverable | verification |
|---|---|---|
| P0 — seams (in place, no package split) | §5 steps 1, 2, 5, 6, 7; app keeps working via `createApp` | `bun run check` (673 tests), `e2e/run.sh` |
| P1 — storage + wire extraction | §5 steps 3, 4 with the `Storage`/`AgentTransport` ports of §8 (recommended) or the driver seam of §3.4 (minimal); `createHost` exists and `app.ts` is rewritten on top of it | same, plus the port conformance suite against sqlite + memory adapters |
| P2 — package split | workspace layout of §3; `apps/host` is the only place with `Bun.*`, env, lode, docs, CLI; `host-channels`/`host-proxy` extracted | per-package `check`; app e2e unchanged; a new `examples/embed-node` (express or Fastify + `ws`) that enrolls a real `gino-agent` and round-trips one task — the SDK's own cross-implementation test |
| P3 — API freeze | typed docs for `Host`/`Wire`/events; semver from 0.x; publish to the internal registry | consumer example pinned to the published version |

**P0 status (2026-09-05):** landed in place on branch `bkd/qsi4ffym` (`src/host/{context,options,events,metrics}.ts`, `src/shared/lib/net.ts`) via merges 8de5d8d (net util), a9fcbf9 (HostContext + de-singleton sessions/launchers + `Metrics` sink), 91597e7 (`HostOptions`, `createStore(storeDir)`), 5e3f49f (`HostEvents`, pump subscription, `webhookPump` removed), 0ac295f (`host.start()/stop()` lifecycle); verified by `bun run check` (701 tests) and `e2e/run.sh` (57/57 checks) on the integrated branch.

**P1 status (2026-09-05):** landed in place on branch `bkd/6p7rx0ly`, taking the recommended ports route of §8 rather than the §3.4 driver seam. What exists now: the `Storage` and `AgentTransport` ports (`src/host/ports/{storage,transport}.ts`) via merges b6aa958 (`Storage` port + memory adapter + conformance suite) and 4161ed6 (`AgentTransport` port); the sqlite reference adapter `src/store/sqlite/**` with `sqliteStorage(db)` via 1bb9f7b (part 1) and aab3fcc (part 2 + `sqliteStorage`); the in-memory adapter `src/store/memory/**` (b6aa958); one conformance suite in `tests/conformance/**` run against both adapters (102 assertions across memory + sqlite); the domain modules rewritten on the ports via d7eea29 (configstore + msgstore), 6df946b (agents + protocol) and 3b2f66c (tasks + channels, compatibility shims removed) — after which both grep gates are zero: no `from 'drizzle-orm'` outside `src/store/**` and `src/db/**`, and no `src/db/*` import from `src/{configstore,msgstore,agents,protocol,tasks,channels}/**`; the wire layer `src/host/wire/**` with `src/edge/agent.ts` reduced to a thin Hono shim over it via 7e72540; and `createHost` in `src/host/host.ts` with `src/app.ts` composed on it via 18650b0. Verified by `bun run check` (824 tests) and `e2e/run.sh` (57/57 checks) on the integrated branch. Deferred to P2: the driver generalization (better-sqlite3 alongside `bun:sqlite`), the package split, `@hono/node-ws`, and the `host-channels`/`host-proxy` extraction.

Estimated size: P0 ≈ 1.5k lines touched (mostly mechanical), P1 ≈ 2k (extraction), P2 ≈ moves +
build config, P3 ≈ docs. No protocol version bump at any phase.

## 7. Open decisions

1. **Admin HTTP API in the adapter or not.** Keeping `adminRouter(host)` costs little (it is
   1:1 over `host.tasks/agents`) and helps drop-in users; it also drags the admin bearer-token
   policy along. Proposal: ship it, but with `authorize: (req) => principal | null` injected by
   the embedder, no built-in token.
2. **`host-channels` in or out of the default install.** Proposal: separate package; the SDK's
   primary consumption model is `host.events` + `host.transcript`.
3. **WS adapter for Node.** `@hono/node-ws` is the least code; embedders not on Hono use the raw
   `WsLike` contract. Proposal: ship the Hono one, document the raw one with the example.
4. **Multi-host per process.** Once singletons are gone this is free; the only shared state left
   would be a shared SQLite file, which must be forbidden (one host per db file) — enforce with the
   existing migration lock file.
5. **Proxy WS bridge** (`edge/llm.ts` raw Codex Responses passthrough) is Bun-socket specific;
   in `host-proxy` it stays Bun-only unless someone needs it on Node.

## 8. Going further: agent transport and storage as ports

§3.4 keeps drizzle and only swaps the SQLite driver. This section answers the stronger
question — *can the agent-communication side and the storage side each sit behind an
interface, so an embedder brings its own database and its own transport?* — with the
measured shape of the code. Short answer: **yes, and the code is already close.**

### 8.1 Evidence

| measure | value | meaning |
|---|---|---|
| domain functions that take `db: AppDatabase` | 94 (tasks 22, agents 20, channels 18, configstore 17, msgstore 10, protocol 7) | that list *is* the storage port; nothing else touches storage |
| explicit transactions | 1 (`configstore/entries.ts` `setEntries`: version bump + scope replace) | one atomic operation to model as a single port method |
| idempotent upserts (`onConflict…`) | 10 | become `…IfAbsent` / `upsert…` port methods with stated keys |
| `.returning()` | 14 | mostly "give me the row back"; the one that matters is the outbox insert (id → `msgId`) |
| raw SQL outside `db/` | 2 × `SELECT 1` (health probes) | no SQL semantics leak into the domain |
| transport-facing seams | `FrameSender { isOnline, send }`, `WsLike { send, readyState?, close, raw? }`, `SessionRegistry` class | already interfaces; only the exported singleton binds them to Bun |
| bytes | `BlobStore` class (`put`, `readVerified`, `exists`, `sizeOf`, `delete`, `listLeaves`, `refFor`) with 5 call sites | one interface away from an S3/object-store implementation |

The protocol itself (`protocol/frames.ts`, `envelope.ts`, the inbound dispatch and the outbox
*semantics*) is **core, not a port**: at-least-once, `(threadId, seq)` dedup, `msgId` acks,
replay-on-reconnect are what the runner depends on and must be identical across every adapter.
Ports abstract *where the rows live* and *how bytes reach the socket*, never *what the frames mean*.

### 8.2 Layout (hexagonal)

```
host-core
  protocol/        frames, codec, inbound dispatch, outbox semantics       (pure)
  domain/          agents · tasks · crons · transcript · config · wire     (pure, take `ports`)
  ports/           Storage (repositories + BlobStore) · AgentTransport      (interfaces + conformance tests)
host-store-sqlite  drizzle + sqlite driver(bun | better-sqlite3 | libsql) — today's 22 drizzle files, moved
host-store-memory  in-memory Storage for tests / prototypes (~400 lines)
host-hono          HTTP mount + WS adapters implementing AgentTransport
```

Domain functions change signature from `(db, …)` to `(ports, …)` — same bodies, with the drizzle
calls replaced by repository calls. The sqlite adapter is the current code regrouped, so the
reference implementation costs no new logic.

### 8.3 Storage port — repositories and the invariant each must keep

The runner's correctness depends on a handful of storage-level guarantees. They are written
into the port as contracts and enforced by a **conformance test suite** that every adapter
must pass (run today against sqlite and memory; tomorrow against Postgres).

```ts
interface Storage {
  agents: AgentRepo; tasks: TaskRepo; outbox: OutboxRepo; transcript: TranscriptRepo;
  threadRuns: ThreadRunRepo; crons: CronMirrorRepo; attachments: AttachmentRepo;
  config: ConfigRepo; blobs: BlobStore; channels?: ChannelRepo;   // channels only with host-channels
  close(): Promise<void>; checkpoint?(): Promise<void>;
}
```

| repo | methods (from today's functions) | invariant the adapter MUST uphold |
|---|---|---|
| `OutboxRepo` | `append(row) → {id}`, `markAcked(agentId, id)`, `listUnacked(agentId)`, `purge(agentId)`, `gc(before)` | **`id` is strictly monotonic per agent and returned on append** — it becomes the wire `msgId`; the runner dedups on it and replays in ascending order. SQL: autoincrement; non-SQL: a per-agent sequence |
| `TranscriptRepo` | `insertIfAbsent(threadId, seq, row) → boolean`, `read(taskId, threadId, {after, limit})`, `stats(threadId)`, `threads(taskId)`, `taskOf(threadId)` | `(threadId, seq)` unique, first writer wins; `read` ordered by `seq` |
| `ThreadRunRepo` | `upsert(threadId, run)`, `get(threadId)` | upsert updates only the mutable subset (`status`, `failure_reason`, `engine_session_id`, `duration_ms`, `final_seq`); `source`/`cron_id` set once |
| `CronMirrorRepo` | `upsertState`, `upsertPrompt`, `get`, `listByTask` | `prompt` and `task_id` preserved across state upserts |
| `AttachmentRepo` | `insertIfAbsent(descriptorRow)`, `getById`, `listBySha` | idempotent on `id` |
| `AgentRepo` | `insert`, `get`, `list`, `getByConnectTokenHash`, `updateStatusIf(agentId, {from?, notFrom?}, to) → boolean`, `touchLiveness`, `setRemoved`, `sweepSilent(before) → ids`, grants (`grant`/`revoke`/`list`/`isPermitted`), `setAccess`, launches (`recordPending`, `correlate`, `hasPending`, `listExpired`, `deletePending`, `reEnrollByNonce`) | **status transitions are compare-and-set** (`removed` terminal; `offline→online` only from `offline`); `launch_nonce` unique; token lookups exclude `removed` |
| `TaskRepo` | `insert`, `get`, `list(filter)`, `update(partial) → Task`, `setPlacementOnce(taskId, agentId) → boolean`, `orphanByAgent(agentId) → ids`, `setCurrentThread`, `setStatus`, `markSeeded` | placement is set-once (CAS on `agent_id IS NULL`) |
| `ConfigRepo` | artifacts: `upsert(name, sha, meta)`, `get`, `getBySha`, `list`, `markUnreferenced(now)`, `droppable(before)`; entries: **`replaceScope(scope, entries) → version`** (the single transaction: replace the scope's desired set AND bump the monotonic global version atomically), `entriesFor(scope)`, `clearScope`, `isShaInDesiredSetOf(agentId, sha)`, `referencedShas()`, `globalVersion()` | `(name, sha256)` unique; `(scope, target)` unique; `replaceScope` atomic and the version monotonic |
| `BlobStore` | `put(name, bytes) → {name, sha256, size, ref}`, `readVerified(ref, sha256)`, `exists`, `sizeOf`, `delete`, `listLeaves`, `refFor` | content-addressed; `readVerified` fails on hash mismatch (never serves corrupt bytes) |
| `ChannelRepo` (optional) | channels/webhooks/bindings/cursors as today | `(channel, conversation_ref)` and `(channel, task)` unique; cursor advance monotonic |

Deliberate omissions: **no generic `transaction()` on the port**. The only cross-row atomicity
the domain needs is `ConfigRepo.replaceScope`; keeping it a named method (instead of exposing
transactions) is what leaves a non-SQL adapter possible. Any future need for atomicity becomes
another named port method, not a leak.

Ids: agent/task/attachment ids are already minted in the domain (ULID/nanoid via an injectable
`ids` option), so only the outbox id is storage-generated — hence the single monotonic-id
contract above.

### 8.4 Agent transport port

```ts
interface AgentTransport {                 // implemented by host-hono (bun ws / node ws) or anything else
  isOnline(agentId): boolean;              // = FrameSender.isOnline
  send(agentId, frame): boolean;           // best-effort push; false → stays in outbox (at-least-once is core's job)
  closeAgent(agentId, code, reason): void; // supersede / remove / sweep
}
// inbound direction is core's `host.wire` (§3.2): the adapter calls attachSocket / onSocketMessage /
// detachSocket; the socket handle only has to satisfy `WsLike`.
```

Because durability, dedup and replay live in core (outbox rows + `msg_ack`), the transport
only needs *a notion of connection* and *best-effort send*. That is what makes a non-WebSocket
link (a message broker, an SSH tunnel with a line protocol) a legitimate adapter — the runner
today speaks WS only, so the shipped adapters are WS, but core does not know that.

### 8.5 What this changes versus §3.4 and §5

- §3.4 (driver-only swap) becomes the **reference adapter** `host-store-sqlite`, not the design
  ceiling. The driver seam still exists inside that adapter.
- §5 step 3 becomes "introduce `Storage` ports + move the 22 drizzle files into the adapter"; step
  1's `HostContext` carries `ports` instead of `db`.
- Tests: unit tests today call `fn(db, …)` with `createDb(":memory:")`. Keep them by exporting a
  `sqliteStorage(db)` factory, so each test changes one setup line; the new conformance suite
  (one spec, N adapters) is where the invariants above are asserted explicitly instead of
  implicitly by the SQL schema.
- Cost: this is the larger refactor — every one of the 94 functions changes signature and ~2k
  lines of drizzle code move. Expected P1 size roughly doubles (≈4k lines touched, mostly
  mechanical). No protocol change; the docker cross-implementation E2E stays the acceptance gate.

### 8.6 Recommendation

Do the ports in P1 rather than the driver seam: the marginal cost over the driver-only design is
mostly signature churn, and it is the only version that lets an embedder keep its existing
database (Postgres via a `host-store-pg` adapter later, with the monotonic outbox id realized as a
sequence) and its existing socket layer. Ship `host-store-sqlite` (reference), `host-store-memory`
(tests), and the WS adapters; publish the conformance suite with the SDK so third-party adapters
can prove themselves.

## 9. `scripts/mini-host.ts` — the minimal host as a reference point

`scripts/mini-host.ts` (502 lines, `bun:sqlite` + `Bun.serve`, no other dependency) is a
protocol-v2 host reduced to the essentials: register / config / WS / output on the agent side,
and tasks / messages / crons / threads on the operator side. Verified on 2026-09-04 against the
real runner (the `bkd/uc3jboex` build = current main) with the e2e fake engine:

- enroll → `task_upsert` → `message` → `thread_message` + `thread_result` mirrored; second turn
  continues the same thread; `interrupt` (live-only) cancelled the in-flight turn;
- `cron_upsert` fired isolated `cronId@epoch` threads on schedule, `cron_state` mirrored,
  `cron_delete` + `task_delete` stopped it (run count unchanged 6 s later);
- outbox fully drained (`0` unacked rows across `task_upsert`/`message`/`cron_upsert`/
  `cron_delete`/`task_delete`) — i.e. the `msgId`-on-every-frame ack model of §4.2 holds;
- zero runner-side `WARN`/`ERROR` (apart from the cleartext-URL notice).

What it proves for this design: the *irreducible* core is small — outbox append/replay/ack
(~40 lines), the inbound mirror for four frame types (~60), a socket map with supersede
semantics (~20), register/config (~40), and the three command emitters (task/message/cron).
Everything else in `src/**` (≈13k lines) is hardening, policy, provisioning, attachments,
channels, proxy and operations. That is exactly the core/adapter split of §3 and §8, arrived at
from the other direction.

What it deliberately lacks (and the SDK must keep from `src/**`): hashed + constant-time
connect-token checks and `removed`-terminal CAS status; heartbeat-based offline detection (it
equates "online" with "socket present"); the attachment byte channel and provisioning entries
(it returns `entries: [], version: 0`); `final_seq` completeness diagnostics; per-agent outbox
GC; any operator auth (loopback only); a `wait` that distinguishes turns (it returns as soon as
*a* run row exists for the thread).

Recommendation: treat `mini-host.ts` as the **executable specification of `host-core`'s
protocol/domain layer** — its `enqueue`/`replayUnacked`/`handleAgentFrame` are the reference
semantics for `OutboxRepo`, `TranscriptRepo`/`ThreadRunRepo`/`CronMirrorRepo` and `host.wire`
(§3.2, §8.3) — and port the hardening from `src/**` behind the ports rather than carving the big
host down. Keep the script in the repo as the smallest cross-implementation check that runs
without docker (`bun scripts/mini-host.ts` + a runner binary + `e2e/fake-claude` on `PATH`).
