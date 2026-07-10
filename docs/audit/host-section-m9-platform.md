# Host Audit — Section M9: platform, db, shared, config, bootstrap

Scope: `src/platform/**`, `src/db/**`, `src/shared/lib/**`, `src/shared/middleware/**`,
`src/config.ts`, `src/config/schema.ts`, `src/dirs.ts`, `src/build-info.ts`, `src/app.ts`,
`src/cli.ts`, `src/index.ts` (26 files, every line read). Ground truth:
`docs/host-architecture.md` (module map + §8 invariants), `docs/protocol.md` §3–§6.
Cross-side patterns re-checked from `docs/audit/agent-audit-2026-07.md` (offline handling,
queue GC, token storage, shutdown chain, double-start).

Verified clean (explicitly checked, no finding):

- **UTC mandate (module focus 2)**: every datetime column is an integer epoch-milliseconds
  value defaulted via `$defaultFn(() => Date.now())` (`src/db/schema.ts`); no
  `CURRENT_TIMESTAMP`, no `datetime('now','localtime')`, no Date-object serialization
  anywhere in the schema or migration SQL. Epoch-ms is timezone-unambiguous.
- **Crypto (module focus 1)**: a timing-safe compare helper exists and is correct —
  `safeEqual` (`src/platform/crypto.ts:15`) SHA-256-digests both inputs to fixed length
  before `timingSafeEqual`, hiding length differences. `hashToken`/`verifyToken` store only
  SHA-256 digests (appropriate for the 256-bit random tokens minted via
  `randomBytes(32)` — CSPRNG, verified in `src/agents/registry.ts:45`). The three-token
  auth middleware (`src/shared/middleware/auth.ts`) uses `safeEqual` for every secret
  comparison and fails closed on an empty configured secret.
- **Migrations (module focus 3)**: a single initial migration
  (`drizzle/0000_smiling_prism.sql`) containing only `CREATE TABLE`/`CREATE INDEX` — no
  destructive statements. The embedded-migration fallback fail-fasts on an empty map and on
  a missing `meta/_journal.json` sentinel (truncated-bundle guard). The pre-migration
  snapshot / migrate / record-version ordering in `src/db/index.ts:59-61` is correct (the
  version marker is written only after `runMigrations` returns).
- **DB PRAGMAs**: `foreign_keys = ON`, WAL, `busy_timeout` are set
  (`src/db/index.ts:32-34`) — the agent-side "FKs never enabled" gap (agent audit P2-19)
  does not recur on the host.
- **Error handler leakage (module focus 4)**: `error-handler.ts` returns generic messages
  for 409/500; internal error details reach only the log, and `NotFoundError` surfaces only
  opaque ids. Validation 422 details are Zod messages, not values.
- **Signals (module focus 6)**: SIGINT, SIGTERM and SIGHUP (log reopen) are all handled
  (`src/index.ts:110-120`) — the agent-side SIGTERM gap (agent audit P1-6) does not recur.
  DB close ordered after server stop; WAL checkpointed on the lode prepare handshake.
- **Auth middleware**: connect-token lookup resolves identity from the token hash (never a
  client-supplied id), excludes `removed` agents, and 403s cross-agent access to
  `:agentId`-scoped routes.

### [P1] Graceful shutdown is inverted: `server.stop(true)` runs first, killing all in-flight work immediately

- Location: src/index.ts:57-67
- Description: The non-fatal shutdown path races `server.stop(true)` against a 25 s timer,
  then calls `server.stop(false)`. In Bun, `stop(true)` immediately terminates every active
  connection — in-flight HTTP requests and all agent WebSocket sessions — so the "bounded
  drain" resolves almost instantly having drained nothing, and the follow-up `stop(false)`
  is a no-op on an already-stopped server. The intended order (stop accepting → drain →
  force-close) is reversed.
- Failure scenario: Every ordinary SIGTERM (systemd stop, deploy, lode cut-over): an admin
  POST mid-write is aborted mid-response; every connected agent's WS is dropped without a
  close handshake and must rely on reconnect+outbox replay. The 25 s drain window the code
  documents never occurs, on every single restart.
- Suggested fix direction: Swap the arguments: race `server.stop(false)` (graceful drain)
  against the 25 s timer, then call `server.stop(true)` as the force-close fallback.

### [P2] Rate-limit middleware is dead code — no route in the host is rate-limited

- Location: src/shared/middleware/rate-limit.ts:94 (definition; zero call sites in `src/**`)
- Description: `rateLimit()` is fully implemented (per-IP buckets, GC timer, eviction cap)
  but never mounted on any router. `docs/host-architecture.md` §M1 states the edge "Owns
  transport, auth, rate-limit, SSRF guard", and `docs/protocol.md` §3 defines the
  `register_error` reason `rate_limited` — which the host can never produce. The
  unauthenticated `POST /agent/register` (shared long-lived bootstrap secret) and the
  channel-token surfaces have no brute-force throttle at all.
- Failure scenario: An attacker with network reach makes unlimited high-rate guesses
  against the shared register token (and the admin bearer); nothing throttles, delays, or
  surfaces the attempts as `rate_limited`. A weak operator-chosen `GINO_REGISTER_TOKEN`
  becomes enrollable-by-brute-force.
- Suggested fix direction: Mount `rateLimit(...)` on the unauthenticated/secret-bearing
  edges (`/agent/register` at minimum, ideally `/admin/*` and `/messages/*` auth failures),
  and wire the register handler to return `register_error{rate_limited}` per protocol §3.

### [P2] Logger redaction misses the host's own secret field names (exact-match key set)

- Location: src/shared/lib/logger.ts:60-78
- Description: `REDACT_KEYS` is matched by exact lowercase key equality, and the set does
  not contain the names this codebase actually uses for secrets: `registerToken` /
  `connectToken` (the protocol §3/§4 wire field names), `x-internal-token` (the connect
  header), `apiKey` / `api_key`, or any `PROXY_*_API_KEY` / `PROXY_*_OAUTH_TOKEN` /
  `PROXY_CODEX_REFRESH_TOKEN` / `PROXY_STATIC_KEY` config keys. Only bare `token`,
  `secret`, `authorization`, etc. are redacted. `host-architecture.md` §M9 names
  "logger(redaction)" as a platform guarantee; the guarantee holds for generic names but
  not the system's own vocabulary. (No current call site logs such an object — verified —
  so this is latent, but redaction exists precisely to protect against future/error-path
  callers.)
- Failure scenario: Any log statement that includes a register request body
  (`{ registerToken: ... }`), a register response (`{ connectToken: ... }`), request
  headers, or the resolved `Config` object writes the secret in cleartext to
  `data/logs/gino.log`, where it survives rotation into archived logs.
- Suggested fix direction: Add the concrete key names used by this codebase
  (`registertoken`, `connecttoken`, `x-internal-token`, `apikey`, `api_key`, and the
  `proxy_*` credential keys), or switch to substring/suffix matching on `token`/`key`/
  `secret` — the file's own comment already prefers false positives over false negatives.

### [P2] Metrics registry grows without bound from unauthenticated request paths

- Location: src/shared/middleware/logging.ts:22-27 (coarsenPath) + src/shared/lib/metrics.ts:33 (registry)
- Description: `loggingMiddleware` records `http_requests_total` and a duration histogram
  labeled by the coarsened request path for every request, including 404s. `coarsenPath`
  only collapses id-shaped segments (32+ hex, 8+ mixed alnum, 4+ digits); alpha-only or
  short segments pass through verbatim. The metrics registry (`Map`) has no eviction, so
  each distinct surviving path creates permanent counter + histogram entries.
- Failure scenario: An unauthenticated client sprays `GET /api/aaa`, `/api/aab`,
  `/api/abc-def`, ... — each unique alpha-only path adds a registry entry (counter +
  11-bucket histogram) that is never reclaimed. Sustained spraying grows host memory
  without limit; no auth is required because 404s are still metered.
- Failure is aggravated by the endpoint never being readable (see the P3 below): the
  memory is spent on data no one can scrape.
- Suggested fix direction: Only record metrics for matched routes (e.g. use Hono's matched
  route pattern instead of the raw path, or label 404s with a fixed `unmatched` route), and
  cap registry size defensively.

### [P2] Proxy allow-list gate fails open when the peer IP is unavailable

- Location: src/shared/lib/client-ip.ts:43
- Description: With `TRUST_PROXY=true` and `TRUSTED_PROXY_IPS` configured, the gate is
  `if (proxyAllowList.length > 0 && peerIp && !isAllowedPeer(...)) return peerIp`. When
  `peerIp` is `undefined` (Bun's `server.requestIP()` can return null, and `c.env.IP` is
  absent for any request not threaded through `index.ts`'s fetch wrapper), the condition
  short-circuits and execution falls through to trusting `X-Forwarded-For` from an
  unverified peer — the opposite of the allow-list's purpose.
- Failure scenario: Operator configures `TRUSTED_PROXY_IPS=10.0.0.5` expecting only the
  LB to set XFF. A request path where the runtime cannot resolve the peer address (or a
  future caller that doesn't thread `IP` into the fetch env) lets any direct client forge
  `X-Forwarded-For` and evade every IP-keyed control.
- Suggested fix direction: When an allow-list is configured and the peer IP cannot be
  determined, fail closed: ignore forwarding headers and return `"unknown"`.

### [P2] Embedded-migration temp directory is predictable and pre-creatable (local TOCTOU → SQL execution)

- Location: src/db/index.ts:160-172
- Description: The compiled-binary migration path writes migration SQL to
  `/tmp/gino-migrations-<pid>` — a predictable name in a world-writable directory —
  using `mkdirSync(..., { recursive: true })`, which succeeds silently when the directory
  already exists and is attacker-owned. Between `writeFileSync` and `migrate()`, a local
  user who owns the directory can swap file contents (or pre-place a symlinked `meta/`);
  the daemon then executes attacker SQL against the host database.
- Failure scenario: Unprivileged local user watches for the gino process, pre-creates
  `/tmp/gino-migrations-<pid>` for likely pids (or races the window), and replaces a
  written `.sql` file before `migrate()` reads it — arbitrary DDL/DML runs with the
  daemon's authority (e.g. rewrite `agents.connect_token_hash`).
- Suggested fix direction: Use `mkdtempSync(join(tmpdir(), "gino-migrations-"))` (random,
  owner-only 0700 directory), or better, run migrations from the in-memory map without
  touching the filesystem.

### [P2] Outbox has no GC and no index for the replay scan

- Location: src/db/schema.ts:146-157 (table definition; grep-verified zero `DELETE`
  statements against `outbox` anywhere in `src/**`)
- Description: Every host→agent frame is persisted to `outbox` (at-least-once, per
  invariant §8) and rows are only ever marked `acked_at` — never deleted. The table has no
  index on `(agent_id, acked_at)`, so the reconnect replay query
  (`WHERE agent_id = ? AND acked_at IS NULL`, src/protocol/outbox.ts:100) full-scans a
  monotonically growing table. This is the host-side twin of agent audit P1-9 ("zero GC on
  inbox/outbox/cron_logs"), which that audit flagged as a pattern to re-check here.
- Failure scenario: A long-lived deployment with active tasks accumulates outbox rows
  indefinitely (every `message`, `task_upsert`, `cron_upsert`, `config_update` forever);
  each agent reconnect scans the whole table under SQLite's writer lock — replay latency
  and DB size degrade linearly with host age until operators intervene manually.
- Suggested fix direction: Add a retention sweep (DELETE acked rows older than a window,
  e.g. alongside the liveness sweep) and a partial index on
  `(agent_id) WHERE acked_at IS NULL` — mirroring the agent-side remediation plan.

### [P3] `isSpoofableProxyConfig` warning is never wired

- Location: src/shared/lib/client-ip.ts:80 (zero call sites in `src/**`)
- Description: The helper exists specifically so the app logs a startup warning when
  `TRUST_PROXY=true` with no `TRUSTED_PROXY_IPS` (the spoofable configuration its own
  doc-comment describes), but nothing calls it — the warning never fires.
- Failure scenario: An operator enables `TRUST_PROXY` on an internet-exposed host without
  an allow-list; every IP-keyed control is silently forgeable and no boot-time signal
  points it out.
- Suggested fix direction: Call it from `bootstrap()` and log a prominent warning when true.

### [P3] Metrics pipeline is write-only: `renderPrometheus` has no endpoint

- Location: src/shared/lib/metrics.ts:125 (no caller outside the module); src/shared/middleware/logging.ts:10 (`SKIP_METRICS_RE` guards a `/metrics` path that does not exist)
- Description: Metrics are collected on every request, but `renderPrometheus()` is never
  mounted on any route — no `/metrics` endpoint exists in `src/routes/**` or
  `src/modules/**`. The collection cost (and the P2 cardinality exposure above) is paid
  for data no one can scrape; the skip-regex in the logging middleware references the
  nonexistent endpoint.
- Failure scenario: Operator points Prometheus at the documented-looking `/metrics` path
  and gets 404; meanwhile the in-process registry consumes memory for nothing.
- Suggested fix direction: Mount a (channel-token-guarded or bind-restricted) `/metrics`
  route in the system module, or remove the collection until it is served.

### [P3] `gino migrate --check` reports every migration as pending

- Location: src/cli.ts:108-128
- Description: `listFsPendingMigrations` maps journal entries to tags and keeps those that
  HAVE a matching `.sql` file on disk — i.e. the full set of available migrations. It never
  consults the database (documented), so nothing is subtracted for already-applied
  migrations: on a healthy, fully-migrated install it prints "N pending migration(s)"
  forever.
- Failure scenario: An operator gating a deploy on `migrate --check` output always sees
  pending migrations and either blocks the rollout or learns to ignore the tool — both
  outcomes defeat its purpose.
- Suggested fix direction: Either read `__drizzle_migrations` from the target DB to
  subtract applied tags, or rename/re-document the command as "list available migrations".

### [P3] `generateId` (8-char nanoid, ~41 bits) is collision-prone for high-volume ids

- Location: src/platform/ids.ts:13 + src/shared/lib/id.ts:11
- Description: `generateId(prefix)` appends an 8-character nanoid over a 36-char alphabet
  (~41.4 bits). Birthday collisions reach ~50 % around 1.7 M ids. It is used for
  attachment primary keys (`generateId("att")`, src/edge/agent.ts:413) — a per-message
  volume table.
- Failure scenario: A long-lived deployment with millions of attachments hits a duplicate
  `attachments.id`; the insert fails with a UNIQUE constraint error surfaced to the agent
  as an opaque 409 on an otherwise-valid upload.
- Suggested fix direction: Lengthen the nanoid (12–16 chars) or use `ulid()` for
  row-volume ids; reserve the short form for human-facing, low-volume resources.

### [P3] Error handler classifies by message text and mislabels non-uniqueness constraints

- Location: src/shared/middleware/error-handler.ts:24-32
- Description: Any error whose `message` matches
  `/(UNIQUE|CHECK|FOREIGN KEY|NOT NULL) constraint failed/i` — including a non-database
  error that merely quotes such text — is returned as 409 "Resource already exists or
  violates a constraint". FK and NOT NULL violations (data bugs, not duplicates) get the
  same misleading "already exists" framing and skip the 500 log line.
- Failure scenario: A FOREIGN KEY failure caused by a genuine host bug (e.g. a mirror
  write for an unknown task) is answered 409 to the caller and never logged as an error,
  hiding the defect.
- Suggested fix direction: Match only on the structured `err.code` prefix
  (`SQLITE_CONSTRAINT*`), branch UNIQUE → 409 from the specific code
  (`SQLITE_CONSTRAINT_UNIQUE`/`_PRIMARYKEY`), and let FK/NOT NULL fall through to the
  logged 500.

### [P3] Compiled binary prefers an on-disk `drizzle/` folder from its cwd

- Location: src/dirs.ts:23-28 (`ROOT_DIR = process.cwd()` under `/$bunfs`) + src/db/index.ts:128-131
- Description: In the compiled binary, `ROOT_DIR` is the process working directory, and
  `runMigrations` prefers `<ROOT_DIR>/drizzle` over the embedded map whenever
  `drizzle/meta/_journal.json` exists there. Launching the binary from any directory that
  happens to contain a (stale, foreign, or attacker-planted) `drizzle/` tree silently
  applies that SQL to the production database instead of the vetted embedded set.
- Failure scenario: Operator runs the packaged binary from a checkout of a different
  branch/version; its `drizzle/` journal differs from the binary's embedded one and the DB
  is migrated with mismatched SQL.
- Suggested fix direction: In the compiled binary (`/$bunfs` detected), use the embedded
  map exclusively (or require an explicit env opt-in to load filesystem migrations).

### [P3] No cross-process double-start guard before migrations run

- Location: src/app.ts:158-181 (`bootstrap()` migrates) + src/index.ts:18 (port bind afterwards)
- Description: Port-bind failure (EADDRINUSE) is the only double-start protection, and it
  happens after `createDb` has already opened the DB, snapshotted, and run migrations.
  Two instances started concurrently both execute the migration path against the same
  file before one loses the bind; there is no pidfile/flock (the same gap the agent audit
  logged as P2-18). With today's single idempotent-under-journal migration the window is
  benign, but any future multi-statement migration can interleave.
- Failure scenario: A supervisor restart races a manual start during an upgrade; both
  processes enter `runMigrations` on a version boundary — the second applies a
  half-visible journal state or fails mid-DDL, and the pre-migration snapshot may be taken
  from an already-half-migrated file.
- Suggested fix direction: Take an exclusive advisory lock (flock on a lockfile beside the
  DB, or `PRAGMA locking_mode`/`BEGIN IMMEDIATE` around the migrate step) before
  snapshot+migrate.

Section summary: P0=0 P1=1 P2=6 P3=7
