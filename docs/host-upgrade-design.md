# Gino host — upgrade & version update design

Status: design proposal. References the **lode** loader (`/srv/lodaer`,
`docs.architecture.md`) as the upgrade mechanism.

## 1. Goal

Give the gino **host** (the Bun control plane, `src/`) a first-class
**verify → launch → supervise → hot-update → rollback** story: ship a generic
image once, then roll new host versions without rebuilding the image, with
signed releases, automatic rollback on a bad version, and no loss of the host's
durable state (SQLite DB + content store + connected agents).

We do **not** invent a new updater. We adopt **lode** — a small static Rust
"verify·launch·update" loader that already does exactly this and is
language-agnostic (it runs any app declared by `[command]` in `lode.toml`). The
host becomes a *lode app*: a signed, versioned artifact lode loads and manages.

## 2. Why lode fits the host

| lode capability | What it gives the host |
|---|---|
| manifest-driven versions + channels | host releases published to a signed `manifest.json` (or GitHub Releases); `stable`/`beta` channels |
| two-layer verify (sha256 + ed25519) | a poisoned CDN/mirror can't ship a tampered or unsigned host binary |
| child-process supervision (PID-1, signal forwarding, keep-alive) | the host is supervised; a crash → backoff retry → pause (never crash-loops the container) |
| hot-update + **single-shot rollback** | a bad new host version auto-rolls-back to `last_good` within `health_grace` |
| `state.json` control channel | operator/admin can trigger update/rollback/restart by writing one file (no bespoke RPC) |
| versions kept side-by-side + GC | instant rollback; `keep_versions` prunes the rest |
| offline-capable | host still boots from the locally-installed version with no network |

The host is already a good lode citizen:

- **Bundles to a single JS file** — `bun scripts/package.ts` runs `bun build
  --target bun` (NOT `--compile`), embedding the drizzle migrations
  (`src/db/embedded-migrations.ts`) and injecting
  `BUILD_COMMIT`/`BUILD_TIME`/`BUILD_VERSION`. The output is one portable
  `dist/gino.js` (~1.2 MB) — a perfect lode asset. We deliberately ship a JS
  bundle, not a `--compile` standalone binary (~90 MB, embeds the Bun runtime):
  lode downloads/caches `bun` ONCE (`[runtime]`, §5) and reuses it across every
  version, so each release ships only the small bundle.
- **Auto-detects the loader's data dir** — relative data paths resolve against a
  persistent base **`DATA_DIR > LODE_DATA_DIR > cwd`** (`src/dirs.ts`), kept
  strictly separate from `ROOT_DIR` (code/migrations). Under lode the DB / store
  / logs land in the loader's persistent `LODE_DATA_DIR` automatically — no manual
  `GINO_DB_PATH` needed. It never falls back to `ROOT_DIR`: that is the swappable
  per-version code dir, so data there would be orphaned on upgrade.
- **Graceful SIGTERM** — `src/index.ts` already drains the server (bounded ~25s),
  flushes the logger, closes the DB, and `exit(0)`. This *is* lode's required
  stop contract.
- **Version self-report** — `GET /api/system/version` returns
  `{ commit, buildTime, version }` from `BUILD_INFO`.
- **Migrations on boot** — `createDb()` runs embedded migrations on every start,
  so a new version self-migrates the existing DB.

## 3. Architecture

```
generic image (built once)             ┌───────────────────────────────────────┐
zzci/ubase + lode (static, few MB)     │  lode  (ENTRYPOINT)                    │
                                       └───────────────────┬───────────────────┘
  /srv/gino/lode.toml (operator config)│ CLI / env / lode.toml
                                       ▼
  github/manifest ──HTTPS──► release/manifest  (channels → versions → asset gino.js)
                                       │  verify sha256 + ed25519 (trusted key)
                                       ▼
            versions/<ver>/gino.js  (atomic symlink switch on update)
                                       │  [command].run = "bun run gino.js"  ([runtime] provides bun)
                                       ▼
   lode spawns + supervises the host child ──── SIGTERM on stop/update (host drains, exit 0)
                                       │
   PERSISTENT data, auto-resolved under LODE_DATA_DIR (NOT the version dir):
     /srv/gino/data/db/gino.db    ← survives every upgrade/rollback
     /srv/gino/data/store
     /srv/gino/state.json         (lode ↔ host control channel)
```

One host = one lode instance following one channel. Switching the release feed
or channel is a config change, never an image rebuild.

## 4. Packaging & release flow (publisher / CI)

CI does this on a published GitHub Release (`.github/workflows/release.yml`,
modelled on `zzci/bithk`). The reusable step is `bun run package`
(`scripts/package.ts`):

```bash
# build the JS bundle + emit a lode/v1 manifest + checksums
bun run package --version 0.2.0 \
    --asset-url https://github.com/gimhq/gino/releases/download/v0.2.0/gino.js
#   → dist/gino.js   (the asset; `.js` ⇒ lode `raw`, lands at versions/<ver>/gino.js)
#   → dist/manifest.json  (schema lode/v1, name gino-host, stable→0.2.0, run="bun run gino.js")
#   → dist/checksums.txt
```

The `release` workflow then `gh release upload`s those (plus the agent static
binary `gino-agent-linux-x64`). **Signing is optional** — with a publisher key in
the `LODE_SIGN_KEY` secret, add `lode-cli sign` / `manifest-sign` steps; an
unsigned manifest still carries the sha256/size integrity and works under
`require_signature = auto` with no key. Both source modes verify the same way:
GitHub Releases (`github = "gimhq/gino"`, reads the assets directly) or the native
`manifest.json`.

`BUILD_VERSION` (injected by `package.ts` at build time) matches the manifest
`version`, so `/api/system/version` and `LODE_ACTIVE_VERSION` agree (used for
health checks / audits). GitHub Releases is the low-friction option: tag `v0.2.0`,
attach `gino.js`, put the per-asset `sig` in the asset **label** when signing;
lode reads `github = "gimhq/gino"`.

## 5. Operator config — `lode.toml` for the host

```toml
[global]
app      = "gino-host"
data_dir = "/srv/gino"            # lode.toml + versions/ + state.json live here

[update]
github   = "gimhq/gino"           # or: manifest = "https://releases.gimhq.dev/gino/manifest.json"
asset    = "gino.js"
channel  = "stable"
policy   = "check"                # off | check | auto  (see §8)
keep_versions = 3

[command]
run  = "bun run gino.js"          # the JS bundle, run by bun; cwd = version dir (where gino.js lands)
exec = "bun"                      # `lode run db:...` ≡ `bun run db:...` passthrough

# lode resolves `bun` PATH → cache → download, caches it at $DATA_DIR/runtime/bun,
# and reuses it on every later boot. One download, shared across all host versions.
[runtime]
runtime  = "bun"
download = "https://github.com/oven-sh/bun/releases/download/bun-v1.3.14/bun-linux-x64.zip"
version  = "1.3.14"               # probed via `bun --version`

[trust]
require_signature = "enforce"     # production: signed releases only

[supervise]
restart      = "on-failure"       # keep-alive: retry w/ backoff, then PAUSE (never crash-loop PID 1)
readiness    = "state"            # wait for the host to report it can serve (see §6)
ready_timeout = 30
health_grace = 20                 # a new version must stay healthy this long, else single-shot rollback
stop_timeout = 28                 # ≥ the host's ~25s drain, and < the container stop grace (raise docker --stop-timeout)

# Data dir is auto-detected: the host resolves relative data paths against
# DATA_DIR > LODE_DATA_DIR > ROOT_DIR, so with data_dir=/srv/gino the DB/store/logs
# land at /srv/gino/data/** automatically — no GINO_DB_PATH needed. Only secrets +
# the listen port are configured; host env (-e/secrets) wins over [env] defaults.
[env]
GINO_LOG_TO_STDOUT = "true"
# DATA_DIR         = "/srv/gino"   # optional explicit override (else LODE_DATA_DIR is used)
# GINO_PORT / GINO_REGISTER_TOKEN / GINO_HTTP_CHANNEL_TOKEN / GINO_PROXY_* come from
# container -e / secrets — never bake secrets here.
```

**Integration rule (now automatic):** the host's DB, content store and logs MUST
live on a **persistent path outside the swappable version directory** (lode swaps
`current → versions/<ver>` on every update). The host handles this itself — it
resolves relative data paths against **`DATA_DIR > LODE_DATA_DIR > cwd`**
(`src/dirs.ts`), **decoupled from `ROOT_DIR`** (which locates code/migrations and,
under lode, the swappable version dir). So under lode they land in the persistent
`LODE_DATA_DIR` (`/srv/gino/data/**`) without any path config, surviving upgrades
**and** rollbacks. `DATA_DIR` overrides; it never falls back to `ROOT_DIR` — data
must never resolve into the per-version code dir.

## 6. Host-side contract — what to keep, what to add

Already satisfied (no change): graceful SIGTERM drain → `exit(0)`; non-zero exit
on fatal startup; `/api/system/version`; boot-time migrations.

Recommended host additions (small, optional but they unlock readiness +
self-service updates):

   > **Implemented via the official lode SDK** (`src/platform/lode-sdk.ts`,
   > vendored verbatim from `github.com/dotns/lode/sdks/lode.ts`) behind a thin
   > gino adapter (`src/platform/lode.ts`). The SDK owns the state.json contract —
   > flock-serialised RMW, field preservation, readiness, update/restart/rollback,
   > the phased handshake (`markServing`/`ackPrepared`), `hold`/`release`, and a
   > `watch()` notifier. Re-copy the file to track upstream; it is lint/`ts`-skipped
   > as vendored code.

1. **Readiness report (`readiness = "state"`).** Once `Bun.serve` is listening,
   `markReady()` writes `state.ready = $LODE_INSTANCE` (SDK; temp-file + rename
   under `state.json.lock` `flock`, preserving lode's fields). Until then lode
   won't commit the new version or stop the old one — so a host that fails to
   bind/migrate never becomes "current".
   - *(Later)* opt into the **phased prepare handshake** (`markServing` → `-0`,
     `ackPrepared` → `-2`, `prepareRequested` → `-1`) so the host can drain
     in-flight work *before* cut-over — useful once zero-downtime modes land in lode.
2. **Admin-triggered self-update (optional).** An `POST /api/admin/system/update
   { version? }` that patches `state.json.target` (= a version or `"latest"`)
   lets an operator upgrade/rollback through the existing admin surface; lode
   polls the file (~1s) and applies it. `POST …/restart` bumps `restart_nonce`.
   This is pure file I/O — no new transport.
3. **Surface lode state read-only.** `GET /api/system/version` can also report
   `current`/`last_good`/`available`/`channel` by reading `state.json`, so the
   admin UI shows "update available / on version X".

## 7. Data & migration safety (the gino-specific risk)

lode rolls the **binary** back on a bad version, but the **DB is shared and
already migrated** — the classic forward-migration vs. rollback hazard. Policy:

- **Expand-contract migrations only.** Each release's schema change must be
  backward-compatible with the immediately-previous version (add columns/tables
  nullable/defaulted; never drop/rename in the same release that starts using the
  new shape). Then `last_good` (the previous binary) still runs against the
  migrated DB after a rollback. Destructive "contract" steps ship a release later,
  once the new shape is proven.
- **Migrations are idempotent + forward-only** (drizzle journal) — already true;
  a re-launched older binary simply finds the journal ahead and does nothing.
- **Back up before apply.** lode keeps the old *binary*; the host should snapshot
  the SQLite file (WAL checkpoint + copy to `data/db/backups/<ver>.db`) on a
  version change before the new binary migrates — cheap insurance for a schema
  change that an expand-contract discipline can't fully undo.
- **Rollback floor.** lode's downgrade floor already refuses to follow `latest`
  below `max(current,last_good)`; combined with expand-contract this keeps the
  DB/binary compatible window safe.

## 8. Update policy & operations

- **`policy = "check"` (recommended default):** lode periodically refreshes the
  catalog and writes `state.available`; nothing auto-applies. The operator (or
  the admin endpoint in §6.2) approves by setting `target`. Safe for a control
  plane — upgrades are deliberate.
- **`policy = "auto"`** for non-critical/edge hosts that should always track
  `latest`.
- **`policy = "off" / pin`** to freeze a version.
- **Rollback:** `lode-cli rollback` (→ `target = last_good`) or automatic on a
  failed health grace.
- **Visibility:** `lode-cli status` prints `state.json` + a manifest summary;
  the host can mirror it under `/api/system/version`.

## 9. Connected-agent continuity during a host restart

A `stop-start` upgrade drops the host's WebSockets briefly. This is tolerable by
design: the **gino-agent** reconnects with backoff and **replays its durable
outbox**, and the host de-dups inbound frames by `(threadId, seq)`. So an upgrade
window = "agents reconnect + replay", not lost work. To minimise it:

- `stop_timeout ≥ 25s` lets the host drain in-flight requests before exit; the new
  version is usually serving within seconds (fast boot — it activates the existing
  version without waiting on the network, lode §5).
- `readiness = "state"` ensures the *old* host isn't stopped until the *new* one
  reports it can serve (in zero-downtime modes; in `stop-start` it gates marking
  the version good).
- **Future zero-downtime:** when lode's `reuseport-overlap` / `socket-activation`
  land (v1 falls back to `stop-start`), the new host can take over the listening
  socket with no gap. The host would read fd 3 (socket activation) — a small
  `Bun.serve({ fd })` path. Out of scope for v1; the reconnect path already makes
  `stop-start` acceptable.

The **agent's own** updates are separate and even simpler: `gino-agent` is itself
a static binary and can be lode-managed the same way (its own `lode.toml` +
manifest). The host's docker launcher just runs a generic `lode`-based agent
image; switching agent versions is a manifest change.

## 10. Image — none of our own; run lode's published image

gino ships **no image**. lode already publishes the generic loader image
(`docker.io/dotns/lode`), which is the whole point ("bake a generic image once").
We just run it with a `lode.toml` pointing at the gino release feed:

```bash
docker run -d --name gino \
  -v gino-data:/srv/gino \                         # persist versions + bun cache + DB/store/state
  -v "$PWD/gino.lode.toml:/srv/gino/lode.toml:ro" \
  -e LODE_TRUSTED_KEYS="<key_id>:<base64-pubkey>" \
  -e GINO_PORT=3000 -p 3000:3000 \
  -e GINO_REGISTER_TOKEN=… -e GINO_HTTP_CHANNEL_TOKEN=… -e GINO_PROXY_ANTHROPIC_API_KEY=… \
  --stop-timeout 30 \                              # ≥ host drain + lode stop_timeout
  docker.io/dotns/lode:latest
```

lode (PID 1) boots → fetches/caches `bun` once into `/srv/gino/runtime/bun` →
verifies + launches the host bundle → supervises + hot-updates per policy. No
`docker build`, no gino-specific image, no Bun baked anywhere — switching host
versions is a manifest/release change. (A compose service is the same: `image:
docker.io/dotns/lode:latest` + the volume + the toml mount.)

## 11. Rollout plan

1. **JS bundle + CI/release** — `bun scripts/package.ts` → `dist/gino.js` and
   `bun run package` → bundle + `manifest.json` + checksums (done). CI
   (`.github/workflows/ci.yml`: host check + agent fmt/clippy/test + security +
   docker e2e) and release (`.github/workflows/release.yml`: on a published
   Release, upload the host bundle+manifest and the agent musl binary) are in
   place, modelled on `zzci/bithk`. Remaining: confirm `bun run gino.js` runs
   under the `[runtime]`-provided bun on `zzci/ubase`; wire `LODE_SIGN_KEY` to
   enable signing.
2. **Persistence** — the host now auto-resolves data under
   `DATA_DIR > LODE_DATA_DIR > cwd` (done, `src/dirs.ts`; decoupled from ROOT_DIR);
   document the `data_dir` layout and verify a rollback keeps the DB/store intact.
3. **Readiness helper** — add the `state.ready` writer (§6.1) gated on
   `readiness=state`; no-op when `LODE_DATA_DIR` is unset (dev / docker e2e).
4. **Admin update surface** (§6.2/6.3) — optional, behind the channel token.
5. **Migration discipline** — adopt expand-contract + pre-apply DB snapshot.
6. **e2e** — extend the docker-compose e2e with a `lode`-managed host: publish
   v0.1.0, boot, publish v0.2.0, observe hot-update + a deliberately-bad v0.3.0 →
   auto-rollback, with agents reconnecting throughout.

## 12. Open decisions

- **Feed:** GitHub Releases (simplest, tag-authoritative) vs. a native signed
  manifest on object storage (catalog signature, private hosting). Recommend
  GitHub Releases for `gimhq/gino` now; native manifest if releases must be
  private/air-gapped.
- **Channels:** `stable` only, or `stable`+`beta` for canarying the host.
- **Zero-downtime:** accept `stop-start` + agent-reconnect for v1; revisit
  socket-activation when lode implements it (it's a forward-looking mode today).
- **Single control plane:** the host is a SQLite-backed singleton, so upgrades are
  one-at-a-time per host; HA/multi-host is a separate design (shared store) and
  out of scope here.
```
