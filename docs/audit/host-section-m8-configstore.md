# Host Audit — M8 Config Store (`src/configstore/**`)

Scope: `src/configstore/{index,store,entries,artifacts,serve,gc}.ts` audited line-by-line against
`docs/host-architecture.md` (§M8, §8) and `docs/protocol.md` (§4–6), with read-only context from
`src/db/schema.ts`, `src/edge/agent.ts`, `src/edge/admin/config.ts`, `src/tasks/{store,orchestrator}.ts`,
`src/protocol/{frames,inbound}.ts`, `src/msgstore/attachments.ts`, and `docs/audit/agent-audit-2026-07.md`.

Module-focus answers up front: (1) the artifact hash is verified on serve (`readVerified`) but NOT on the
write dedup path — a corrupted/collided blob is never *served* under a valid hash (it 409s), but it can
become permanently unserveable with no self-heal (P2 below). (2) The GC scan itself is synchronous and
cannot interleave with reference creation in-process, but it has no grace period after un-reference, no
referential validation on the entries side, and is not crash-atomic per artifact (P2/P3 below). (3) The
serve path has no traversal today only because the sole `attachments.ref` writer is host-side; the store
never enforces root containment (P2 below); serving is connect-token + placement/desired-set authorized at
the edge; no caching headers exist at all (P3 below). (4) Scope resolution has a delivery-contract break
for the global scope (P0 below) and orphaned-entry lifecycle gaps (P2 below).

### [P0] Global config carries no `version`; the documented reconcile gate is broken, so global desired-set changes are never applied

- Location: src/configstore/entries.ts:164 (delivery shape), manifests at src/edge/agent.ts:221 and src/edge/agent.ts:295
- Description: `docs/host-architecture.md` §M8 (delivery) mandates: "The global config carries a
  host-assigned monotonic `version`; the agent reconciles the global scope only when the version advances
  (restart-durable)", and `agent-architecture.md` §9b specifies "same/absent version ⇒ repair-only". The
  host never mints, stores, or sends any version: `resolveEntriesForEngine` returns a bare entry array, the
  register/config responses attach it as `entries` with no version field, and no `configVersion`-like
  symbol exists anywhere in `src/**`. The agent-side audit (agent-audit-2026-07.md, Summary) confirms the
  agent's §9b version gate is implemented and tested — it will treat every host response as
  absent-version, i.e. repair-only. Additionally, the documented live path ("re-pushed live via M4
  `config_update`") does not exist: `emitConfigUpdate` (src/tasks/orchestrator.ts:209) has no production
  caller, and `PUT /admin/config/entries` only persists rows.
- Failure scenario: admin uploads a new global `CLAUDE.md` and calls `PUT /admin/config/entries
  {scope:'global', ...}`. The rows persist, but no `config_update` is emitted and the next
  register/`GET /agent/config` response carries no version. A spec-compliant agent classifies the pull as
  absent-version → repair-only → the new instruction file and skills are never materialized on any agent,
  silently and indefinitely. The host's core promise ("single source of truth for all agent config") is
  broken in the default flow.
- Suggested fix direction: persist a monotonic global config version (e.g. a `router_state` counter or a
  `config_meta` row bumped inside `setEntries` for the global scope), include it in the register/config
  payload, and emit a `config_update` carrying the new version + entries to every online agent when the
  global scope changes.

### [P1] `setEntries` delete-then-insert is not transactional — a scope's entire desired set can be wiped

- Location: src/configstore/entries.ts:113-123
- Description: the REPLACE is two separate statements (`DELETE` then `INSERT`) with no wrapping
  transaction. Both run synchronously so no other request interleaves in-process, but the pair is not
  atomic against a crash or an insert-time error: after the delete commits, any failure before/during the
  insert leaves the scope durably empty.
- Failure scenario: host process is killed (OOM, deploy restart) between the delete and the insert of a
  `PUT /admin/config/entries {scope:'global'}` call. On restart the global desired set is empty; agents
  pulling config receive an empty entry list, and (per agent-architecture §9b) a versioned-empty set is
  authoritative-empty — agents would deprovision all global instructions/skills. The admin gets no error;
  the loss is discovered only when agents run without their config.
- Suggested fix direction: wrap the delete+insert in a single SQLite transaction (drizzle `db.transaction`
  or `BEGIN IMMEDIATE`), so REPLACE is all-or-nothing.

### [P1] Global entries are filtered to the agent's primary engine only — multi-engine agents lose their secondary engine's config

- Location: src/configstore/entries.ts:164-171, with src/edge/agent.ts:57-59 (`primaryEngine` = `engines[0]`)
- Description: `docs/host-architecture.md` §6 (data model notes) states "a multi-engine standard lands as
  both a `CLAUDE.md` and an `AGENTS.md` row; **an agent lands all targets and its engine ignores the
  irrelevant file**". The code deviates: `resolveEntriesForEngine` drops every row owned by another engine
  (`targetEngine(r.target) !== engine`), and the edge resolves the global scope for the *first* declared
  capability only. An agent declaring `engines: ["claude", "codex"]` therefore never receives global
  `AGENTS.md` / `.codex/skills/*` rows.
- Failure scenario: a multi-engine agent (capabilities `["claude","codex"]`) is placed a task with
  `engine: "codex"`. Task-scope entries resolve for codex correctly (orchestrator uses `task.engine`), but
  the global scope was delivered claude-only — the codex run executes without the globally declared
  `AGENTS.md` instructions and codex skills, silently diverging from the declared desired state.
- Suggested fix direction: deliver the union of the agent's declared engines (or all rows, per the doc)
  for the global scope instead of `engines[0]`; alternatively update the doc if per-engine filtering is
  intended, and derive the filter from the full capability list.

### [P2] Blob write dedup trusts a 48-bit path prefix without re-verifying existing bytes — a corrupted or prefix-colliding blob is permanently unserveable with no self-heal

- Location: src/configstore/store.ts:87-92 (`put` skips when `existsSync(path)`)
- Description: the content address on disk is `store/<name>/<sha256[:12]>` (48 bits). `put()` computes the
  full sha256 of the incoming bytes but, when the leaf path already exists, silently skips the write
  without verifying that the existing file's content hashes to the same value. Serving does verify
  (`readVerified`, store.ts:114-121), so wrong bytes never leave the host — but that turns any on-disk
  mismatch into a permanent 409, because every re-upload of the correct bytes is a dedup no-op. This is
  the host-side mirror of agent-audit finding P2-17 (same 12-hex short-id reuse without full-hash
  re-verification).
- Failure scenario: (a) adversarial — an agent (connect-token write access via `POST /agent/attachments`)
  who can predict a future artifact's public content computes a second payload whose sha256 shares the
  first 12 hex chars (~2^48 work) and uploads it under the expected name first; when the admin later
  ingests the real artifact, `put()` no-ops, `config_artifacts` registers `(name, realSha)` with a
  `store_ref` pointing at the attacker's bytes, and every agent fetch of that skill 409s forever.
  (b) non-adversarial — bit rot or manual tampering of a stored file: same permanent-409 outcome, and
  re-ingesting the correct artifact cannot repair it.
- Suggested fix direction: on the exists-path, verify the existing file's full sha256; on mismatch,
  rewrite it (temp+rename) instead of no-opping. Optionally use the full sha256 (or a longer prefix) as
  the leaf name.

### [P2] GC roots only on `config_artifacts` rows — orphan blobs, temp files, and all attachment-store blobs are unreclaimable

- Location: src/configstore/gc.ts:51-61 (`findDroppable` iterates `configArtifacts` only), src/configstore/artifacts.ts:32-45, src/configstore/store.ts:89
- Description: the droppable candidate set is derived exclusively from `config_artifacts` rows. Three
  classes of on-disk bytes therefore can never be reclaimed: (a) blobs orphaned by a crash between
  `store.put` and the DB insert (`ingestArtifact` and the edge attachment-ingest are both
  non-transactional across FS+DB); (b) `.tmp-*` files left behind when `writeFileSync` fails mid-write;
  (c) every attachment blob — attachments get no `config_artifacts` row, and `attachments` descriptor rows
  are never deleted, so the attachment half of the "unified store" has no reclamation path at all. This
  contradicts host-architecture §M8: "the same GC that backs the attachment store".
- Failure scenario: a long-running host accumulates attachment blobs for every task ever run plus any
  crash-orphaned blobs; even with the sweep enabled and all references gone, `STORE_DIR` grows without
  bound until the disk fills.
- Suggested fix direction: make the sweep filesystem-aware — walk `STORE_DIR`, treat any leaf not
  referenced by `config_artifacts.store_ref` ∪ `attachments.ref` (plus an age guard) as droppable, and
  clean stale `.tmp-*` files. Define a retention/deletion story for `attachments` rows.

### [P2] GC ages by `created_at` with no grace after un-reference, and in-flight references are not roots

- Location: src/configstore/gc.ts:59 (`now - a.createdAt >= maxAgeMs`)
- Description: the doc frames droppability as "unreferenced … AND aged out (LRU)", but the age test uses
  creation time, not time-since-unreferenced (nothing records when the last reference disappeared). Any
  artifact older than 7 days becomes droppable the *instant* its last `config_entries`/`attachments` row
  goes away. Additionally, references held only in flight — e.g. unacked `task_upsert` outbox rows whose
  `data.entries` name a sha — are not part of `referencedShas`.
- Failure scenario: admin swaps the global `CLAUDE.md` entry from shaA (ingested months ago) to shaB; an
  enabled sweep runs a minute later and deletes shaA's blob + row. The admin rolls back to shaA (or an
  offline agent reconnects and replays an outbox `task_upsert` still naming shaA): the desired set now
  references bytes the host no longer holds; agent fetches 404 and provisioning wedges. Impact is gated on
  the sweep being explicitly enabled (OD11), but the enabled mode is unsafe as designed.
- Suggested fix direction: track an `unreferenced_since` timestamp (set when the sweep first observes
  refcount 0, cleared when re-referenced) and age against that; include unacked outbox entry payloads (or
  simply all outbox rows' entry shas) in the live set.

### [P2] Entries accept unvalidated `sha256` and `scope` — the desired set can declare bytes the host does not hold

- Location: src/configstore/entries.ts:113-123 (`setEntries`), admin surface src/edge/admin/config.ts:27-37 (zod `min(1)` only)
- Description: `setEntries` persists whatever it is given: `sha256` is not checked against
  `config_artifacts` (nor even for 64-hex shape), and a task `scope` is not checked to be an existing task
  id. Two live consequences: (a) an entry can reference a sha the store has never held (typo, or
  `PUT /entries` ordered before `POST /artifacts`) — delivered to agents, whose byte fetch then 404s;
  (b) a sha that exists only as an *attachment* blob is in the desired set but `serveBySha`
  (src/edge/agent.ts:118) grants desired-set authorization only when a `config_artifacts` row exists
  (`artifact != null && isShaInAgentDesiredSet(...)`), so the agent is 403'd on bytes the host both holds
  and declared for it.
- Failure scenario: admin scripts the provisioning flow and calls `PUT /admin/config/entries` with a sha
  from the wrong environment. The call returns 200 and logs "config entries set"; every agent's reconcile
  then fails on a 404 byte fetch, with no host-side signal that the desired set is unsatisfiable.
- Suggested fix direction: in `setEntries` (or the admin handler), reject entries whose `sha256` has no
  `config_artifacts` row (and validate 64-hex shape + task-scope existence), returning a 422 that names
  the missing artifacts.

### [P2] Entry `name`/`target` are persisted and delivered verbatim — traversal-shaped landing paths reach every agent

- Location: src/configstore/entries.ts:56-65 (`resolveTarget` honors explicit `target` verbatim; `dir` builds `<skillDir>/<name>`)
- Description: `assertSafeName` guards only the *store* path segment inside `BlobStore.put`. Nothing in M8
  validates the provisioning fields the agent will materialize: an explicit `target` of
  `../../../home/user/.bashrc` (or a `name` of `../..` for a `kind:dir` entry, yielding
  `.claude/skills/../..`) is accepted by the zod schema (`min(1)` only), persisted, and delivered in
  `config.entries`/`task_upsert.data.entries`. The host — the self-declared single source of truth —
  relies entirely on agent-side path containment for what lands where on every enrolled machine.
- Failure scenario: a compromised or careless channel-token holder sets a global entry with a traversal
  `target`; every agent whose reconciler does not independently reject absolute/`..` targets writes
  host-declared bytes outside its project directory (config poisoning → code execution on the agent
  host).
- Suggested fix direction: validate `name` with `assertSafeName` and `target` as a normalized relative
  path (no leading `/`, no `..` segments) inside `resolveEntries`/`setEntries`, so unsafe landing paths
  are rejected at the M8 boundary regardless of the admin surface used.

### [P2] `BlobStore` never enforces root containment of a `ref` — latent path traversal on the serve path

- Location: src/configstore/store.ts:72-74 (`abs` = `join(root, ref)`), consumed via src/configstore/serve.ts:55-80
- Description: `read`/`readVerified`/`delete`/`sizeOf` join a caller-supplied `ref` onto the store root
  with no check that the result stays under `root`. Today this is unreachable from untrusted input only
  by accident: the sole writer of `attachments.ref` is the edge upload path (host-generated
  `blob.ref`). But both protocol §5 and `src/msgstore/attachments.ts` document agent-supplied descriptors
  (`thread_result.data.attachments`, carrying a free-form `ref`) being persisted to `attachments` — the
  moment that documented wiring lands (the inbound handler currently drops descriptors), an agent-chosen
  `ref` like `../../data/gino.db` flows straight into `join(root, ref)` on serve, and the sha comparison
  in `readVerified` becomes a content-confirmation oracle (200 iff the attacker-declared sha matches the
  target file).
- Failure scenario: after the M5 descriptor-persistence gap is fixed as documented, a compromised agent
  reports a descriptor `{ref:"../../data/gino.db", sha256:<guess>}` on its own task, then fetches it by
  id (placement-authorized automatically) — confirming or exfiltrating host files outside the store.
- Suggested fix direction: in `abs()`, resolve the joined path and assert it is strictly inside `root`
  (and/or validate `ref` as `<safe-name>/<12-hex>` before any FS access), throwing `ConfigStoreError`
  otherwise.

### [P2] No lifecycle cleanup of task-scope entries — orphaned scopes pin artifacts forever

- Location: src/configstore/entries.ts:113 (only same-scope REPLACE deletes rows), src/configstore/gc.ts:29-38
- Description: the only deletion path for `config_entries` rows is an explicit same-scope `setEntries`
  call. The host has no task-deletion flow (the `task_delete` frame is defined but never emitted, and no
  admin route removes task rows), and agent removal deliberately keeps orphaned tasks as history — but
  their config entries remain in the *desired set* table, not the history mirror. `referencedShas` roots
  on all `config_entries` rows, so every artifact ever referenced by any past task stays pinned; GC can
  never converge. This is the host-side twin of agent-audit P1-11 (provisioning rows that outlive their
  scope defeat the GC).
- Failure scenario: a year of task turnover leaves thousands of `config_entries` rows for closed/orphaned
  tasks; the sweep (even enabled) reports droppable=0 for artifacts that no live task uses, and the store
  grows monotonically.
- Suggested fix direction: define the entry lifecycle — clear `config_entries WHERE scope = task_id` when
  a task reaches a terminal status or its agent is removed (or exclude entries of terminal/orphaned tasks
  from `referencedShas` so they age out normally).

### [P3] The GC sweep has no production caller — documented reclamation is unreachable

- Location: src/configstore/gc.ts:83 (`sweep` referenced only by tests)
- Description: host-architecture §M8 step 5 documents store GC as part of the end-to-end flow, and OD11
  frames it as "disabled by default" — but there is no admin endpoint, timer, or CLI that invokes
  `sweep`/`findDroppable` at all, so even deliberate, guarded eviction is impossible without new code.
- Failure scenario: operator needs to reclaim store space, finds no supported control surface; the only
  option is manual FS/DB surgery, which the module's invariants (registry↔blob consistency) make risky.
- Suggested fix direction: expose the sweep behind an explicit admin route (e.g.
  `POST /admin/config/gc {enabled}`) returning the `SweepResult`, keeping the OD11 dry-run default.

### [P3] Mid-sweep crash leaves a dangling registry row, and a missing blob serves as an unhandled 500

- Location: src/configstore/gc.ts:90-96 (blob delete then row delete, no transaction), src/configstore/store.ts:101-107
- Description: per-artifact eviction is blob-first-then-row (the safe order — the reverse would orphan
  blobs invisibly), but a crash between the two leaves a `config_artifacts` row whose `store_ref` no
  longer exists. Serving that sha then throws `ConfigStoreError("missing blob")` from `read()`, which the
  edge maps to a generic 500 (`serveBlob` handles only `ShaMismatchError`), not a 404/410. The dangling
  row self-heals only on the *next explicitly enabled* sweep, which may never run.
- Failure scenario: host restarts during an enabled sweep; an agent fetching a sha whose blob was deleted
  but whose row survived receives 500s (surfacing as host errors in monitoring) instead of a clean
  not-found, until an operator re-runs an enabled sweep.
- Suggested fix direction: delete each blob+row pair atomically from the caller's perspective (row delete
  committed immediately after the blob delete, or a two-phase mark-then-delete), and have the serve path
  map a missing blob for a known row to a 404/410 with a loud integrity log.

### [P3] Re-ingesting an existing `(name, sha256)` with a different `kind`/`content_type` is silently ignored

- Location: src/configstore/artifacts.ts:43 (`onConflictDoNothing`)
- Description: the registry PK is `(name, sha256)`; `kind` and `content_type` are metadata outside the
  content address. On conflict the insert is dropped entirely, so an ingest that corrects a wrong `kind`
  (e.g. a skill first uploaded as `kind:file`) returns 201 with the *old* row — the caller's requested
  metadata is discarded without any signal.
- Failure scenario: admin uploads a skill bundle with `?kind=file` by mistake, notices, and re-uploads
  identical bytes with `?kind=dir`. The response looks successful but still says `kind:file`; entries
  built on it make the agent materialize the bundle as a single file. The only workaround is changing the
  bytes (a new sha).
- Suggested fix direction: on conflict, either update the mutable metadata columns
  (`onConflictDoUpdate` on kind/content_type) or reject with an explicit "metadata differs from the
  registered artifact" error.

### [P3] Byte channel sets no caching headers — no `Cache-Control`, no `ETag`

- Location: src/configstore/serve.ts:55 (resolver output), headers assembled at src/edge/agent.ts:141-148
- Description: blob responses carry only `content-type`/`content-length`/`x-content-sha256`. Two sides:
  (a) hardening — authentication rides the non-standard `x-internal-token` header, which (unlike
  `Authorization`) does not inhibit shared caches; a misconfigured intermediary may heuristically cache an
  authenticated 200 and replay it without auth. (b) correctness/efficiency — the bytes are
  content-addressed and immutable, the ideal case for `Cache-Control: private, immutable` + an
  `ETag: "<sha256>"`, yet every re-fetch ships the full body.
- Failure scenario: a deployment fronts the host with a caching reverse proxy using default heuristics; a
  config blob fetched by agent A is served from cache to any client hitting the same URL, bypassing the
  desired-set/placement authorization.
- Suggested fix direction: set explicit `Cache-Control: private, no-store` (or `private, immutable` given
  the content addressing) plus `ETag`/`If-None-Match` handling on the serve path.

Section summary: P0=1 P1=2 P2=7 P3=4
