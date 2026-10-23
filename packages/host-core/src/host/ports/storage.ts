// HOST PORTS — the Storage port (host-sdk-design §8.3).
//
// One repository per table group; each method is ONE storage intent (one SQL
// statement in the sqlite adapter). The port stores and queries rows — it never
// hashes tokens, mints ids, resolves engine targets, or decides status policy;
// that stays in the domain. The invariants the runner's correctness depends on
// (monotonic outbox ids, `(threadId, seq)` first-writer-wins, compare-and-set
// status transitions, set-once placement, atomic `replaceScope`, verified blob
// reads, …) are stated on the method that carries them and enforced by the
// adapter-parameterized conformance suite (`tests/conformance/`).
//
// Timestamp rule (every adapter): an INSERT fills `createdAt` / `updatedAt` /
// other defaulted columns from `Date.now()` when the input omits them (the
// schema `$defaultFn`); every UPDATE that stamps a timestamp takes `now`
// EXPLICITLY from the caller (the domain owns the clock seam). No method reads
// the clock for an update on its own.
//
// Ordering rule: "newest first" means `createdAt` descending (or `updatedAt`
// descending where stated); rows with an equal timestamp keep insertion order.
// Every method that returns a list states its order.
//
// Deliberately NOT here: a generic `transaction()`. The only cross-row atomic
// operation the domain needs is `ConfigRepo.replaceScope`; a future need for
// atomicity becomes another named method, not a leak. All methods are
// synchronous in P1 (the domain is synchronous today).
import type {
  AgentCapabilitiesJson,
  AgentGrantRow,
  AgentRow,
  AttachmentRow,
  ChannelBindingRow,
  ChannelRow,
  ConfigArtifactRow,
  ConfigEntryInput,
  ConfigEntryRow,
  CronRow,
  MessageRow,
  NewAgentRow,
  NewAttachmentRow,
  NewChannelBindingRow,
  NewChannelRow,
  NewConfigArtifactRow,
  NewMessageRow,
  NewOutboxRow,
  NewPendingLaunchRow,
  NewTaskRow,
  NewThreadRunRow,
  OutboxRow,
  PendingLaunchRow,
  TaskRow,
  ThreadRunRow,
} from "./rows";

export type * from "./rows";

// ─── AgentRepo (M3: agents + pending_launches + agent_grants) ───

/** Compare-and-set guard for `AgentRepo.updateStatusIf`. */
/**
 * Compare-and-set guard for `AgentRepo.updateStatusIf`. Give `from` for "only
 * when it is exactly this", `notFrom` for "anything but this"; both may be
 * omitted, which still refuses to move a `removed` agent.
 */
export interface StatusGuard {
  /** Transition only when the current status equals `from`. */
  readonly from?: string;
  /** Transition only when the current status differs from `notFrom`. */
  readonly notFrom?: string;
}

/**
 * Agent identity, liveness, access grants and launch correlation.
 *
 * The invariant an adapter MUST keep: status transitions are COMPARE-AND-SET
 * and `removed` is terminal, so a revoked agent can never come back online.
 * `launchNonce` is unique across agents, and token lookups never return a
 * removed agent. The repository stores hashes it is handed — it never hashes,
 * mints or decides policy itself.
 */
export interface AgentRepo {
  /**
   * Persist a new agent row (`registry.enrollAgent`). `createdAt` defaults to
   * `Date.now()`, `activeThreads` to `0` when omitted. Returns the stored row.
   * Throws an `Error` whose message contains `UNIQUE` on a duplicate `id` or a
   * duplicate non-null `launchNonce`.
   */
  insert: (row: NewAgentRow) => AgentRow;
  get: (agentId: string) => AgentRow | undefined;
  /** Every agent (including `removed`), newest first (`createdAt` desc; ties in insertion order). */
  list: () => AgentRow[];
  /**
   * The agent whose `connectTokenHash` equals `hash`, EXCLUDING `removed`
   * agents (their token is revoked) — the token→identity resolver for
   * the connect-token gate and the M7 proxy credential lookup. The caller
   * hashes the presented token; the port never sees plaintext.
   */
  getByConnectTokenHash: (hash: string) => AgentRow | undefined;
  /**
   * The non-`removed` agent bound to a launch nonce (`reEnrollByLaunchNonce`).
   * A `removed` agent is never resurrected → `undefined`.
   */
  getByLaunchNonce: (nonce: string) => AgentRow | undefined;
  /**
   * COMPARE-AND-SET status transition. Returns `true` iff exactly this agent
   * was transitioned. It returns `false` and leaves the row UNCHANGED when the
   * agent is unknown, when the guard does not hold, or — regardless of the
   * guard — when the current status is `removed` (`removed` is TERMINAL: the
   * only way in is `setRemoved`, and nothing leads out). Today's callers:
   *   • `setAgentStatus`  → `{ notFrom: "removed" }`
   *   • `touchLiveness`   → `{ from: "offline" }` to `online`
   */
  updateStatusIf: (agentId: string, guard: StatusGuard, to: string) => boolean;
  /**
   * Liveness touch (`heartbeat` / any inbound frame): set `lastSeen = now` and,
   * when given, `activeThreads`. Never touches `status` (the offline→online
   * restore is a separate `updateStatusIf`). No-op for an unknown agent.
   */
  touchLiveness: (agentId: string, now: number, activeThreads?: number) => void;
  /**
   * Set the access policy. Skipped (no-op) when the agent is `removed`
   * (mirrors `WHERE status != 'removed'`).
   */
  setAccess: (agentId: string, access: string) => void;
  /** Replace the stored capabilities. Skipped (no-op) when the agent is `removed` or unknown. */
  setCapabilities: (agentId: string, capabilities: AgentCapabilitiesJson) => void;
  /**
   * Permanent removal bookkeeping (`registry.removeAgent`): flip
   * `status` to `removed` AND overwrite `connectTokenHash` with the caller's
   * revoked (unmatchable) hash, unconditionally — repeating it on a removed
   * agent re-applies the same state. Orphaning tasks is `TaskRepo.orphanByAgent`.
   */
  setRemoved: (agentId: string, revokedTokenHash: string) => void;
  /**
   * Rotate the connect token hash (`reEnrollByLaunchNonce`). Returns the
   * updated row, or `undefined` for an unknown agent.
   */
  setConnectTokenHash: (agentId: string, hash: string) => AgentRow | undefined;
  /**
   * Offline-grace sweep (`liveness.sweepLiveness`): flip EVERY agent that
   * is `online` AND has a non-null `lastSeen < before` to `offline`, in one
   * pass. Agents that never connected (`lastSeen` null), already `offline`,
   * `enrolled`, or `removed` are untouched. Returns the transitioned ids in
   * insertion order.
   */
  sweepSilent: (before: number) => string[];

  // grants

  /** Grant an owner access — idempotent on `(agentId, ownerId)`; `createdAt` defaults to `Date.now()`. */
  grant: (agentId: string, ownerId: string) => void;
  /** Revoke an owner's grant — idempotent. */
  revoke: (agentId: string, ownerId: string) => void;
  /** Grants on an agent, in insertion order. */
  listGrants: (agentId: string) => AgentGrantRow[];
  /** Whether a grant row exists (the `dedicated` half of `isOwnerPermitted`; the `public` short-circuit stays in the domain). */
  hasGrant: (agentId: string, ownerId: string) => boolean;

  // launch↔register correlation

  /**
   * Record a pending launch keyed by its nonce. `access` defaults to
   * `dedicated`, `createdAt` to `Date.now()`. `nonce` is UNIQUE: a second
   * record with the same nonce throws an `Error` whose message contains
   * `UNIQUE` and changes nothing.
   */
  recordPending: (row: NewPendingLaunchRow) => void;
  /**
   * The pending→agent move (`registry.correlateLaunch`): CONSUME the pending
   * launch for `nonce` (delete it) and bind its `launcherKind` / `launchRef` /
   * `access` plus the nonce itself onto the agent row. Returns the consumed
   * pending row, or `undefined` (nothing changed) when no launch is pending
   * for `nonce`. `launchNonce` is UNIQUE across agents: when another agent
   * already carries `nonce` the call throws an `Error` whose message contains
   * `UNIQUE` and leaves the pending row in place.
   */
  correlate: (agentId: string, nonce: string) => PendingLaunchRow | undefined;
  hasPending: (nonce: string) => boolean;
  /** Pending launches with `createdAt < before` (the TTL reaper's input), in insertion order. */
  listExpiredPending: (before: number) => PendingLaunchRow[];
  /** Drop a pending launch — idempotent. */
  deletePending: (nonce: string) => void;
}

// ─── TaskRepo (M4: tasks) ───

/** Equality filter for `TaskRepo.list`; omitted keys are unconstrained. */
export interface TaskListFilter {
  readonly ownerId?: string;
  readonly agentId?: string;
  readonly status?: string;
}

/**
 * Task rows, their placement and their lifecycle columns.
 *
 * The invariant an adapter MUST keep: placement is SET-ONCE — `setPlacementOnce`
 * is a compare-and-set on `agentId IS NULL` and returns `false` without
 * changing anything once a task is placed, including on the same agent. That
 * is what makes one-task-one-agent hold; the domain decides whether a `false`
 * means "idempotent" or "conflict".
 */
export interface TaskRepo {
  /** Persist a new task (`createTask`). `createdAt`/`updatedAt` default to `Date.now()`. Throws `UNIQUE` on a duplicate id. */
  insert: (row: NewTaskRow) => TaskRow;
  get: (taskId: string) => TaskRow | undefined;
  /** Tasks matching every given filter key (equality), newest first (`createdAt` desc; ties in insertion order). */
  list: (filter?: TaskListFilter) => TaskRow[];
  /**
   * SET-ONCE placement (§3 no-migration): compare-and-set on `agentId IS
   * NULL`. On success it stores `agentId`, `assignedAt = now`, resets the
   * placement-scoped state (`seededAt = null`, `currentThreadId = null`),
   * stamps `updatedAt = now`, and returns `true`. When the task is unknown or
   * already placed (on ANY agent, including the same one) it returns `false`
   * and changes nothing — the idempotent-same-agent / 409-different-agent
   * decision is the domain's (`tasks/store.setPlacement`).
   */
  setPlacementOnce: (taskId: string, agentId: string, now: number) => boolean;
  /** Set `status` + `updatedAt = now`; returns the updated row or `undefined` for an unknown task. */
  setStatus: (taskId: string, status: string, now: number) => TaskRow | undefined;
  /** Set `currentThreadId` + `updatedAt = now`; returns the updated row or `undefined` for an unknown task. */
  setCurrentThread: (taskId: string, threadId: string, now: number) => TaskRow | undefined;
  /** Set `seededAt = now` ONLY when it is still null (set once; re-placement clears it via `setPlacementOnce`). */
  markSeeded: (taskId: string, now: number) => void;
  /**
   * Agent removal: clear `agentId` on every task placed on `agentId`
   * (rows are kept as history), stamping `updatedAt = now`. Returns the
   * orphaned task ids in insertion order.
   */
  orphanByAgent: (agentId: string, now: number) => string[];
}

// ─── OutboxRepo (M2: outbox) ───

/** Type guard for `OutboxRepo.markAcked`. */
export interface MarkAckedOptions {
  /**
   * Refuse to ack a row of this `type` (the `ack{id}` frame never acks a
   * `message` row — only the durable-storage `msg_ack` does, protocol §4.2).
   * Omit for `msg_ack`, which acks any type.
   */
  readonly excludeType?: string;
}

/**
 * The durable host→agent queue behind at-least-once delivery.
 *
 * The invariant an adapter MUST keep: `append` returns a STRICTLY MONOTONIC,
 * globally unique `id` that is never reused, because that id becomes the wire
 * `msgId` the runner dedups on. Replay reads unacked rows in ascending id
 * order, so ordering follows from the same sequence. SQLite gets this from
 * AUTOINCREMENT; a non-SQL backend needs one global increasing sequence, not a
 * per-agent counter.
 */
export interface OutboxRepo {
  /**
   * Persist a host→agent frame and return the stored row INCLUDING its `id`.
   * `id` is STORAGE-GENERATED and STRICTLY MONOTONIC across the whole table
   * (sqlite: AUTOINCREMENT; other backends: one global increasing sequence —
   * a per-agent counter is NOT enough, ids must be unique across agents):
   * every later `append`, for any agent, gets a larger id, and an id is never
   * reused after a delete. The id becomes the wire `outboxId` / `msgId` the
   * runner dedups on and replays in ascending order. `createdAt` defaults to
   * `Date.now()`, `ackedAt` to `null`.
   */
  append: (row: NewOutboxRow) => OutboxRow;
  /** Unacked rows (`ackedAt IS NULL`) of ONE agent, ascending by `id` (replay order). */
  listUnacked: (agentId: string) => OutboxRow[];
  /** Every unacked row across all agents, ascending by `id` (the GC root-set scan). */
  listAllUnacked: () => OutboxRow[];
  /**
   * Set `ackedAt = now` on row `id` — SCOPED to `agentId`'s own rows (ids are
   * a global sequence, so a foreign id is guessable; a foreign agent's ack is
   * a no-op) and, with `excludeType`, to rows of a different type. Acking an
   * already-acked row re-stamps it. Returns `true` iff a row was updated.
   */
  markAcked: (agentId: string, id: number, now: number, opts?: MarkAckedOptions) => boolean;
  /** Delete an agent's UNACKED rows only; acked history stays. Returns the count deleted. */
  purge: (agentId: string) => number;
  /** Retention GC: delete rows that are ACKED with `ackedAt < before`; unacked rows are NEVER deleted. Returns the count. */
  gc: (before: number) => number;
}

// ─── TranscriptRepo (M5: messages) ───

/**
 * Cursor + cap for `TranscriptRepo.read` — the RESOLVED form, not the caller's
 * options. The domain (`getTranscript`, `host.transcript.get`) defaults and
 * clamps the cap before it reaches the port, so `limit` is required here and an
 * adapter applies it verbatim. The caller-facing shape, with `limit` optional,
 * is `TranscriptReadOptions`.
 */
export interface TranscriptReadQuery {
  /** Only rows with `seq > afterSeq`. */
  readonly afterSeq?: number;
  /** Row cap (already clamped by the domain; the port applies it verbatim). */
  readonly limit: number;
}

/** Cheap completeness probe for a thread. */
export interface TranscriptStats {
  messageCount: number;
  /** `max(seq)`, or null when the thread has no rows. */
  lastSeq: number | null;
}

/** A thread's first appearance in the transcript (`min(createdAt)` over its rows). */
export interface ThreadFirstSeen {
  threadId: string;
  firstAt: number;
}

/**
 * The transcript — the durable record the SDK's event hints point at.
 *
 * The invariants an adapter MUST keep: `(threadId, seq)` is unique and the
 * FIRST writer wins, so a redelivered frame is silently dropped rather than
 * overwriting; `read` orders by `seq` and never by arrival; and `id` is
 * storage-generated and monotonic, because it is the channels egress cursor.
 */
export interface TranscriptRepo {
  /**
   * Persist one transcript event, IDEMPOTENT on `(threadId, seq)`: the FIRST
   * writer wins and its row is retained unchanged; a later write for the same
   * key is ignored and returns `false` (never overwrites, never throws). `id`
   * is storage-generated and strictly monotonic (the M6 egress cursor key).
   * `createdAt` defaults to `Date.now()`.
   */
  insertIfAbsent: (row: NewMessageRow) => boolean;
  /** A thread's rows ORDERED BY `seq` (protocol §6 — never arrival), after `afterSeq` when given, capped at `limit`. */
  read: (threadId: string, opts: TranscriptReadQuery) => MessageRow[];
  /** `count(*)` + `max(seq)` for a thread (`getTranscriptStats` / `checkTranscriptCompleteness`). */
  stats: (threadId: string) => TranscriptStats;
  /** Distinct threads of a task with their earliest `createdAt`, ordered by that time (ties in first-insertion order). */
  threads: (taskId: string) => ThreadFirstSeen[];
  /** The `taskId` of any stored row of a thread, or `undefined` when the thread has no transcript rows. */
  taskOf: (threadId: string) => string | undefined;
  /**
   * Egress tail window (`channels/egress.tailTask`): a task's rows with
   * `id > afterId`, ORDERED BY `id` ascending, capped at `limit` — the lowest
   * `limit` ids past the cursor, so a capped batch is contiguous.
   */
  readAfterId: (taskId: string, afterId: number, limit: number) => MessageRow[];
}

// ─── ThreadRunRepo (M5: thread_runs) ───

/**
 * The per-thread terminal-outcome mirror.
 *
 * The invariant an adapter MUST keep: `upsert` inserts the whole row once, then
 * updates ONLY the mutable subset. `taskId`, `cronId`, `source` and the
 * timestamps are set once, so replaying an identical result is a no-op.
 */
export interface ThreadRunRepo {
  /**
   * UPSERT keyed by `threadId`. The FIRST call inserts the whole row
   * (`createdAt` defaults to `Date.now()`). Every later call updates ONLY the
   * terminal mirror fields — `status`, `failureReason`, `engineSessionId`,
   * `durationMs`, `finalSeq` — and leaves `taskId`, `cronId`, `source`,
   * `startedAt`, `finishedAt`, `createdAt` exactly as first written (an
   * identical replay is a no-op).
   */
  upsert: (row: NewThreadRunRow) => void;
  get: (threadId: string) => ThreadRunRow | undefined;
  /** Runs of a task, in insertion order. */
  listByTask: (taskId: string) => ThreadRunRow[];
}

// ─── CronMirrorRepo (M5: crons) ───

/** The `cron_state` mirror fields (`msgstore/crons.writeCronState`), with `nextRun` already coerced to epoch-ms by the domain. */
/** The mirror fields of a `cron_state` frame, with `nextRun` already epoch-ms. */
export interface CronStateUpsert {
  readonly id: string;
  readonly taskId: string;
  readonly kind: string;
  readonly scheduleType: string | null;
  readonly scheduleValue: string | null;
  readonly nextRun: number | null;
  readonly status: string | null;
  readonly lastStatus: string | null;
  readonly roundCount: number | null;
  readonly skippedCount: number | null;
  readonly updatedAt: number;
  /** Written only when present (the wire carries it optionally); an insert without it stores `""`. */
  readonly continuity?: string;
}

/** The dispatch-owned half of a cron row: the recurring prompt the frame never echoes. */
export interface CronPromptUpsert {
  readonly id: string;
  readonly taskId: string;
  readonly prompt: string;
  readonly updatedAt: number;
}

/**
 * The read-only cron mirror. Crons live on the agent; nothing here schedules.
 *
 * The invariant an adapter MUST keep: the two upserts own DISJOINT column sets.
 * A state upsert never touches `prompt` or `taskId`, and a prompt upsert never
 * touches the mirror fields, so the two can arrive in either order without
 * either erasing the other.
 */
export interface CronMirrorRepo {
  /**
   * Mirror a `cron_state`, UPSERT keyed by `id`. INSERT on first sight (with
   * `prompt = ""` and `continuity = input.continuity ?? ""`); on conflict
   * overwrite the mirror fields (`kind`, `scheduleType`, `scheduleValue`,
   * `nextRun`, `status`, `lastStatus`, `roundCount`, `skippedCount`,
   * `updatedAt`) and `continuity` ONLY when the input carries it. `prompt`
   * and `taskId` are PRESERVED (never in the update set).
   */
  upsertState: (input: CronStateUpsert) => void;
  /**
   * Persist the dispatch-owned `prompt`, UPSERT keyed by `id`.
   * INSERT a stub (`kind = ""`, `continuity = ""`, every other mirror column
   * null/default) when the command precedes the first `cron_state`; on
   * conflict update ONLY `prompt` + `updatedAt` — every mirror field and
   * `taskId` are preserved.
   */
  upsertPrompt: (input: CronPromptUpsert) => void;
  get: (cronId: string) => CronRow | undefined;
  /** A task's mirrored crons, most recently updated first (`updatedAt` desc; ties in insertion order). */
  listByTask: (taskId: string) => CronRow[];
}

// ─── AttachmentRepo (M5/M8: attachments) ───

/**
 * Attachment descriptors — metadata only; the bytes live in `BlobStorePort`.
 *
 * The invariant an adapter MUST keep: `insertIfAbsent` is idempotent on `id`,
 * first writer wins, so re-registering a descriptor from a replayed frame
 * neither throws nor overwrites.
 */
export interface AttachmentRepo {
  /** Persist a descriptor, IDEMPOTENT on `id`: first writer wins (row retained), a duplicate returns `false`. `createdAt` defaults to `Date.now()`. */
  insertIfAbsent: (row: NewAttachmentRow) => boolean;
  getById: (id: string) => AttachmentRow | undefined;
  /** Every descriptor carrying `sha256`, in insertion order. */
  listBySha: (sha256: string) => AttachmentRow[];
  /** Every descriptor, in insertion order (the GC root-set scan over `sha256` / `ref`). */
  listAll: () => AttachmentRow[];
}

// ─── ConfigRepo (M8: config_artifacts + config_entries + the global version) ───

/**
 * The artifact index and the per-scope desired sets.
 *
 * The invariants an adapter MUST keep: `(name, sha256)` and `(scope, target)`
 * are unique; and `replaceScope` is the port's ONE atomic operation — it
 * replaces a scope's rows and, for the global scope, bumps the durable version
 * in the same all-or-nothing unit. A partial failure must leave both the old
 * set and the old version intact, because agents reconcile against that
 * version. There is deliberately no generic `transaction()`.
 */
export interface ConfigRepo {
  // artifacts — `(name, sha256)` UNIQUE (content-addressed PK)

  /**
   * Ingest index write (`artifacts.ingestArtifact`): INSERT the row, or on an
   * existing `(name, sha256)` update ONLY `kind` + `contentType`;
   * `size`, `storeRef`, `unreferencedSince`, `createdAt` stay as first
   * written. Returns the stored row.
   */
  upsertArtifact: (row: NewConfigArtifactRow) => ConfigArtifactRow;
  getArtifact: (name: string, sha256: string) => ConfigArtifactRow | undefined;
  /** Any artifact with `sha256`, newest first when several names share it (`createdAt` desc; ties: the latest inserted). */
  getArtifactBySha: (sha256: string) => ConfigArtifactRow | undefined;
  /** Every artifact, newest first (`createdAt` desc; ties in insertion order). */
  listArtifacts: () => ConfigArtifactRow[];
  /** Set the GC aging mark (`null` = referenced again, a timestamp = first observed unreferenced). */
  setUnreferencedSince: (name: string, sha256: string, value: number | null) => void;
  /** Delete one artifact row (GC eviction, row-first). Idempotent. */
  deleteArtifact: (name: string, sha256: string) => void;

  // entries — `(scope, target)` UNIQUE

  /**
   * REPLACE a scope's desired set and return the GLOBAL config version
   * afterwards. ATOMIC, all-or-nothing: the scope's previous rows are deleted
   * and `entries` inserted (with `updatedAt = Date.now()`) in one unit — on
   * ANY failure (e.g. two entries sharing a `target`, which throws an `Error`
   * whose message contains `UNIQUE`) the previous set is intact and the
   * version unchanged. Mirrors `configstore/entries.setEntries`: a write to
   * the `global` scope ALWAYS bumps the version by one in the same unit
   * (`router_state.global_config_version`, so it is restart-durable and
   * strictly increasing); a task-scope write never touches it. An empty
   * `entries` clears the scope. Validation (safe names, known shas, existing
   * task scope) is the domain's job before calling.
   */
  replaceScope: (scope: string, entries: readonly ConfigEntryInput[]) => number;
  /** A scope's rows, in insertion order. */
  entriesFor: (scope: string) => ConfigEntryRow[];
  /** Every scope's rows, in insertion order (the GC root-set scan over `sha256`). */
  listAllEntries: () => ConfigEntryRow[];
  /** Drop a scope's set WITHOUT touching the version (task lifecycle cleanup). Idempotent. */
  clearScope: (scope: string) => void;
  /**
   * The distinct scopes whose desired set names `sha256` (the byte-channel
   * read authorization: the domain answers "global, or a task scope placed on
   * this agent" from these + `TaskRepo.get`). Insertion order.
   */
  scopesWithSha: (sha256: string) => string[];
  /** The monotonic global config version; `0` when the global set was never written. */
  globalVersion: () => number;
}

// ─── BlobStorePort (M8: the content-addressed bytes) ───

/** What a successful `put` stored: the sanitized name, its digest, size and ref. */
export interface StoredBlobRef {
  name: string;
  sha256: string;
  size: number;
  /** Store-relative ref `<name>/<sha256[:12]>` — the value persisted as `storeRef` / `ref`. */
  ref: string;
}

/** One physical leaf in the store (the filesystem-aware GC input). */
export interface BlobLeaf {
  ref: string;
  mtimeMs: number;
  /** True for an in-flight/leftover temp file (never addressable — always junk once aged). */
  isTmp: boolean;
}

/**
 * The byte store. Content-addressed: `ref` is a pure function of
 * `(name, sha256)` (`refFor`), so an identical re-`put` is an idempotent
 * no-op and `readVerified` can refuse to serve corrupt bytes. The filesystem
 * `class BlobStore` (`configstore/store.ts`) implements this structurally.
 */
export interface BlobStorePort {
  /** The deterministic ref for `(name, sha256)`. Throws on an unsafe `name`. */
  refFor: (name: string, sha256: string) => string;
  /**
   * Hash + persist `bytes` under `refFor(name, sha256(bytes))`. Idempotent
   * when the same content is already stored; a leaf holding DIFFERENT bytes
   * is rewritten from the payload (self-heal). Returns the
   * descriptor `{ name, sha256, size, ref }`.
   */
  put: (name: string, bytes: Uint8Array) => StoredBlobRef;
  exists: (ref: string) => boolean;
  /** Raw read — throws `MissingBlobError` when `ref` holds nothing. Prefer `readVerified` for serving. */
  read: (ref: string) => Uint8Array;
  /**
   * Read + verify: throws `MissingBlobError` when absent and
   * `ShaMismatchError` when the stored bytes do not hash to `expected` — a
   * corrupt or tampered blob is NEVER returned.
   */
  readVerified: (ref: string, expected: string) => Uint8Array;
  /** Physical size of a blob, `0` when absent. */
  sizeOf: (ref: string) => number;
  /** Remove a blob — idempotent (absent is fine). */
  delete: (ref: string) => void;
  /** Every physical leaf (blobs and leftover temp files). */
  listLeaves: () => BlobLeaf[];
}

// ─── ChannelRepo (M6: channels + channel_bindings + channel_cursors; optional) ───

/**
 * Channel routing state, used only when `@gino/host-channels` is installed.
 *
 * The invariants an adapter MUST keep: `(channelId, conversationRef)` and
 * `(channelId, taskId)` are unique, and `advanceCursor` is MONOTONIC — a stale
 * or duplicate position is a no-op, so a redelivered batch cannot rewind
 * egress and replay messages that were already handed out.
 */
export interface ChannelRepo {
  /** Persist a channel (`createdAt` defaults to `Date.now()`); throws `UNIQUE` on a duplicate id. */
  insert: (row: NewChannelRow) => ChannelRow;
  get: (channelId: string) => ChannelRow | undefined;
  /** The FIRST channel (insertion order) carrying `name` — names are not unique. */
  getByName: (name: string) => ChannelRow | undefined;
  /** Every channel, newest first (`createdAt` desc; ties in insertion order). */
  list: () => ChannelRow[];
  /** Replace the opaque `config` JSON; returns the updated row or `undefined` for an unknown channel. */
  setConfig: (channelId: string, config: Record<string, unknown> | null) => ChannelRow | undefined;

  // bindings — `(channelId, conversationRef)` UNIQUE

  /** Bind a conversation to a task, IDEMPOTENT on `(channelId, conversationRef)`: first writer wins, a duplicate returns `false`. */
  bindIfAbsent: (row: NewChannelBindingRow) => boolean;
  getBinding: (channelId: string, conversationRef: string) => ChannelBindingRow | undefined;
  /** A channel's bindings, newest first (`createdAt` desc; ties in insertion order). */
  listBindings: (channelId: string) => ChannelBindingRow[];
  /** Whether ANY binding of `channelId` points at `taskId` (the egress IDOR guard's binding half). */
  isTaskBound: (channelId: string, taskId: string) => boolean;

  // cursors — `(channelId, taskId)` PK; the M6 egress high-water mark over `messages.id`

  /** The stored cursor, `0` when none (`messages.id` starts at 1). */
  readCursor: (channelId: string, taskId: string) => number;
  /**
   * MONOTONIC advance (`egress.ackCursor`): store `position` as the
   * `(channelId, taskId)` cursor ONLY when it is greater than the stored
   * value (creating the row when absent), stamping `updatedAt = now`. A
   * stale or duplicate position is a no-op. Returns `true` iff it moved.
   */
  advanceCursor: (channelId: string, taskId: string, position: number, now: number) => boolean;
}

// ─── Storage ───

/**
 * Everything the host persists, as one object of repositories plus the blob
 * store. This is the port an adapter implements; `memoryStorage()` and
 * `@gino/host-store-sqlite` are the two that ship.
 *
 * Write a new adapter against the invariants documented on each repository —
 * they are not inferable from the type signatures — and prove it with the
 * conformance suite exported from `@gino/host-core/testing`, which asserts
 * every one of them against your implementation. Methods are synchronous by
 * design: the whole domain is synchronous.
 */
export interface Storage {
  readonly agents: AgentRepo;
  readonly tasks: TaskRepo;
  readonly outbox: OutboxRepo;
  readonly transcript: TranscriptRepo;
  readonly threadRuns: ThreadRunRepo;
  readonly crons: CronMirrorRepo;
  readonly attachments: AttachmentRepo;
  readonly config: ConfigRepo;
  readonly blobs: BlobStorePort;
  /** Present only when the host runs the channels module. */
  readonly channels?: ChannelRepo;
  /** Health probe (the `SELECT 1` behind `/health` and the CLI check): returns normally, throws when the backend is unusable. */
  ping: () => void;
  /** Release the backend; the storage is unusable afterwards. */
  close: () => void;
  /** Optional durability fold (sqlite WAL checkpoint); absent on backends without one. */
  checkpoint?: () => void;
}
