// M8 — REFCOUNT / GC (the sweep backing BOTH stores; disabled by default).
//
// A blob `(name, sha256)` is droppable when NOTHING references its `sha256` —
// neither a `config_entries` desired-set row, NOR an `attachments` descriptor,
// NOR an in-flight (unacked) outbox frame naming the sha — AND it
// has stayed unreferenced for a grace period. Aging runs from the moment the
// sweep FIRST observed the artifact unreferenced (`unreferenced_since`), never
// from `created_at`, so a just-swapped-out artifact survives a rollback window.
// The sweep is also filesystem-aware: store leaves no DB row
// references (crash orphans, stale `.tmp-*` files) are reclaimed once aged.
// the sweep is a GUARDED no-op by default: it computes the droppable
// set but DELETES NOTHING unless explicitly `enabled` — so normal operation
// never auto-evicts.
import type { Storage } from "../host/ports/storage";
import type { Logger } from "../logger";
import { SHA_PREFIX_LEN } from "./store";

/** Default grace before an unreferenced artifact/orphan leaf is droppable (7 days). */
export const DEFAULT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

/** Default cadence of the periodic GC sweep (ms) — hourly. */
export const CONFIG_GC_INTERVAL_MS = 60 * 60 * 1000;

export interface DroppableArtifact {
  name: string;
  sha256: string;
  storeRef: string;
  createdAt: number;
}

/** Shas named by a frame payload's `entries` array (in-flight desired-set refs). */
function entryShasOf(data: unknown): string[] {
  const entries = (data as { entries?: unknown })?.entries;
  if (!Array.isArray(entries)) {
    return [];
  }
  return entries
    .map(e => (e as { sha256?: unknown })?.sha256)
    .filter((s): s is string => typeof s === "string");
}

/**
 * The LIVE set: every `sha256` referenced by the desired set (`config_entries`),
 * an attachment descriptor (`attachments`), OR an in-flight UNACKED outbox frame
 * whose payload names entries (a replayable `task_upsert` still
 * owed to an offline agent pins its shas). An artifact whose `sha256` is in
 * this set is pinned and never droppable.
 */
export function referencedShas(storage: Storage): Set<string> {
  const live = new Set<string>();
  for (const r of storage.config.listAllEntries()) {
    live.add(r.sha256);
  }
  for (const r of storage.attachments.listAll()) {
    live.add(r.sha256);
  }
  for (const r of storage.outbox.listAllUnacked()) {
    for (const sha of entryShasOf(r.dataJson)) {
      live.add(sha);
    }
  }
  return live;
}

export interface GcOptions {
  /** Wall clock for the age test (defaults to now). */
  now?: number;
  /** Grace after un-reference before an artifact is droppable (defaults to 7d). */
  maxAgeMs?: number;
}

/**
 * Artifacts that are BOTH unreferenced (refcount 0) AND past the un-reference
 * grace — the droppable set. Reconciles the `unreferenced_since` marks as a
 * side effect: a row first observed unreferenced is stamped `now` (aging
 * starts), a re-referenced row is cleared (aging resets) — so droppability
 * always ages from the LAST un-reference, never from `created_at`.
 */
export function findDroppable(storage: Storage, opts: GcOptions = {}): DroppableArtifact[] {
  const now = opts.now ?? Date.now();
  const maxAgeMs = opts.maxAgeMs ?? DEFAULT_MAX_AGE_MS;
  const live = referencedShas(storage);
  const rows = storage.config.listArtifacts();
  const droppable: DroppableArtifact[] = [];
  for (const a of rows) {
    if (live.has(a.sha256)) {
      if (a.unreferencedSince != null) {
        storage.config.setUnreferencedSince(a.name, a.sha256, null);
      }
      continue;
    }
    if (a.unreferencedSince == null) {
      storage.config.setUnreferencedSince(a.name, a.sha256, now);
      continue;
    }
    if (now - a.unreferencedSince >= maxAgeMs) {
      droppable.push({ name: a.name, sha256: a.sha256, storeRef: a.storeRef, createdAt: a.createdAt });
    }
  }
  return droppable;
}

export interface SweepOptions extends GcOptions {
  /**
   * GUARD: when false/omitted the sweep is a DRY RUN — it reports the
   * droppable set but deletes nothing. Only `enabled: true` evicts.
   */
  enabled?: boolean;
}

export interface SweepResult {
  scanned: number;
  droppable: number;
  dropped: number;
  /** Store leaves no DB row references (crash orphans + stale `.tmp-*`), aged out. */
  orphanLeaves: number;
  orphansDropped: number;
}

/** Every store-leaf `ref` a DB row still points at (never an FS orphan). */
function referencedRefs(storage: Storage): Set<string> {
  const refs = new Set<string>();
  for (const r of storage.config.listArtifacts()) {
    refs.add(r.storeRef);
  }
  for (const r of storage.attachments.listAll()) {
    refs.add(r.ref);
  }
  return refs;
}

/**
 * Refcount/LRU sweep over BOTH the registry and the physical store. By default
 * a DRY RUN: it reports the droppable/orphan counts and deletes nothing.
 * With `enabled: true`:
 *
 * - Each droppable artifact is evicted ROW-FIRST, blob second: a
 *   crash between the two leaves an orphan BLOB the next sweep reclaims —
 *   never a dangling registry row that would 404/500 on serve. The blob is
 *   only removed when no surviving row still points at the same leaf.
 * - Aged filesystem orphans — leaves without any `config_artifacts.store_ref`
 *   or `attachments.ref`, plus stale `.tmp-*` files — are reclaimed.
 * A recent leaf (inside the grace window) counts as in-flight
 *   and is left alone, as is any blob whose leaf name matches a live sha.
 */
export function sweep(storage: Storage, opts: SweepOptions = {}): SweepResult {
  const now = opts.now ?? Date.now();
  const maxAgeMs = opts.maxAgeMs ?? DEFAULT_MAX_AGE_MS;
  const store = storage.blobs;
  const scanned = storage.config.listArtifacts().length;
  const droppable = findDroppable(storage, opts);

  let dropped = 0;
  if (opts.enabled) {
    for (const a of droppable) {
      // Row FIRST: the registry never points at deleted bytes.
      storage.config.deleteArtifact(a.name, a.sha256);
      // Same leaf may back another row (12-hex prefix reuse) — keep it then.
      if (!referencedRefs(storage).has(a.storeRef)) {
        store.delete(a.storeRef);
      }
      dropped += 1;
    }
  }

  // Filesystem pass: reclaim aged leaves nothing references.
  const liveRefs = referencedRefs(storage);
  const livePrefixes = new Set([...referencedShas(storage)].map(sha => sha.slice(0, SHA_PREFIX_LEN)));
  const orphans = store.listLeaves().filter((leaf) => {
    if (now - leaf.mtimeMs < maxAgeMs) {
      return false; // recent = possibly in-flight (mid-ingest) — a root.
    }
    if (leaf.isTmp) {
      return true; // an aged temp file is never addressable — always junk.
    }
    const base = leaf.ref.split("/").pop() ?? "";
    return !liveRefs.has(leaf.ref) && !livePrefixes.has(base);
  });
  let orphansDropped = 0;
  if (opts.enabled) {
    for (const leaf of orphans) {
      store.delete(leaf.ref);
      orphansDropped += 1;
    }
  }

  return { scanned, droppable: droppable.length, dropped, orphanLeaves: orphans.length, orphansDropped };
}

export interface ConfigStoreGcDeps {
  readonly storage: Storage;
  readonly logger?: Logger;
  /** Guard — false/omitted keeps every periodic pass a DRY RUN. */
  readonly enabled?: boolean;
  /** Grace after un-reference (and FS-orphan age guard) in ms. Defaults to 7d. */
  readonly maxAgeMs?: number;
  /** Sweep cadence in ms. Defaults to `CONFIG_GC_INTERVAL_MS`. */
  readonly intervalMs?: number;
  /** Clock seam (tests inject a fake clock). Defaults to `Date.now`. */
  readonly now?: () => number;
}

export interface ConfigStoreGcHandle {
  /** Stop the periodic sweep (call on shutdown). Idempotent. */
  readonly stop: () => void;
  /** Run a sweep pass immediately; returns its result. */
  readonly sweep: () => SweepResult;
}

/**
 * Start the periodic config-store GC sweep (the production caller
 * the documented reclamation was missing). The timer is `unref`'d so it never
 * keeps the process alive on its own; `stop()` clears it. A failing pass is
 * logged and swallowed so the loop survives a transient DB/FS error.
 * the pass stays a DRY RUN (observability only) unless `enabled` is set.
 */
export function startConfigStoreGc(deps: ConfigStoreGcDeps): ConfigStoreGcHandle {
  const pass = (): SweepResult => sweep(deps.storage, {
    ...(deps.enabled != null ? { enabled: deps.enabled } : {}),
    ...(deps.maxAgeMs != null ? { maxAgeMs: deps.maxAgeMs } : {}),
    ...(deps.now ? { now: deps.now() } : {}),
  });
  const tick = () => {
    try {
      const result = pass();
      if (result.droppable > 0 || result.dropped > 0 || result.orphanLeaves > 0) {
        deps.logger?.info({ ...result, enabled: deps.enabled === true }, "config store gc sweep");
      }
    }
    catch (err) {
      deps.logger?.error({ err }, "config store gc failed");
    }
  };

  const timer = setInterval(tick, deps.intervalMs ?? CONFIG_GC_INTERVAL_MS);
  timer.unref?.();

  return {
    stop: () => clearInterval(timer),
    sweep: pass,
  };
}
