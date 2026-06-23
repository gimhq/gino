import type { BlobStore } from "./store";
// M8 — REFCOUNT / GC (the sweep backing BOTH stores; disabled by default, OD11).
//
// A blob `(name, sha256)` is droppable when NOTHING references its `sha256` —
// neither a `config_entries` desired-set row NOR an `attachments` descriptor —
// AND it has aged out (LRU by `created_at`). The same refcount backs the config
// store and the attachment store (one unified store). Per OD11 the sweep is a
// GUARDED no-op by default: it computes the droppable set but DELETES NOTHING
// unless explicitly `enabled` — so normal operation never auto-evicts.
import type { AppDatabase } from "@/db";
import { and, eq } from "drizzle-orm";
import { attachments, configArtifacts, configEntries } from "@/db/schema";

/** Default LRU age before an unreferenced artifact is droppable (7 days). */
export const DEFAULT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

export interface DroppableArtifact {
  name: string;
  sha256: string;
  storeRef: string;
  createdAt: number;
}

/**
 * The LIVE set: every `sha256` referenced by the desired set (`config_entries`)
 * OR an attachment descriptor (`attachments`). An artifact whose `sha256` is in
 * this set is pinned and never droppable.
 */
export function referencedShas(db: AppDatabase): Set<string> {
  const live = new Set<string>();
  for (const r of db.select({ sha256: configEntries.sha256 }).from(configEntries).all()) {
    live.add(r.sha256);
  }
  for (const r of db.select({ sha256: attachments.sha256 }).from(attachments).all()) {
    live.add(r.sha256);
  }
  return live;
}

export interface GcOptions {
  /** Wall clock for the age test (defaults to now). */
  now?: number;
  /** LRU age threshold; an artifact younger than this is kept (defaults to 7d). */
  maxAgeMs?: number;
}

/**
 * Artifacts that are BOTH unreferenced (refcount 0) AND aged out — the droppable
 * set. Pure computation: it deletes nothing.
 */
export function findDroppable(db: AppDatabase, opts: GcOptions = {}): DroppableArtifact[] {
  const now = opts.now ?? Date.now();
  const maxAgeMs = opts.maxAgeMs ?? DEFAULT_MAX_AGE_MS;
  const live = referencedShas(db);
  return db
    .select()
    .from(configArtifacts)
    .all()
    .filter(a => !live.has(a.sha256) && now - a.createdAt >= maxAgeMs)
    .map(a => ({ name: a.name, sha256: a.sha256, storeRef: a.storeRef, createdAt: a.createdAt }));
}

export interface SweepOptions extends GcOptions {
  /**
   * GUARD (OD11): when false/omitted the sweep is a DRY RUN — it reports the
   * droppable set but deletes nothing. Only `enabled: true` evicts.
   */
  enabled?: boolean;
}

export interface SweepResult {
  scanned: number;
  droppable: number;
  dropped: number;
}

/**
 * Refcount/LRU sweep. By default (OD11) a DRY RUN: it returns the droppable
 * count with `dropped: 0` and leaves both the blob store and `config_artifacts`
 * untouched. With `enabled: true` it deletes each droppable blob + its registry
 * row (the GC is opt-in, never automatic).
 */
export function sweep(db: AppDatabase, store: BlobStore, opts: SweepOptions = {}): SweepResult {
  const scanned = db.select({ sha256: configArtifacts.sha256 }).from(configArtifacts).all().length;
  const droppable = findDroppable(db, opts);
  if (!opts.enabled) {
    return { scanned, droppable: droppable.length, dropped: 0 };
  }
  let dropped = 0;
  for (const a of droppable) {
    store.delete(a.storeRef);
    db
      .delete(configArtifacts)
      .where(and(eq(configArtifacts.name, a.name), eq(configArtifacts.sha256, a.sha256)))
      .run();
    dropped += 1;
  }
  return { scanned, droppable: droppable.length, dropped };
}
