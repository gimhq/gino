import type { AppDatabase } from "@/db";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  BlobStore,
  DEFAULT_MAX_AGE_MS,
  findDroppable,
  GLOBAL_SCOPE,
  ingestArtifact,
  referencedShas,
  resolveEntries,
  setEntries,
  sweep,
} from "@/configstore";
import { createDb } from "@/db";
import { attachments } from "@/db/schema";

// M8 — refcount/LRU GC backing BOTH stores. DISABLED by default (OD11): the
// sweep computes the droppable set but auto-deletes nothing unless `enabled`.

const enc = new TextEncoder();
let storeDir: string;
let store: BlobStore;
let db: AppDatabase;

beforeEach(() => {
  storeDir = resolve(tmpdir(), `gino-gc-${process.pid}-${Date.now()}-${Math.round(Math.random() * 1e6)}`);
  store = new BlobStore(storeDir);
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
  rmSync(storeDir, { recursive: true, force: true });
});

/** Insert an attachment descriptor row referencing a sha256 (the unified store). */
function addAttachment(sha256: string, ref: string) {
  db.insert(attachments).values({
    id: `att_${sha256.slice(0, 8)}`,
    taskId: "t1",
    threadId: "th1",
    name: "f.bin",
    contentType: "application/octet-stream",
    size: 1,
    sha256,
    ref,
    direction: "outbound",
  }).run();
}

describe("refcount — referencedShas", () => {
  it("unions sha256 referenced by config_entries and attachments", () => {
    const a = ingestArtifact(db, store, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("inst") });
    const b = ingestArtifact(db, store, { name: "blob", kind: "file", bytes: enc.encode("att-bytes") });

    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: a.sha256, kind: "file" }]));
    addAttachment(b.sha256, b.storeRef);

    const live = referencedShas(db);
    expect(live.has(a.sha256)).toBe(true);
    expect(live.has(b.sha256)).toBe(true);
  });
});

describe("findDroppable — unreferenced AND aged out", () => {
  it("keeps referenced artifacts and recent ones; drops only unreferenced + aged", () => {
    const referenced = ingestArtifact(db, store, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("ref") });
    const orphan = ingestArtifact(db, store, { name: "old", kind: "file", bytes: enc.encode("orphan") });
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: referenced.sha256, kind: "file" }]));

    // now == createdAt → nothing aged out yet.
    expect(findDroppable(db, { now: orphan.createdAt, maxAgeMs: DEFAULT_MAX_AGE_MS })).toHaveLength(0);

    // Far in the future → the orphan ages out; the referenced one is still pinned.
    const future = orphan.createdAt + DEFAULT_MAX_AGE_MS + 1;
    const droppable = findDroppable(db, { now: future, maxAgeMs: DEFAULT_MAX_AGE_MS });
    expect(droppable.map(d => d.name)).toEqual(["old"]);
    expect(droppable[0]!.sha256).toBe(orphan.sha256);
  });

  it("an artifact pinned by an attachment is never droppable", () => {
    const shared = ingestArtifact(db, store, { name: "shared", kind: "file", bytes: enc.encode("shared-bytes") });
    addAttachment(shared.sha256, shared.storeRef);
    const future = shared.createdAt + DEFAULT_MAX_AGE_MS + 1;
    expect(findDroppable(db, { now: future })).toHaveLength(0);
  });
});

describe("sweep — guarded, disabled by default (OD11)", () => {
  it("a default sweep is a DRY RUN: reports droppable but deletes nothing", () => {
    const orphan = ingestArtifact(db, store, { name: "old", kind: "file", bytes: enc.encode("orphan") });
    const future = orphan.createdAt + DEFAULT_MAX_AGE_MS + 1;

    const result = sweep(db, store, { now: future });
    expect(result.droppable).toBe(1);
    expect(result.dropped).toBe(0);
    // Blob + registry row both untouched.
    expect(store.exists(orphan.storeRef)).toBe(true);
    expect(findDroppable(db, { now: future })).toHaveLength(1);
  });

  it("an enabled sweep deletes the unreferenced + aged blobs and rows", () => {
    const orphan = ingestArtifact(db, store, { name: "old", kind: "file", bytes: enc.encode("orphan") });
    const kept = ingestArtifact(db, store, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("ref") });
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: kept.sha256, kind: "file" }]));
    const future = orphan.createdAt + DEFAULT_MAX_AGE_MS + 1;

    const result = sweep(db, store, { now: future, enabled: true });
    expect(result.dropped).toBe(1);
    expect(store.exists(orphan.storeRef)).toBe(false);
    expect(store.exists(kept.storeRef)).toBe(true);
    expect(findDroppable(db, { now: future })).toHaveLength(0);
  });
});
