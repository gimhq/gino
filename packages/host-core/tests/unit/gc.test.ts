import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Storage } from "../../src/host/ports/storage";
import { rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { agents, attachments, configArtifacts, outbox } from "@gino/host-store-sqlite/schema";
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
  startConfigStoreGc,
  sweep,
} from "../../src/configstore";

// M8 — refcount/LRU GC backing BOTH stores. DISABLED by default (OD11): the
// sweep computes the droppable set but auto-deletes nothing unless `enabled`.
// Droppability ages from UN-REFERENCE time (HOST-M8-06), the sweep is
// filesystem-aware (HOST-M8-05), and eviction is row-first (HOST-M8-12).

const enc = new TextEncoder();
let storeDir: string;
let store: BlobStore;
let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  storeDir = resolve(tmpdir(), `gino-gc-${process.pid}-${Date.now()}-${Math.round(Math.random() * 1e6)}`);
  store = new BlobStore(storeDir);
  db = createDb(":memory:");
  storage = sqliteStorage(db, { blobs: store });
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

/** Insert an outbox frame (optionally acked) whose payload names entry shas. */
function addOutboxFrame(sha256: string, opts: { acked?: boolean } = {}) {
  db.insert(agents).values({
    id: `ag_${sha256.slice(0, 8)}`,
    source: "manual",
    access: "dedicated",
    connectTokenHash: "h",
    capabilities: { engines: ["claude"] },
    status: "offline",
  }).run();
  db.insert(outbox).values({
    agentId: `ag_${sha256.slice(0, 8)}`,
    type: "task_upsert",
    dataJson: { entries: [{ name: "skill", sha256, target: "CLAUDE.md", kind: "file" }] },
    ...(opts.acked ? { ackedAt: Date.now() } : {}),
  }).run();
}

/** Backdate a store leaf's mtime so the FS age guard sees it as old. */
function backdateLeaf(ref: string, ageMs: number) {
  const past = new Date(Date.now() - ageMs);
  utimesSync(resolve(storeDir, ref), past, past);
}

describe("refcount — referencedShas", () => {
  it("unions sha256 referenced by config_entries and attachments", () => {
    const a = ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("inst") });
    const b = ingestArtifact(storage, { name: "blob", kind: "file", bytes: enc.encode("att-bytes") });

    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: a.sha256, kind: "file" }]));
    addAttachment(b.sha256, b.storeRef);

    const live = referencedShas(storage);
    expect(live.has(a.sha256)).toBe(true);
    expect(live.has(b.sha256)).toBe(true);
  });

  it("an UNACKED outbox frame naming a sha pins it; an acked one does not (HOST-M8-06)", () => {
    const inflight = ingestArtifact(storage, { name: "inflight", kind: "file", bytes: enc.encode("owed to an offline agent") });
    const done = ingestArtifact(storage, { name: "done", kind: "file", bytes: enc.encode("already acked") });
    addOutboxFrame(inflight.sha256);
    addOutboxFrame(done.sha256, { acked: true });

    const live = referencedShas(storage);
    expect(live.has(inflight.sha256)).toBe(true);
    expect(live.has(done.sha256)).toBe(false);
  });
});

describe("findDroppable — unreferenced AND past the un-reference grace (HOST-M8-06)", () => {
  it("keeps referenced artifacts and recent ones; drops only unreferenced + aged", () => {
    const referenced = ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("ref") });
    const orphan = ingestArtifact(storage, { name: "old", kind: "file", bytes: enc.encode("orphan") });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: referenced.sha256, kind: "file" }]));

    // First observation only STARTS the grace clock — nothing droppable yet.
    expect(findDroppable(storage, { now: orphan.createdAt, maxAgeMs: DEFAULT_MAX_AGE_MS })).toHaveLength(0);

    // Past the grace since first-unreferenced → the orphan drops; the referenced one is pinned.
    const future = orphan.createdAt + DEFAULT_MAX_AGE_MS + 1;
    const droppable = findDroppable(storage, { now: future, maxAgeMs: DEFAULT_MAX_AGE_MS });
    expect(droppable.map(d => d.name)).toEqual(["old"]);
    expect(droppable[0]!.sha256).toBe(orphan.sha256);
  });

  it("ages by UN-REFERENCE time, not created_at: a just-unreferenced old artifact survives the grace window", () => {
    const a = ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("shaA — months old") });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: a.sha256, kind: "file" }]));

    // Months later the admin swaps the entry away from shaA...
    const swapTime = a.createdAt + 90 * 24 * 60 * 60 * 1000;
    const b = ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("shaB") });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: b.sha256, kind: "file" }]));

    // ...and a sweep runs a minute later: shaA is FAR older than the grace by
    // created_at, but was just un-referenced — it must NOT be droppable.
    expect(findDroppable(storage, { now: swapTime + 60_000 })).toHaveLength(0);

    // Only once the grace has elapsed SINCE THE SWAP does it age out.
    const aged = findDroppable(storage, { now: swapTime + 60_000 + DEFAULT_MAX_AGE_MS });
    expect(aged.map(d => d.sha256)).toEqual([a.sha256]);
  });

  it("re-referencing an artifact resets its un-reference clock", () => {
    const a = ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("rollback target") });
    const t0 = a.createdAt;

    // Observed unreferenced at t0; re-declared (rollback) before the grace ran out.
    findDroppable(storage, { now: t0 });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: a.sha256, kind: "file" }]));
    findDroppable(storage, { now: t0 + 1000 }); // clears the mark
    expect(db.select().from(configArtifacts).all()[0]!.unreferencedSince).toBeNull();

    // Un-referenced again much later: aging restarts from the NEW un-reference.
    setEntries(storage, GLOBAL_SCOPE, []);
    const t1 = t0 + DEFAULT_MAX_AGE_MS * 2;
    findDroppable(storage, { now: t1 }); // marks at t1
    expect(findDroppable(storage, { now: t1 + 1 })).toHaveLength(0);
    expect(findDroppable(storage, { now: t1 + DEFAULT_MAX_AGE_MS })).toHaveLength(1);
  });

  it("an artifact pinned by an attachment is never droppable", () => {
    const shared = ingestArtifact(storage, { name: "shared", kind: "file", bytes: enc.encode("shared-bytes") });
    addAttachment(shared.sha256, shared.storeRef);
    const future = shared.createdAt + DEFAULT_MAX_AGE_MS + 1;
    findDroppable(storage, { now: future });
    expect(findDroppable(storage, { now: future + DEFAULT_MAX_AGE_MS })).toHaveLength(0);
  });
});

describe("sweep — guarded, disabled by default (OD11)", () => {
  /** Ingest an artifact and age it past the grace (marked at createdAt, aged at the returned `now`). */
  function agedOrphan(name: string) {
    const orphan = ingestArtifact(storage, { name, kind: "file", bytes: enc.encode(`${name}-bytes`) });
    findDroppable(storage, { now: orphan.createdAt }); // start the un-reference clock
    return { orphan, future: orphan.createdAt + DEFAULT_MAX_AGE_MS + 1 };
  }

  it("a default sweep is a DRY RUN: reports droppable but deletes nothing", () => {
    const { orphan, future } = agedOrphan("old");

    const result = sweep(storage, { now: future });
    expect(result.droppable).toBe(1);
    expect(result.dropped).toBe(0);
    // Blob + registry row both untouched.
    expect(store.exists(orphan.storeRef)).toBe(true);
    expect(findDroppable(storage, { now: future })).toHaveLength(1);
  });

  it("an enabled sweep deletes the unreferenced + aged blobs and rows", () => {
    const { orphan, future } = agedOrphan("old");
    const kept = ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("ref") });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: kept.sha256, kind: "file" }]));

    const result = sweep(storage, { now: future, enabled: true });
    expect(result.dropped).toBe(1);
    expect(store.exists(orphan.storeRef)).toBe(false);
    expect(store.exists(kept.storeRef)).toBe(true);
    expect(findDroppable(storage, { now: future })).toHaveLength(0);
    // Row-first eviction (HOST-M8-12): no dangling registry row survives.
    expect(db.select().from(configArtifacts).all().map(a => a.name)).toEqual(["CLAUDE.md"]);
  });

  it("reclaims an aged ORPHAN blob (no registry/attachment row) — HOST-M8-05", () => {
    // A crash between store.put and the DB insert leaves exactly this shape.
    const blob = store.put("crash-orphan", enc.encode("no row points here"));
    backdateLeaf(blob.ref, DEFAULT_MAX_AGE_MS + 60_000);

    const dry = sweep(storage, {});
    expect(dry.orphanLeaves).toBe(1);
    expect(dry.orphansDropped).toBe(0);
    expect(store.exists(blob.ref)).toBe(true);

    const result = sweep(storage, { enabled: true });
    expect(result.orphansDropped).toBe(1);
    expect(store.exists(blob.ref)).toBe(false);
  });

  it("keeps a RECENT orphan leaf (possibly a mid-ingest blob) and any attachment-referenced blob", () => {
    const recent = store.put("mid-ingest", enc.encode("just written"));
    const att = store.put("att-blob", enc.encode("attachment bytes"));
    addAttachment(att.sha256, att.ref);
    backdateLeaf(att.ref, DEFAULT_MAX_AGE_MS + 60_000); // old but referenced

    const result = sweep(storage, { enabled: true });
    expect(result.orphansDropped).toBe(0);
    expect(store.exists(recent.ref)).toBe(true);
    expect(store.exists(att.ref)).toBe(true);
  });

  it("cleans a STALE temp file but keeps a fresh (in-flight) one — HOST-M8-05", () => {
    const blob = store.put("skill", enc.encode("real bytes"));
    addAttachment(blob.sha256, blob.ref);
    const stale = resolve(storeDir, "skill", "deadbeef0000.tmp-123-456");
    const fresh = resolve(storeDir, "skill", "cafebabe0000.tmp-789-012");
    writeFileSync(stale, "half-written long ago");
    writeFileSync(fresh, "being written right now");
    const past = new Date(Date.now() - DEFAULT_MAX_AGE_MS - 60_000);
    utimesSync(stale, past, past);

    const result = sweep(storage, { enabled: true });
    expect(result.orphansDropped).toBe(1);
    expect(store.exists("skill/deadbeef0000.tmp-123-456")).toBe(false);
    expect(store.exists("skill/cafebabe0000.tmp-789-012")).toBe(true);
    expect(store.exists(blob.ref)).toBe(true);
  });
});

describe("startConfigStoreGc — the periodic production caller (HOST-M8-11)", () => {
  it("sweeps on its interval and stop() halts it", async () => {
    const orphan = ingestArtifact(storage, { name: "old", kind: "file", bytes: enc.encode("orphan") });
    // Clock far in the future: the first tick marks un-referenced, later ticks age it out.
    let now = orphan.createdAt;
    const handle = startConfigStoreGc({
      storage,
      enabled: true,
      maxAgeMs: 1,
      intervalMs: 5,
      now: () => {
        now += DEFAULT_MAX_AGE_MS;
        return now;
      },
    });
    try {
      const deadline = Date.now() + 2000;
      while (store.exists(orphan.storeRef) && Date.now() < deadline) {
        await Bun.sleep(10);
      }
      expect(store.exists(orphan.storeRef)).toBe(false);
      expect(db.select().from(configArtifacts).all()).toHaveLength(0);
    }
    finally {
      handle.stop();
    }
  });

  it("exposes a manual sweep() pass and defaults to DRY RUN", () => {
    const orphan = ingestArtifact(storage, { name: "old", kind: "file", bytes: enc.encode("orphan") });
    let now = orphan.createdAt;
    const handle = startConfigStoreGc({ storage, intervalMs: 60_000, maxAgeMs: 1, now: () => {
      now += DEFAULT_MAX_AGE_MS;
      return now;
    } });
    try {
      handle.sweep(); // marks un-referenced
      const second = handle.sweep();
      expect(second.droppable).toBe(1);
      expect(second.dropped).toBe(0); // OD11: not enabled → nothing deleted
      expect(store.exists(orphan.storeRef)).toBe(true);
    }
    finally {
      handle.stop();
    }
  });
});
