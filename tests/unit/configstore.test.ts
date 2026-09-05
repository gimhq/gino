import type { AppDatabase } from "@/db";
import type { Storage } from "@/host/ports/storage";
import { existsSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  BlobStore,
  ConfigStoreError,
  getArtifact,
  getArtifactBySha,
  ingestArtifact,
  listArtifacts,
  MissingBlobError,
  sha256Hex,
  ShaMismatchError,
} from "@/configstore";
import { createDb } from "@/db";
import { sqliteStorage } from "@/store/sqlite";

// M8 — the content-addressed blob store + the immutable artifact registry index.

const enc = new TextEncoder();
let storeDir: string;
let store: BlobStore;
let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  storeDir = resolve(tmpdir(), `gino-store-${process.pid}-${Date.now()}-${Math.round(Math.random() * 1e6)}`);
  store = new BlobStore(storeDir);
  db = createDb(":memory:");
  storage = sqliteStorage(db, { blobs: store });
});

afterEach(() => {
  db.close();
  rmSync(storeDir, { recursive: true, force: true });
});

describe("BlobStore — content-addressed put/read", () => {
  it("stores at store/<name>/<sha256[:12]> and round-trips the bytes", () => {
    const bytes = enc.encode("hello skills");
    const blob = store.put("CLAUDE.md", bytes);

    expect(blob.sha256).toBe(sha256Hex(bytes));
    expect(blob.ref).toBe(`CLAUDE.md/${blob.sha256.slice(0, 12)}`);
    expect(blob.size).toBe(bytes.byteLength);
    expect(store.exists(blob.ref)).toBe(true);
    expect(new TextDecoder().decode(store.read(blob.ref))).toBe("hello skills");
  });

  it("dedups identical content (idempotent put — one physical copy)", () => {
    const bytes = enc.encode("dedupe me");
    const a = store.put("note.md", bytes);
    const b = store.put("note.md", bytes);

    expect(a.ref).toBe(b.ref);
    expect(a.sha256).toBe(b.sha256);
    // Exactly one physical file under the name dir (no .tmp leftovers either).
    const files = readdirSync(resolve(storeDir, "note.md"));
    expect(files).toEqual([a.sha256.slice(0, 12)]);
  });

  it("different content under the same name lands at distinct content addresses", () => {
    const a = store.put("note.md", enc.encode("v1"));
    const b = store.put("note.md", enc.encode("v2"));
    expect(a.ref).not.toBe(b.ref);
    expect(readdirSync(resolve(storeDir, "note.md")).length).toBe(2);
  });

  it("readVerified passes on a matching sha256 and throws ShaMismatchError otherwise", () => {
    const blob = store.put("a.txt", enc.encode("integrity"));
    expect(new TextDecoder().decode(store.readVerified(blob.ref, blob.sha256))).toBe("integrity");
    expect(() => store.readVerified(blob.ref, "f".repeat(64))).toThrow(ShaMismatchError);
  });

  it("rejects an unsafe (traversal) name", () => {
    expect(() => store.put("../escape", enc.encode("x"))).toThrow(ConfigStoreError);
    expect(() => store.put("a/b", enc.encode("x"))).toThrow(ConfigStoreError);
    expect(() => store.put("", enc.encode("x"))).toThrow(ConfigStoreError);
  });

  it("read of a missing ref throws MissingBlobError", () => {
    expect(() => store.read("nope/000000000000")).toThrow(ConfigStoreError);
    expect(() => store.read("nope/000000000000")).toThrow(MissingBlobError);
  });

  it("self-heals a corrupted/squatted leaf on re-put instead of no-opping (HOST-M8-04)", () => {
    const bytes = enc.encode("the real artifact bytes");
    const blob = store.put("skill.md", bytes);

    // Corrupt the stored file underneath the content address (bit rot / a
    // prefix-squatted leaf) — the old dedup would have trusted the path and
    // left the wrong bytes in place forever.
    writeFileSync(resolve(storeDir, blob.ref), "WRONG BYTES");
    expect(() => store.readVerified(blob.ref, blob.sha256)).toThrow(ShaMismatchError);

    // Re-ingesting the correct bytes repairs the blob (no permanent 409).
    const healed = store.put("skill.md", bytes);
    expect(healed.ref).toBe(blob.ref);
    expect(new TextDecoder().decode(store.readVerified(blob.ref, blob.sha256))).toBe("the real artifact bytes");
  });

  it("enforces root containment of ANY ref before touching the filesystem (HOST-M8-09)", () => {
    // A traversal-shaped ref (e.g. an agent-supplied attachment descriptor)
    // must never resolve outside the store root — on any store operation.
    const secret = resolve(storeDir, "..", `gino-secret-${process.pid}.txt`);
    writeFileSync(secret, "host-private bytes");
    try {
      const escape = `../${secret.split("/").pop()}`;
      expect(() => store.read(escape)).toThrow(ConfigStoreError);
      expect(() => store.readVerified(escape, sha256Hex(enc.encode("host-private bytes")))).toThrow(ConfigStoreError);
      expect(() => store.sizeOf(escape)).toThrow(ConfigStoreError);
      expect(() => store.delete(escape)).toThrow(ConfigStoreError);
      expect(() => store.exists(escape)).toThrow(ConfigStoreError);
      expect(() => store.read("/etc/hostname")).toThrow(ConfigStoreError);
      expect(() => store.read("name/../../x")).toThrow(ConfigStoreError);
      expect(existsSync(secret)).toBe(true); // delete() never reached it
    }
    finally {
      rmSync(secret, { force: true });
    }
  });
});

describe("config_artifacts registry — ingest/get/list", () => {
  it("ingests bytes + upserts an immutable row (dedup by sha256)", () => {
    const bytes = enc.encode("# skill bundle");
    const first = ingestArtifact(storage, { name: "my-skill", kind: "dir", bytes, contentType: "application/gzip" });

    expect(first.name).toBe("my-skill");
    expect(first.kind).toBe("dir");
    expect(first.sha256).toBe(sha256Hex(bytes));
    expect(first.storeRef).toBe(`my-skill/${first.sha256.slice(0, 12)}`);
    expect(first.contentType).toBe("application/gzip");

    // A re-ingest of identical (name, content) is a no-op: still one row.
    ingestArtifact(storage, { name: "my-skill", kind: "dir", bytes });
    expect(listArtifacts(storage)).toHaveLength(1);

    expect(getArtifact(storage, "my-skill", first.sha256)?.size).toBe(bytes.byteLength);
    expect(getArtifactBySha(storage, first.sha256)?.name).toBe("my-skill");
  });

  it("a new version (new content) is a new immutable row", () => {
    ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("v1") });
    ingestArtifact(storage, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("v2") });
    expect(listArtifacts(storage).filter(a => a.name === "CLAUDE.md")).toHaveLength(2);
  });

  it("re-ingesting identical (name, bytes) with different metadata UPDATES kind/content_type (HOST-M8-13)", () => {
    const bytes = enc.encode("bundle-bytes");
    const first = ingestArtifact(storage, { name: "bundle", kind: "file", bytes, contentType: "text/plain" });
    expect(first.kind).toBe("file");
    expect(first.contentType).toBe("text/plain");

    // The corrective re-ingest is NOT silently ignored: metadata last-write-wins.
    const second = ingestArtifact(storage, { name: "bundle", kind: "dir", bytes, contentType: "application/gzip" });
    expect(second.sha256).toBe(first.sha256);
    expect(second.kind).toBe("dir");
    expect(second.contentType).toBe("application/gzip");

    // Still one content-addressed row + one physical blob (the bytes dedup).
    expect(listArtifacts(storage).filter(a => a.name === "bundle")).toHaveLength(1);
    expect(readdirSync(resolve(storeDir, "bundle"))).toHaveLength(1);
  });

  it("a file artifact carries kind:file (a skill is just kind:dir — one abstraction)", () => {
    const file = ingestArtifact(storage, { name: "design.md", kind: "file", bytes: enc.encode("doc") });
    const dir = ingestArtifact(storage, { name: "reviewer", kind: "dir", bytes: enc.encode("bundle") });
    expect(file.kind).toBe("file");
    expect(dir.kind).toBe("dir");
    // Both materialized as physical blobs under the unified store.
    expect(existsSync(resolve(storeDir, file.storeRef))).toBe(true);
    expect(existsSync(resolve(storeDir, dir.storeRef))).toBe(true);
  });
});
