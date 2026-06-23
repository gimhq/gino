import type { AppDatabase } from "@/db";
import { existsSync, readdirSync, rmSync } from "node:fs";
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
  sha256Hex,
  ShaMismatchError,
} from "@/configstore";
import { createDb } from "@/db";

// M8 — the content-addressed blob store + the immutable artifact registry index.

const enc = new TextEncoder();
let storeDir: string;
let store: BlobStore;
let db: AppDatabase;

beforeEach(() => {
  storeDir = resolve(tmpdir(), `gino-store-${process.pid}-${Date.now()}-${Math.round(Math.random() * 1e6)}`);
  store = new BlobStore(storeDir);
  db = createDb(":memory:");
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

  it("read of a missing ref throws", () => {
    expect(() => store.read("nope/000000000000")).toThrow(ConfigStoreError);
  });
});

describe("config_artifacts registry — ingest/get/list", () => {
  it("ingests bytes + upserts an immutable row (dedup by sha256)", () => {
    const bytes = enc.encode("# skill bundle");
    const first = ingestArtifact(db, store, { name: "my-skill", kind: "dir", bytes, contentType: "application/gzip" });

    expect(first.name).toBe("my-skill");
    expect(first.kind).toBe("dir");
    expect(first.sha256).toBe(sha256Hex(bytes));
    expect(first.storeRef).toBe(`my-skill/${first.sha256.slice(0, 12)}`);
    expect(first.contentType).toBe("application/gzip");

    // A re-ingest of identical (name, content) is a no-op: still one row.
    ingestArtifact(db, store, { name: "my-skill", kind: "dir", bytes });
    expect(listArtifacts(db)).toHaveLength(1);

    expect(getArtifact(db, "my-skill", first.sha256)?.size).toBe(bytes.byteLength);
    expect(getArtifactBySha(db, first.sha256)?.name).toBe("my-skill");
  });

  it("a new version (new content) is a new immutable row", () => {
    ingestArtifact(db, store, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("v1") });
    ingestArtifact(db, store, { name: "CLAUDE.md", kind: "file", bytes: enc.encode("v2") });
    expect(listArtifacts(db).filter(a => a.name === "CLAUDE.md")).toHaveLength(2);
  });

  it("a file artifact carries kind:file (a skill is just kind:dir — one abstraction)", () => {
    const file = ingestArtifact(db, store, { name: "design.md", kind: "file", bytes: enc.encode("doc") });
    const dir = ingestArtifact(db, store, { name: "reviewer", kind: "dir", bytes: enc.encode("bundle") });
    expect(file.kind).toBe("file");
    expect(dir.kind).toBe("dir");
    // Both materialized as physical blobs under the unified store.
    expect(existsSync(resolve(storeDir, file.storeRef))).toBe(true);
    expect(existsSync(resolve(storeDir, dir.storeRef))).toBe(true);
  });
});
