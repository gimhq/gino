// M8 — CONTENT-ADDRESSED BLOB STORE (the physical bytes the host holds + serves).
//
// One unified store backs BOTH the config/skills registry (`config_artifacts`)
// and the attachment byte channel (`attachments`) — there is no separate skill
// store. A blob lands at `store/<name>/<sha256[:12]>`; the leaf IS the content
// hash, so an identical re-`put` is an idempotent no-op (content-addressed
// dedup). Reads are verified against the FULL `sha256` so a truncated/corrupt
// blob never serves silently. The host NEVER touches an agent's filesystem —
// this is purely the host-side blob index the byte channel reads from.
import type { BlobStorePort } from "../host/ports/storage";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import process from "node:process";

/** How many hex chars of the sha256 name the content-addressed leaf. */
export const SHA_PREFIX_LEN = 12;

/** A single safe path segment — no separators, no traversal. */
const RE_SAFE_NAME = /^[\w.@+-]+$/;

export class ConfigStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConfigStoreError";
  }
}

/** Thrown when stored/ingested bytes do not match the expected `sha256`. */
export class ShaMismatchError extends ConfigStoreError {
  constructor(message: string) {
    super(message);
    this.name = "ShaMismatchError";
  }
}

/**
 * Thrown when a `ref` resolves to no physical blob (HOST-M8-12) — a dangling
 * registry/descriptor row (e.g. a crash between a GC row delete and blob
 * delete). The serve path maps this to a 404, never an unhandled 500.
 */
export class MissingBlobError extends ConfigStoreError {
  constructor(message: string) {
    super(message);
    this.name = "MissingBlobError";
  }
}

/** Full SHA-256 hex digest over raw bytes (the content address). */
export function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** True iff `name` is a single safe path segment (no separators, no traversal). */
export function isSafeName(name: string): boolean {
  return !!name && name.length <= 255 && name !== "." && name !== ".." && RE_SAFE_NAME.test(name);
}

/**
 * Reject a name that is not a single safe path segment (defends the store dir
 * against `..`/separator traversal). Artifact + attachment names land as a
 * store sub-directory, so they MUST be a plain segment.
 */
export function assertSafeName(name: string): void {
  if (!isSafeName(name)) {
    throw new ConfigStoreError(`unsafe store name: ${JSON.stringify(name)}`);
  }
}

export interface StoredBlob {
  name: string;
  sha256: string;
  size: number;
  /** Store-relative ref `<name>/<sha256[:12]>` (the value persisted as `store_ref`/`ref`). */
  ref: string;
}

/**
 * Store-relative ref for a `(name, sha256)` pair — the deterministic location
 * `put()` writes to. Exposed as a free function so registry writers can derive
 * the store ref for a descriptor WITHOUT trusting the wire `ref` (the wire
 * carries a byte-channel URL, never a store path). Rejects unsafe names.
 */
export function storeRefFor(name: string, sha256: string): string {
  assertSafeName(name);
  return `${name}/${sha256.slice(0, SHA_PREFIX_LEN)}`;
}

/**
 * Map an arbitrary wire attachment name to a safe single store segment. The
 * runner names an outbound attachment by its path RELATIVE to `output/`
 * (agent-architecture §9), so separators are expected on the wire — they are
 * flattened deterministically so the byte-upload path and the later
 * `thread_result` descriptor registration derive the SAME store ref for the
 * same wire name.
 */
export function sanitizeAttachmentName(raw: string): string {
  const flattened = raw
    .replace(/[/\\]+/g, "__")
    .replace(/[^\w.@+-]/g, "_")
    .slice(0, 255);
  return isSafeName(flattened) ? flattened : "attachment";
}

/**
 * The content-addressed blob store rooted at a single directory (`GINO_STORE_DIR`).
 * Cheap to construct (it only holds the root path) — build one per request.
 */
export class BlobStore implements BlobStorePort {
  constructor(readonly root: string) {}

  /** Store-relative ref for a `(name, sha256)` pair. */
  refFor(name: string, sha256: string): string {
    return storeRefFor(name, sha256);
  }

  /**
   * Resolve a `ref` to its absolute path, ENFORCING root containment
   * (HOST-M8-09): the resolved path must be strictly inside the store root, so
   * a traversal-shaped ref (`../../data/gino.db`, an absolute path, …) can
   * never reach the filesystem — regardless of where the ref came from.
   */
  private abs(ref: string): string {
    const root = resolve(this.root);
    const path = resolve(root, ref);
    if (!path.startsWith(root + sep)) {
      throw new ConfigStoreError(`ref escapes the store root: ${JSON.stringify(ref)}`);
    }
    return path;
  }

  /**
   * Content-address + persist bytes. Returns the `{name, sha256, size, ref}`
   * descriptor. The write is idempotent: when the content-addressed path
   * already exists AND its bytes still hash to the same sha256 the write is a
   * dedup no-op. When the existing bytes DON'T match (bit rot, tampering, or a
   * 48-bit prefix collision squatting the leaf — HOST-M8-04) the blob is
   * rewritten from the incoming payload (self-heal) instead of no-opping, so a
   * re-ingest of the correct bytes always repairs the store. The write itself
   * is temp-file + rename so a reader never observes a partial blob.
   */
  put(name: string, bytes: Uint8Array): StoredBlob {
    assertSafeName(name);
    const sha256 = sha256Hex(bytes);
    const ref = this.refFor(name, sha256);
    const path = this.abs(ref);
    if (existsSync(path)) {
      const existing = sha256Hex(new Uint8Array(readFileSync(path)));
      if (existing === sha256) {
        return { name, sha256, size: bytes.byteLength, ref };
      }
      console.warn(`[configstore] blob ${ref} holds bytes hashing to ${existing}, expected ${sha256} — rewriting from the incoming payload (HOST-M8-04 self-heal)`);
    }
    mkdirSync(dirname(path), { recursive: true });
    const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
    writeFileSync(tmp, bytes);
    renameSync(tmp, path);
    return { name, sha256, size: bytes.byteLength, ref };
  }

  exists(ref: string): boolean {
    return existsSync(this.abs(ref));
  }

  /** Raw read (no integrity check) — prefer `readVerified` for serving. */
  read(ref: string): Uint8Array {
    const path = this.abs(ref);
    if (!existsSync(path)) {
      throw new MissingBlobError(`missing blob: ${ref}`);
    }
    return new Uint8Array(readFileSync(path));
  }

  /**
   * Read + verify the FULL sha256 against `expected`. Throws `ShaMismatchError`
   * when the stored bytes do not hash to `expected` — the integrity gate the
   * byte channel serves through, so a corrupt/tampered blob is never returned.
   */
  readVerified(ref: string, expected: string): Uint8Array {
    const bytes = this.read(ref);
    const actual = sha256Hex(bytes);
    if (actual !== expected) {
      throw new ShaMismatchError(`sha256 mismatch for ${ref}: expected ${expected}, got ${actual}`);
    }
    return bytes;
  }

  /** Physical size of a stored blob (0 when absent). */
  sizeOf(ref: string): number {
    const path = this.abs(ref);
    return existsSync(path) ? statSync(path).size : 0;
  }

  /** Remove a blob (used only by the guarded GC sweep — never in normal operation). */
  delete(ref: string): void {
    rmSync(this.abs(ref), { force: true });
  }

  /**
   * Every physical leaf under the store root — blobs AND leftover `.tmp-*`
   * files. The filesystem-aware GC input (HOST-M8-05): a leaf no DB row
   * references is an orphan the sweep may reclaim; `mtimeMs` is its age guard.
   */
  listLeaves(): StoreLeaf[] {
    if (!existsSync(this.root)) {
      return [];
    }
    const leaves: StoreLeaf[] = [];
    for (const dir of readdirSync(this.root, { withFileTypes: true })) {
      if (!dir.isDirectory()) {
        continue;
      }
      for (const leaf of readdirSync(join(this.root, dir.name), { withFileTypes: true })) {
        if (!leaf.isFile()) {
          continue;
        }
        const ref = `${dir.name}/${leaf.name}`;
        leaves.push({ ref, mtimeMs: statSync(join(this.root, dir.name, leaf.name)).mtimeMs, isTmp: leaf.name.includes(".tmp-") });
      }
    }
    return leaves;
  }
}

/** One physical file under the store root (see `BlobStore.listLeaves`). */
export interface StoreLeaf {
  /** Store-relative `<name>/<leaf>` path. */
  ref: string;
  mtimeMs: number;
  /** True for an in-flight/leftover temp file (`*.tmp-<pid>-<ts>`). */
  isTmp: boolean;
}
