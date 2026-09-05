// MEMORY STORE — in-memory `BlobStorePort` (content-addressed, keyed by ref).
import type { BlobLeaf, BlobStorePort, StoredBlobRef } from "@/host/ports/storage";
import { MissingBlobError, sha256Hex, ShaMismatchError, storeRefFor } from "@/configstore/store";

interface Leaf {
  bytes: Uint8Array;
  mtimeMs: number;
}

export interface MemoryBlobStore extends BlobStorePort {
  /** Test hook: overwrite the stored bytes behind `ref` so `readVerified` sees a mismatch. Throws when absent. */
  tamper: (ref: string) => void;
}

export function memoryBlobStore(): MemoryBlobStore {
  const leaves = new Map<string, Leaf>();

  const leafOf = (ref: string): Leaf => {
    const leaf = leaves.get(ref);
    if (!leaf) {
      throw new MissingBlobError(`missing blob: ${ref}`);
    }
    return leaf;
  };

  return {
    refFor: storeRefFor,
    put(name, bytes): StoredBlobRef {
      const sha256 = sha256Hex(bytes);
      const ref = storeRefFor(name, sha256);
      const existing = leaves.get(ref);
      if (!existing || sha256Hex(existing.bytes) !== sha256) {
        leaves.set(ref, { bytes: new Uint8Array(bytes), mtimeMs: Date.now() });
      }
      return { name, sha256, size: bytes.byteLength, ref };
    },
    exists(ref) {
      return leaves.has(ref);
    },
    read(ref) {
      return new Uint8Array(leafOf(ref).bytes);
    },
    readVerified(ref, expected) {
      const bytes = new Uint8Array(leafOf(ref).bytes);
      const actual = sha256Hex(bytes);
      if (actual !== expected) {
        throw new ShaMismatchError(`sha256 mismatch for ${ref}: expected ${expected}, got ${actual}`);
      }
      return bytes;
    },
    sizeOf(ref) {
      return leaves.get(ref)?.bytes.byteLength ?? 0;
    },
    delete(ref) {
      leaves.delete(ref);
    },
    listLeaves(): BlobLeaf[] {
      return [...leaves.entries()].map(([ref, leaf]) => ({ ref, mtimeMs: leaf.mtimeMs, isTmp: false }));
    },
    tamper(ref) {
      const leaf = leafOf(ref);
      leaf.bytes = new Uint8Array([...leaf.bytes, 0x00]);
    },
  };
}
