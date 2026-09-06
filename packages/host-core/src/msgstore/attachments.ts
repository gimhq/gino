// M5 — attachment descriptors → `attachments`.
//
// Bytes never travel in frames (protocol §5) — only descriptors do. Descriptors
// ride on `thread_result.data.attachments` (agent-produced ⇒ `outbound`) or
// `message.data.attachments` (user-supplied ⇒ `inbound`); messages reference a
// row by its `id`. Idempotent on `id` (first writer wins) so a replayed carrier
// frame neither duplicates nor overwrites a descriptor.
import type { Storage } from "../host/ports/storage";
import type { AttachmentDescriptor } from "../protocol/frames";
import { sanitizeAttachmentName, storeRefFor } from "../configstore";

export interface AttachmentWrite {
  taskId: string;
  threadId: string;
  descriptors: AttachmentDescriptor[];
}

/**
 * The byte-channel URL an agent resolves a descriptor `ref` against
 * (agent-architecture §9 host-proxy backend: the runner joins `ref` to its
 * host base URL). This — not the store-internal ref — is what every wire
 * descriptor must carry.
 */
export function wireAttachmentRef(id: string): string {
  return `/agent/attachments/${id}`;
}

/**
 * Persist attachment descriptors, idempotent on `id`. Each descriptor's
 * `direction` is honored when present; absent, it defaults to `outbound`
 * (descriptors most commonly arrive on `thread_result`, i.e. agent-produced).
 * A first writer of a given `id` wins; later duplicates are ignored.
 *
 * The stored `name`/`ref` are DERIVED — the wire name is sanitized to a store
 * segment (the runner names outbound files by `output/`-relative path, so
 * separators are expected) and the ref is the deterministic store ref for
 * `(sanitized name, sha256)`. The wire descriptor's `ref` is never trusted:
 * it is a byte-channel URL (and, being peer-supplied, unsafe as a filesystem
 * path). Sanitizing here with the SAME function the byte-upload route uses
 * keeps the registry row pointing at the blob that upload stored.
 */
export function writeAttachmentDescriptors(
  storage: Storage,
  { taskId, threadId, descriptors }: AttachmentWrite,
): void {
  if (descriptors.length === 0) {
    return;
  }
  for (const d of descriptors) {
    const name = sanitizeAttachmentName(d.name);
    storage.attachments.insertIfAbsent({
      id: d.id,
      taskId,
      threadId,
      name,
      contentType: d.content_type,
      size: d.size,
      sha256: d.sha256,
      ref: storeRefFor(name, d.sha256),
      direction: d.direction ?? "outbound",
    });
  }
}
