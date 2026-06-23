// M5 — attachment descriptors → `attachments`.
//
// Bytes never travel in frames (protocol §5) — only descriptors do. Descriptors
// ride on `thread_result.data.attachments` (agent-produced ⇒ `outbound`) or
// `message.data.attachments` (user-supplied ⇒ `inbound`); messages reference a
// row by its `id`. Idempotent on `id` (ON CONFLICT DO NOTHING) so a replayed
// carrier frame neither duplicates nor overwrites a descriptor.
import type { AppDatabase } from "@/db";
import type { AttachmentDescriptor } from "@/protocol/frames";
import { attachments } from "@/db/schema";

export interface AttachmentWrite {
  taskId: string;
  threadId: string;
  descriptors: AttachmentDescriptor[];
}

/**
 * Persist attachment descriptors, idempotent on `id`. Each descriptor's
 * `direction` is honored when present; absent, it defaults to `outbound`
 * (descriptors most commonly arrive on `thread_result`, i.e. agent-produced).
 * A first writer of a given `id` wins; later duplicates are ignored.
 */
export function writeAttachmentDescriptors(
  db: AppDatabase,
  { taskId, threadId, descriptors }: AttachmentWrite,
): void {
  if (descriptors.length === 0) {
    return;
  }
  db.insert(attachments)
    .values(
      descriptors.map(d => ({
        id: d.id,
        taskId,
        threadId,
        name: d.name,
        contentType: d.content_type,
        size: d.size,
        sha256: d.sha256,
        ref: d.ref,
        direction: d.direction ?? "outbound",
      })),
    )
    .onConflictDoNothing({ target: attachments.id })
    .run();
}
