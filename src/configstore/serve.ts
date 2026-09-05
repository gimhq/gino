// M8 — UNIFIED BYTE-CHANNEL RESOLVER (`GET /agent/attachments` read side).
//
// The byte channel serves from the ONE unified store, addressing a blob by an
// attachment `id` OR a content `sha256`. Resolution looks in `attachments`
// first (the descriptor carries `ref` + `sha256`), then falls back to the
// `config_artifacts` registry (a skill/instruction blob). The caller then reads
// it through `BlobStore.readVerified`, so a corrupt/mismatched blob never
// serves. The host only RESOLVES + SERVES bytes — it never touches the agent FS.
import type { AttachmentRow, Storage } from "@/host/ports/storage";
import { getArtifactBySha } from "./artifacts";

/**
 * One attachment descriptor by its id (the byte channel's id-addressed read).
 * Carries `task_id`, so the edge can authorize the read by task placement before
 * serving bytes (the IDOR fix — see `src/edge/agent.ts`).
 */
export function getAttachmentById(storage: Storage, id: string): AttachmentRow | undefined {
  return storage.attachments.getById(id);
}

/**
 * Every attachment descriptor carrying a content `sha256`. The same content may
 * be referenced by several tasks (content-addressed), so authorization checks
 * placement across ALL of them — an agent placed on ANY referencing task may read
 * those bytes.
 */
export function attachmentsBySha(storage: Storage, sha256: string): AttachmentRow[] {
  return storage.attachments.listBySha(sha256);
}

/** A located blob ready to read+verify+serve. */
export interface ResolvedBlob {
  ref: string;
  sha256: string;
  name: string;
  contentType: string;
  size: number;
}

export interface BlobSelector {
  /** An attachment id (descriptor primary key). */
  id?: string;
  /** A content hash (serves any attachment OR registered artifact with this sha256). */
  sha256?: string;
}

/**
 * Resolve a blob selector to its store ref + metadata, across the unified store.
 * `id` resolves an `attachments` descriptor; `sha256` resolves an attachment by
 * content, else a registered artifact. Returns `null` when nothing matches.
 */
export function resolveBlobRef(storage: Storage, sel: BlobSelector): ResolvedBlob | null {
  if (sel.id) {
    const att = storage.attachments.getById(sel.id);
    if (att) {
      return { ref: att.ref, sha256: att.sha256, name: att.name, contentType: att.contentType, size: att.size };
    }
    return null;
  }
  if (sel.sha256) {
    const att = storage.attachments.listBySha(sel.sha256)[0];
    if (att) {
      return { ref: att.ref, sha256: att.sha256, name: att.name, contentType: att.contentType, size: att.size };
    }
    const artifact = getArtifactBySha(storage, sel.sha256);
    if (artifact) {
      return {
        ref: artifact.storeRef,
        sha256: artifact.sha256,
        name: artifact.name,
        contentType: artifact.contentType ?? "application/octet-stream",
        size: artifact.size,
      };
    }
  }
  return null;
}
