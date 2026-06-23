// M8 — UNIFIED BYTE-CHANNEL RESOLVER (`GET /agent/attachments` read side).
//
// The byte channel serves from the ONE unified store, addressing a blob by an
// attachment `id` OR a content `sha256`. Resolution looks in `attachments`
// first (the descriptor carries `ref` + `sha256`), then falls back to the
// `config_artifacts` registry (a skill/instruction blob). The caller then reads
// it through `BlobStore.readVerified`, so a corrupt/mismatched blob never
// serves. The host only RESOLVES + SERVES bytes — it never touches the agent FS.
import type { AppDatabase } from "@/db";
import type { Attachment } from "@/db/schema";
import { eq } from "drizzle-orm";
import { attachments } from "@/db/schema";
import { getArtifactBySha } from "./artifacts";

/**
 * One attachment descriptor by its id (the byte channel's id-addressed read).
 * Carries `task_id`, so the edge can authorize the read by task placement before
 * serving bytes (the IDOR fix — see `src/edge/agent.ts`).
 */
export function getAttachmentById(db: AppDatabase, id: string): Attachment | undefined {
  return db.select().from(attachments).where(eq(attachments.id, id)).get();
}

/**
 * Every attachment descriptor carrying a content `sha256`. The same content may
 * be referenced by several tasks (content-addressed), so authorization checks
 * placement across ALL of them — an agent placed on ANY referencing task may read
 * those bytes.
 */
export function attachmentsBySha(db: AppDatabase, sha256: string): Attachment[] {
  return db.select().from(attachments).where(eq(attachments.sha256, sha256)).all();
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
export function resolveBlobRef(db: AppDatabase, sel: BlobSelector): ResolvedBlob | null {
  if (sel.id) {
    const att = db.select().from(attachments).where(eq(attachments.id, sel.id)).get();
    if (att) {
      return { ref: att.ref, sha256: att.sha256, name: att.name, contentType: att.contentType, size: att.size };
    }
    return null;
  }
  if (sel.sha256) {
    const att = db.select().from(attachments).where(eq(attachments.sha256, sel.sha256)).get();
    if (att) {
      return { ref: att.ref, sha256: att.sha256, name: att.name, contentType: att.contentType, size: att.size };
    }
    const artifact = getArtifactBySha(db, sel.sha256);
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
