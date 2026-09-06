import type { Storage } from "../ports/storage";
import type { AttachmentIngestInput, AttachmentsWire, HttpResult, WireContext } from "./index";
import {
  attachmentsBySha,
  getArtifactBySha,
  getAttachmentById,
  isShaInAgentDesiredSet,
  MissingBlobError,
  resolveBlobRef,
  sanitizeAttachmentName,
  sha256Hex,
  ShaMismatchError,
} from "../../configstore";
import { wireAttachmentRef, writeAttachmentDescriptors } from "../../msgstore";
import { generateId } from "../../platform";
import { ForbiddenError, NotFoundError } from "../../shared/lib/errors";
import { getTask } from "../../tasks";

// Attachment byte channel (M8/B6) — bytes ride out-of-band here (never inline
// in frames, protocol §5), verified against the full sha256. The unified store
// backs BOTH attachments and config/skill artifacts, so a skill bundle is
// fetched the same way (no dedicated `/agent/skills`).
//
// Authorization (B6/IDOR fix): the requesting agent identity is the
// connect-token-resolved `requesterId` (set by `connectTokenAuth`), NEVER a
// client-supplied param. Every read/write is gated against it.
//
// `403`-vs-`404` policy (consistent across the byte channel): an UNKNOWN resource
// (no such attachment id/sha, no such config artifact) → `404`; a KNOWN resource
// the requester is not authorized for → `403`.

function badRequest(message: string): HttpResult {
  return { status: 400, body: { success: false, error: { code: "BAD_REQUEST", message } } };
}

/** True iff `agentId` is the agent currently PLACED on `taskId` (set-once, §3). */
function isAgentPlacedOnTask(storage: Storage, agentId: string, taskId: string): boolean {
  const task = getTask(storage, taskId);
  return task != null && task.agentId === agentId;
}

/** True iff a request `If-None-Match` header matches the blob's strong ETag. */
function ifNoneMatchHits(header: string | undefined, etag: string): boolean {
  if (!header) {
    return false;
  }
  return header
    .split(",")
    .map(v => v.trim())
    .some(v => v === "*" || v === etag || v === `W/${etag}`);
}

/**
 * Serve a resolved blob from the unified store, integrity-checked against the
 * full sha256 (B6 byte channel). A missing blob 404s — including a dangling
 * registry/descriptor row whose bytes are gone (HOST-M8-12), never a 500; a
 * stored-bytes/sha256 mismatch is a 409 (the host never serves corrupt/
 * tampered bytes). Responses are content-addressed and immutable, so they
 * carry `Cache-Control: private, immutable` + a strong sha256 `ETag`, and a
 * matching `If-None-Match` short-circuits to a bodyless 304 (HOST-M8-14).
 */
function serveBlob(ctx: WireContext, sel: { id?: string; sha256?: string }, ifNoneMatch: string | undefined): HttpResult {
  const resolved = resolveBlobRef(ctx.storage, sel);
  if (!resolved) {
    throw new NotFoundError("attachment", sel.id ?? sel.sha256 ?? "");
  }
  const etag = `"${resolved.sha256}"`;
  const cacheHeaders = {
    // `private` keeps shared caches out (auth rides the non-standard
    // x-internal-token header, which does not inhibit them); the bytes behind
    // a sha256 never change, hence `immutable`.
    "cache-control": "private, max-age=31536000, immutable",
    "etag": etag,
    "x-content-sha256": resolved.sha256,
  };
  if (ifNoneMatchHits(ifNoneMatch, etag)) {
    return { status: 304, headers: cacheHeaders, body: null };
  }
  try {
    const bytes = ctx.storage.blobs.readVerified(resolved.ref, resolved.sha256);
    return {
      status: 200,
      headers: {
        "content-type": resolved.contentType,
        "content-length": String(bytes.byteLength),
        ...cacheHeaders,
      },
      body: bytes,
    };
  }
  catch (err) {
    if (err instanceof ShaMismatchError) {
      ctx.logger.error({ err, ref: resolved.ref }, "attachment serve: sha256 integrity failure");
      return { status: 409, body: { success: false, error: { code: "BLOB_INTEGRITY", message: "stored bytes failed sha256 verification" } } };
    }
    if (err instanceof MissingBlobError) {
      ctx.logger.error({ err, ref: resolved.ref, sha256: resolved.sha256 }, "attachment serve: registry row points at a missing blob (dangling row — HOST-M8-12)");
      throw new NotFoundError("attachment", sel.id ?? sel.sha256 ?? "");
    }
    throw err;
  }
}

/**
 * Authorize + serve an attachment by descriptor `id`: the requester must be the
 * agent placed on the attachment's `task_id`. Unknown id → 404; wrong agent → 403.
 */
function serveById(ctx: WireContext, requesterId: string, id: string, opts: { ifNoneMatch?: string | undefined }): HttpResult {
  const att = getAttachmentById(ctx.storage, id);
  if (!att) {
    throw new NotFoundError("attachment", id);
  }
  if (!isAgentPlacedOnTask(ctx.storage, requesterId, att.taskId)) {
    throw new ForbiddenError("attachment belongs to another agent's task");
  }
  return serveBlob(ctx, { id }, opts.ifNoneMatch);
}

/**
 * Authorize + serve a blob by content `sha256`. Authorized when EITHER an
 * attachment carrying this sha is on a task placed on the requester, OR the sha
 * is in the requester's desired set (a config/skill artifact the host declared
 * for it). The bytes are content-addressed, so any authorized path serves the
 * same verified content. Unknown sha → 404; known-but-unauthorized → 403.
 */
function serveBySha(ctx: WireContext, requesterId: string, sha256: string, opts: { ifNoneMatch?: string | undefined }): HttpResult {
  const storage = ctx.storage;
  const atts = attachmentsBySha(storage, sha256);
  const artifact = getArtifactBySha(storage, sha256);
  if (atts.length === 0 && !artifact) {
    throw new NotFoundError("attachment", sha256);
  }
  const placed = atts.some(a => isAgentPlacedOnTask(storage, requesterId, a.taskId));
  const inDesiredSet = artifact != null && isShaInAgentDesiredSet(storage, requesterId, sha256);
  if (!placed && !inDesiredSet) {
    throw new ForbiddenError("sha256 is neither on a task placed on this agent nor in its desired set");
  }
  return serveBlob(ctx, { sha256 }, opts.ifNoneMatch);
}

/**
 * Ingest bytes → unified store + an `attachments` descriptor. A declared
 * `sha256` that does not match the computed digest is rejected (the host
 * stores nothing). The adapter has already capped the body (HOST-M1M2-11).
 */
function ingest(ctx: WireContext, requesterId: string, input: AttachmentIngestInput): HttpResult {
  const { name, taskId, threadId, contentType, direction, declaredSha, bytes } = input;
  if (!name) {
    return badRequest("name query param or x-attachment-name header is required");
  }
  // The runner's upload carries NO task/thread context — the binding
  // arrives later on `thread_result.data.attachments` (protocol §5). When
  // a caller DOES bind at upload time it must name both ids, and an agent
  // may write attachments ONLY for its OWN placed tasks — authorize BEFORE
  // storing any bytes (a foreign task_id never touches the store).
  if ((taskId == null) !== (threadId == null)) {
    return badRequest("task_id and thread_id must be provided together");
  }
  if (taskId != null && !isAgentPlacedOnTask(ctx.storage, requesterId, taskId)) {
    throw new ForbiddenError("cannot write an attachment for a task placed on another agent");
  }

  const sha256 = sha256Hex(bytes);
  if (declaredSha && declaredSha !== sha256) {
    return { status: 400, body: { success: false, error: { code: "SHA_MISMATCH", message: `computed ${sha256} != declared ${declaredSha}` } } };
  }

  let blob;
  try {
    // The runner names an outbound file by its `output/`-relative path
    // (separators included) — flatten to a store segment with the same
    // function the descriptor registry uses, so the later
    // `thread_result` registration derives this exact store ref.
    blob = ctx.storage.blobs.put(sanitizeAttachmentName(name), bytes);
  }
  catch (err) {
    return badRequest(err instanceof Error ? err.message : "store rejected the blob");
  }

  // The wire descriptor carries the RESOLVABLE byte-channel ref (the
  // runner parses the flat `{id, ref, …}` body and later fetches `ref`
  // joined to its host base — agent-architecture §14 A1); the store ref
  // stays host-private (derived again at registry-write time).
  const id = generateId("att");
  const descriptor = {
    id,
    name: blob.name,
    content_type: contentType,
    size: blob.size,
    sha256: blob.sha256,
    ref: wireAttachmentRef(id),
    direction,
  };
  if (taskId != null && threadId != null) {
    writeAttachmentDescriptors(ctx.storage, { taskId, threadId, descriptors: [descriptor] });
  }
  ctx.logger.info({ id: descriptor.id, taskId, threadId, sha256: blob.sha256, size: blob.size }, "attachment ingested");
  return { status: 201, body: descriptor };
}

export function attachmentsWire(ctx: WireContext): AttachmentsWire {
  return {
    serveById: (requesterId, id, opts) => serveById(ctx, requesterId, id, opts),
    serveBySha: (requesterId, sha256, opts) => serveBySha(ctx, requesterId, sha256, opts),
    ingest: (requesterId, input) => ingest(ctx, requesterId, input),
  };
}
