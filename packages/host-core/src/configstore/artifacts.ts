// M8 — CONFIG ARTIFACTS (the immutable, content-addressed registry index).
//
// `config_artifacts(name, sha256, kind, size, content_type?, store_ref)` is the
// host's index over the blob store: one row per `(name, sha256)`, immutable
// (the PK is content-addressed, so a re-ingest of identical bytes is a no-op).
// A SKILL is not special — it is a `kind:dir` artifact; an instruction file is
// a `kind:file` artifact. One abstraction, both just content-addressed bytes.
import type { ConfigArtifactRow, Storage } from "../host/ports/storage";

/** Artifact kinds — a directory bundle (skill) or a single file (instruction/config). */
export type ArtifactKind = "dir" | "file";

/** Bytes to store content-addressed, plus the metadata indexed alongside them. */
export interface IngestArtifactInput {
  name: string;
  /** `dir` (skill bundle) | `file` (instruction/config). */
  kind: string;
  bytes: Uint8Array;
  contentType?: string;
}

/**
 * Ingest an artifact: store its bytes content-addressed (dedup by sha256) and
 * upsert the `config_artifacts` row. The CONTENT ADDRESS is immutable (a new
 * version is a new `sha256`, hence a new row) but the metadata OUTSIDE it —
 * `kind` / `content_type` — is last-write-wins: a repeat ingest of identical
 * `(name, sha256)` updates them to the caller's values instead of silently
 * discarding the request (HOST-M8-13 — e.g. re-uploading a bundle wrongly
 * ingested as `kind:file` with `kind:dir` corrects the row).
 */
export function ingestArtifact(storage: Storage, input: IngestArtifactInput): ConfigArtifactRow {
  const blob = storage.blobs.put(input.name, input.bytes);
  return storage.config.upsertArtifact({
    name: blob.name,
    sha256: blob.sha256,
    kind: input.kind,
    size: blob.size,
    contentType: input.contentType ?? null,
    storeRef: blob.ref,
  });
}

/** Fetch one artifact by its content-addressed `(name, sha256)` key. */
export function getArtifact(storage: Storage, name: string, sha256: string): ConfigArtifactRow | undefined {
  return storage.config.getArtifact(name, sha256);
}

/**
 * Fetch any artifact row carrying a given `sha256` (content-addressed lookup for
 * the byte channel's serve-by-sha path). Returns the newest match.
 */
export function getArtifactBySha(storage: Storage, sha256: string): ConfigArtifactRow | undefined {
  return storage.config.getArtifactBySha(sha256);
}

/** List every registered artifact (admin observe), newest first. */
export function listArtifacts(storage: Storage): ConfigArtifactRow[] {
  return storage.config.listArtifacts();
}
