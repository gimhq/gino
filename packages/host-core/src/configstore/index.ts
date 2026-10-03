// M8 — CONFIG STORE (the host-side provisioning source ★).
//
// The host is the SINGLE SOURCE OF TRUTH for all agent config. This module owns
// the host half: the content-addressed blob store + the registry
// index (`config_artifacts`) + the per-scope desired set (`config_entries`) with
// per-engine `target` resolution + the unified byte-channel resolver + the
// refcount/GC. A skill is NOT special — it is a `kind:dir` artifact; an
// instruction file is a `kind:file` artifact. One abstraction.
//
// The host NEVER materializes anything on an agent — it declares the desired set
// (delivered via M3 register/config + M4 `task_upsert`) and serves bytes
// (`/agent/attachments`); the AGENT reconciles/symlinks/GCs its own filesystem.
//
//   store.ts     content-addressed blob store (`store/<name>/<sha256[:12]>`, verify-on-read)
//   artifacts.ts immutable `config_artifacts` registry index (ingest/get/list)
//   entries.ts   `config_entries` desired set + per-engine target resolution + delivery shape
//   serve.ts     unified byte-channel resolver (by id | sha256, across both stores)
//   gc.ts        refcount/LRU GC backing both stores (disabled by default)
import { BlobStore } from "./store";

export const MODULE = "configstore" as const;

/** Build the unified blob store rooted at `storeDir`. */
export function createStore(storeDir: string): BlobStore {
  return new BlobStore(storeDir);
}

export {
  getArtifact,
  getArtifactBySha,
  ingestArtifact,
  type IngestArtifactInput,
  listArtifacts,
} from "./artifacts";
export {
  clearEntries,
  DEFAULT_ENGINE,
  ENGINE_TARGETS,
  type EntryInput,
  getEntries,
  getGlobalConfigVersion,
  GLOBAL_SCOPE,
  isShaInAgentDesiredSet,
  resolveEntries,
  resolveEntriesForEngine,
  resolveEntriesForEngines,
  resolveTarget,
  setEntries,
  targetEngine,
} from "./entries";
export {
  CONFIG_GC_INTERVAL_MS,
  type ConfigStoreGcDeps,
  type ConfigStoreGcHandle,
  DEFAULT_MAX_AGE_MS,
  type DroppableArtifact,
  findDroppable,
  referencedShas,
  startConfigStoreGc,
  sweep,
  type SweepResult,
} from "./gc";
export {
  attachmentsBySha,
  type BlobSelector,
  getAttachmentById,
  resolveBlobRef,
  type ResolvedBlob,
} from "./serve";
export {
  BlobStore,
  ConfigStoreError,
  isSafeName,
  MissingBlobError,
  sanitizeAttachmentName,
  sha256Hex,
  SHA_PREFIX_LEN,
  ShaMismatchError,
  type StoredBlob,
  type StoreLeaf,
  storeRefFor,
} from "./store";
