import type { BlobStorePort, Storage } from "@gino/host-core/domain";
// SQLITE STORE — the drizzle + sqlite `Storage` adapter (host-sdk-design
// §8.2 `host-store-sqlite`): the reference implementation, one repository per
// table group over the existing `AppDatabase`, plus the filesystem `BlobStore`
// for the bytes. `channels` is always present here.
import type { AppDatabase } from "../driver";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BlobStore } from "@gino/host-core/domain";
import { sql } from "drizzle-orm";
import { sqliteAgentRepo } from "./agents";
import { sqliteAttachmentRepo } from "./attachments";
import { sqliteChannelRepo } from "./channels";
import { sqliteConfigRepo } from "./config";
import { sqliteCronMirrorRepo } from "./crons";
import { sqliteOutboxRepo } from "./outbox";
import { sqliteTaskRepo } from "./tasks";
import { sqliteThreadRunRepo } from "./thread-runs";
import { sqliteTranscriptRepo } from "./transcript";

export { sqliteAgentRepo } from "./agents";
export { sqliteAttachmentRepo } from "./attachments";
export { sqliteChannelRepo } from "./channels";
export { sqliteConfigRepo } from "./config";
export { sqliteCronMirrorRepo } from "./crons";
export { sqliteOutboxRepo } from "./outbox";
export { sqliteTaskRepo } from "./tasks";
export { sqliteThreadRunRepo } from "./thread-runs";
export { sqliteTranscriptRepo } from "./transcript";

export interface SqliteStorageOptions {
  /** An already-built byte store; wins over `storeDir`. */
  readonly blobs?: BlobStorePort;
  /** Root directory for the filesystem `BlobStore` (the host's `options.storeDir`). */
  readonly storeDir?: string;
}

/**
 * Build a `Storage` over an open `AppDatabase`. Without `blobs` / `storeDir`
 * the byte store defaults to a fresh `mkdtemp` directory created LAZILY on the
 * first `storage.blobs` access — so `sqliteStorage(db)` in a unit test costs
 * nothing until bytes are actually stored.
 */
export function sqliteStorage(db: AppDatabase, opts: SqliteStorageOptions = {}): Storage {
  let blobs = opts.blobs ?? (opts.storeDir !== undefined ? new BlobStore(opts.storeDir) : undefined);
  return {
    agents: sqliteAgentRepo(db),
    tasks: sqliteTaskRepo(db),
    outbox: sqliteOutboxRepo(db),
    transcript: sqliteTranscriptRepo(db),
    threadRuns: sqliteThreadRunRepo(db),
    crons: sqliteCronMirrorRepo(db),
    attachments: sqliteAttachmentRepo(db),
    config: sqliteConfigRepo(db),
    channels: sqliteChannelRepo(db),
    get blobs() {
      blobs ??= new BlobStore(mkdtempSync(join(tmpdir(), "gino-store-")));
      return blobs;
    },
    ping() {
      db.run(sql`SELECT 1`);
    },
    close() {
      db.close();
    },
    checkpoint() {
      db.checkpoint();
    },
  };
}
