// MEMORY STORE — the in-memory `Storage` adapter (host-sdk-design §8.2
// `host-store-memory`): every repository backed by Maps/arrays, for tests and
// prototypes. Ordering matches what the sqlite queries return (see the port's
// ordering rule); `close()` drops everything; `ping()` is a no-op; there is no
// `checkpoint`.
import type { Storage } from "@/host/ports/storage";
import { memoryAgentRepo } from "./agents";
import { memoryAttachmentRepo } from "./attachments";
import { memoryBlobStore } from "./blobs";
import { memoryChannelRepo } from "./channels";
import { memoryConfigRepo } from "./config";
import { memoryCronMirrorRepo } from "./crons";
import { memoryOutboxRepo } from "./outbox";
import { memoryTaskRepo } from "./tasks";
import { memoryThreadRunRepo } from "./thread-runs";
import { memoryTranscriptRepo } from "./transcript";

export interface MemoryStorageForTests {
  storage: Storage;
  /** Corrupt the bytes behind a blob ref (conformance: `readVerified` must then throw `ShaMismatchError`). */
  tamperBlob: (ref: string) => void;
}

function buildRepos() {
  return {
    agents: memoryAgentRepo(),
    tasks: memoryTaskRepo(),
    outbox: memoryOutboxRepo(),
    transcript: memoryTranscriptRepo(),
    threadRuns: memoryThreadRunRepo(),
    crons: memoryCronMirrorRepo(),
    attachments: memoryAttachmentRepo(),
    config: memoryConfigRepo(),
    blobs: memoryBlobStore(),
    channels: memoryChannelRepo(),
  };
}

/** A fresh, empty in-memory `Storage` plus the test-only blob tamper hook. */
export function memoryStorageForTests(): MemoryStorageForTests {
  let repos = buildRepos();
  let closed = false;
  const storage: Storage = {
    get agents() {
      return repos.agents;
    },
    get tasks() {
      return repos.tasks;
    },
    get outbox() {
      return repos.outbox;
    },
    get transcript() {
      return repos.transcript;
    },
    get threadRuns() {
      return repos.threadRuns;
    },
    get crons() {
      return repos.crons;
    },
    get attachments() {
      return repos.attachments;
    },
    get config() {
      return repos.config;
    },
    get blobs() {
      return repos.blobs;
    },
    get channels() {
      return repos.channels;
    },
    ping() {
      if (closed) {
        throw new Error("memory storage is closed");
      }
    },
    close() {
      // Drop every repo: all state is released and a later `ping` fails.
      closed = true;
      repos = buildRepos();
    },
  };
  return { storage, tamperBlob: ref => repos.blobs.tamper(ref) };
}

/** A fresh, empty in-memory `Storage`. */
export function memoryStorage(): Storage {
  return memoryStorageForTests().storage;
}
