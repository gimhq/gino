import type { Storage } from "@/host/ports/storage";
import { sql } from "drizzle-orm";
import { createDb } from "@/db";
import { sqliteAgentRepo } from "@/store/sqlite/agents";
import { sqliteOutboxRepo } from "@/store/sqlite/outbox";
import { sqliteTaskRepo } from "@/store/sqlite/tasks";
import { sqliteThreadRunRepo } from "@/store/sqlite/thread-runs";
import { sqliteTranscriptRepo } from "@/store/sqlite/transcript";
import {
  describeAgentRepoConformance,
  describeOutboxRepoConformance,
  describeTaskRepoConformance,
  describeThreadRunRepoConformance,
  describeTranscriptRepoConformance,
} from "./storage.conformance";

// The sqlite (drizzle + bun:sqlite) `Storage` adapter against the shared
// conformance spec — part 1: the five repos that exist so far. The partial
// object is cast because the remaining repos land with part 2.

const target = {
  name: "sqlite",
  open: () => {
    const db = createDb(":memory:");
    // The spec exercises each repo in isolation (an outbox row for an agent
    // that was never enrolled, a run for a task never created), so the
    // schema's foreign keys are switched off for the conformance database.
    db.run(sql`PRAGMA foreign_keys = OFF`);
    const storage = {
      agents: sqliteAgentRepo(db),
      tasks: sqliteTaskRepo(db),
      outbox: sqliteOutboxRepo(db),
      transcript: sqliteTranscriptRepo(db),
      threadRuns: sqliteThreadRunRepo(db),
    } as unknown as Storage;
    return {
      storage,
      tamperBlob: () => {
        throw new Error("not in part 1");
      },
      close: () => db.close(),
    };
  },
};

describeAgentRepoConformance(target);
describeTaskRepoConformance(target);
describeOutboxRepoConformance(target);
describeTranscriptRepoConformance(target);
describeThreadRunRepoConformance(target);
