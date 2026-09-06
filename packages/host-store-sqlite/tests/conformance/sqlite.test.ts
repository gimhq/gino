import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describeStorageConformance } from "@gino/host-core/testing";
import { sql } from "drizzle-orm";
import { createDb } from "../../src/db";
import { sqliteStorage } from "../../src/store";

// The sqlite (drizzle + bun:sqlite) `Storage` adapter against the shared
// conformance spec, with the filesystem `BlobStore` rooted in a throwaway
// directory per test.

describeStorageConformance({
  name: "sqlite",
  open: () => {
    const db = createDb(":memory:");
    // The spec exercises each repo in isolation (an outbox row for an agent
    // that was never enrolled, a run for a task never created), so the
    // schema's foreign keys are switched off for the conformance database.
    db.run(sql`PRAGMA foreign_keys = OFF`);
    const storeDir = mkdtempSync(join(tmpdir(), "gino-conformance-store-"));
    const storage = sqliteStorage(db, { storeDir });
    return {
      storage,
      // A ref `<name>/<sha256[:12]>` is the file `<storeDir>/<name>/<sha256[:12]>`.
      tamperBlob: ref => writeFileSync(join(storeDir, ref), "tampered bytes"),
      close: () => {
        db.close();
        rmSync(storeDir, { recursive: true, force: true });
      },
    };
  },
});
