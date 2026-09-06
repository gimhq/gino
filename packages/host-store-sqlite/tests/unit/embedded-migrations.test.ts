import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "bun:test";
import { collectMigrationFiles, renderEmbeddedMigrations } from "../../scripts/gen-embedded-migrations";
import { embeddedMigrations } from "../../src/embedded-migrations";

// D9: the runtime migrates from the committed `src/embedded-migrations.ts`
// only, so it must mirror `drizzle/**` exactly. After `drizzle-kit generate`,
// run `bun run gen:migrations` and commit the result.
describe("embedded migrations are in sync with drizzle/", () => {
  it("has the same files with the same contents", () => {
    const onDisk = collectMigrationFiles();
    expect([...embeddedMigrations.keys()]).toEqual([...onDisk.keys()]);
    for (const [name, content] of onDisk) {
      expect(embeddedMigrations.get(name)).toBe(content);
    }
  });

  it("lists every journal entry's .sql file", () => {
    const journal = JSON.parse(embeddedMigrations.get("meta/_journal.json")!) as { entries: { tag: string }[] };
    expect(journal.entries.length).toBeGreaterThan(0);
    for (const entry of journal.entries) {
      expect(embeddedMigrations.has(`${entry.tag}.sql`)).toBe(true);
    }
  });

  it("is byte-identical to the generator output", () => {
    const committed = readFileSync(resolve(import.meta.dirname, "../../src/embedded-migrations.ts"), "utf8");
    expect(committed).toBe(renderEmbeddedMigrations(collectMigrationFiles()));
  });
});
