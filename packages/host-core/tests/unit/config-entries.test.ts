import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Storage } from "../../src/host/ports/storage";
import type { ConfigEntry } from "../../src/protocol/frames";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { configArtifacts } from "@gino/host-store-sqlite/schema";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  clearEntries,
  getEntries,
  getGlobalConfigVersion,
  GLOBAL_SCOPE,
  resolveEntries,
  resolveEntriesForEngine,
  resolveEntriesForEngines,
  resolveTarget,
  setEntries,
  targetEngine,
} from "../../src/configstore";
import { ValidationError } from "../../src/shared/lib/errors";
import { closeTask, createTask, setTaskEntries, transitionTaskStatus } from "../../src/tasks";

// M8 — the per-scope desired set + per-engine `target` resolution. The host
// resolves the landing path; the agent stays engine-agnostic (target/kind only).

let db: AppDatabase;
let storage: Storage;

/** Register a `config_artifacts` row so a sha passes write-time validation. */
function seedArtifact(sha: string, name = `seed-${sha.slice(0, 8)}`): void {
  db
    .insert(configArtifacts)
    .values({ name, sha256: sha, kind: "file", size: 1, storeRef: `${name}/${sha.slice(0, 12)}` })
    .onConflictDoNothing()
    .run();
}

const SHA_A = "a".repeat(64);
const SHA_B = "b".repeat(64);
const SHA_C = "c".repeat(64);
const SHA_D = "d".repeat(64);

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
  for (const sha of [SHA_A, SHA_B, SHA_C, SHA_D]) {
    seedArtifact(sha);
  }
});

afterEach(() => {
  db.close();
});

describe("per-engine target resolution", () => {
  it("resolves a skill (kind:dir) under the engine skill dir", () => {
    expect(resolveTarget("claude", "reviewer", "dir")).toBe(".claude/skills/reviewer");
    expect(resolveTarget("codex", "reviewer", "dir")).toBe(".codex/skills/reviewer");
  });

  it("resolves an instruction-role file to the engine instruction file", () => {
    expect(resolveTarget("claude", "CLAUDE.md", "file")).toBe("CLAUDE.md");
    expect(resolveTarget("codex", "instructions", "file")).toBe("AGENTS.md");
    expect(resolveTarget("claude", "instructions", "file")).toBe("CLAUDE.md");
  });

  it("leaves a plain file at its literal (engine-agnostic) name", () => {
    expect(resolveTarget("claude", "design.md", "file")).toBe("design.md");
    expect(resolveTarget("codex", "design.md", "file")).toBe("design.md");
  });

  it("honors an explicit target verbatim (admin-resolved)", () => {
    expect(resolveTarget("claude", "x", "file", "custom/path.md")).toBe("custom/path.md");
  });

  it("targetEngine reverse-maps an engine-owned target, else null (universal)", () => {
    expect(targetEngine("CLAUDE.md")).toBe("claude");
    expect(targetEngine(".claude/skills/foo")).toBe("claude");
    expect(targetEngine("AGENTS.md")).toBe("codex");
    expect(targetEngine("design.md")).toBeNull();
  });
});

describe("desired set — set/get/replace + UNIQUE(scope, target)", () => {
  it("sets and reads back a scope's resolved entries", () => {
    const entries = resolveEntries("claude", [
      { name: "CLAUDE.md", sha256: SHA_A, kind: "file" },
      { name: "reviewer", sha256: SHA_B, kind: "dir" },
    ]);
    setEntries(storage, GLOBAL_SCOPE, entries);

    const rows = getEntries(storage, GLOBAL_SCOPE);
    expect(rows.map(r => r.target).sort()).toEqual([".claude/skills/reviewer", "CLAUDE.md"]);
  });

  it("REPLACES the desired set (an empty list clears it)", () => {
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }]));
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "reviewer", sha256: SHA_B, kind: "dir" }]));
    expect(getEntries(storage, GLOBAL_SCOPE).map(r => r.target)).toEqual([".claude/skills/reviewer"]);

    setEntries(storage, GLOBAL_SCOPE, []);
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(0);
  });

  it("dedups by target (last write wins) — never violates UNIQUE(scope, target)", () => {
    // Two entries resolving to the same instruction file → one row, the later sha256.
    const entries = resolveEntries("claude", [
      { name: "CLAUDE.md", sha256: SHA_A, kind: "file" },
      { name: "instructions", sha256: SHA_C, kind: "file" },
    ]);
    setEntries(storage, GLOBAL_SCOPE, entries);
    const rows = getEntries(storage, GLOBAL_SCOPE);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.target).toBe("CLAUDE.md");
    expect(rows[0]!.sha256).toBe(SHA_C);
  });

  it("scopes are independent (global vs a task id)", () => {
    const task = createTask(storage, { ownerId: "o", engine: "claude", model: "sonnet" });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }]));
    setEntries(storage, task.id, resolveEntries("claude", [{ name: "reviewer", sha256: SHA_B, kind: "dir" }]));
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(1);
    expect(getEntries(storage, task.id)).toHaveLength(1);
    expect(getEntries(storage, task.id)[0]!.target).toBe(".claude/skills/reviewer");
  });
});

describe("global config version — monotonic, bumped only by GLOBAL writes (HOST-M8-01)", () => {
  it("starts at 0 and advances on every global setEntries (including a clear)", () => {
    expect(getGlobalConfigVersion(storage)).toBe(0);

    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }]));
    expect(getGlobalConfigVersion(storage)).toBe(1);

    setEntries(storage, GLOBAL_SCOPE, []);
    expect(getGlobalConfigVersion(storage)).toBe(2);
  });

  it("a task-scope write never touches the global version", () => {
    const task = createTask(storage, { ownerId: "o", engine: "claude", model: "sonnet" });
    setEntries(storage, task.id, resolveEntries("claude", [{ name: "reviewer", sha256: SHA_B, kind: "dir" }]));
    expect(getGlobalConfigVersion(storage)).toBe(0);
  });
});

describe("setEntries is transactional — delete+insert is all-or-nothing (HOST-M8-02)", () => {
  it("a failing insert leaves the previous set intact and the version unmoved", () => {
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }]));
    expect(getGlobalConfigVersion(storage)).toBe(1);

    // A NULL kind passes write-time validation but violates NOT NULL at insert
    // time — AFTER the delete ran.
    const bad = [{ name: "reviewer", sha256: SHA_B, target: ".claude/skills/reviewer", kind: null }] as unknown as ConfigEntry[];
    expect(() => setEntries(storage, GLOBAL_SCOPE, bad)).toThrow();

    // Rolled back: the old desired set survives, the version did not advance.
    expect(getEntries(storage, GLOBAL_SCOPE).map(r => r.target)).toEqual(["CLAUDE.md"]);
    expect(getGlobalConfigVersion(storage)).toBe(1);
  });
});

describe("write-time validation — sha256 + scope (HOST-M8-07)", () => {
  it("rejects an entry whose sha256 no ingested artifact holds (422)", () => {
    const unknownSha = "e".repeat(64);
    expect(() => setEntries(storage, GLOBAL_SCOPE, [{ name: "ghost.md", sha256: unknownSha, target: "ghost.md", kind: "file" }]))
      .toThrow(ValidationError);
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(0);
  });

  it("rejects a malformed sha256 (not 64 lowercase hex)", () => {
    for (const sha of ["abc123", "A".repeat(64), `${"a".repeat(63)}g`, ""]) {
      expect(() => setEntries(storage, GLOBAL_SCOPE, [{ name: "x.md", sha256: sha, target: "x.md", kind: "file" }]))
        .toThrow(ValidationError);
    }
  });

  it("rejects a scope that is neither 'global' nor an existing task id (422)", () => {
    expect(() => setEntries(storage, "no-such-task", [{ name: "x.md", sha256: SHA_A, target: "x.md", kind: "file" }]))
      .toThrow(ValidationError);
  });
});

describe("write-time validation — traversal-shaped name/target (HOST-M8-08)", () => {
  it("rejects a target with traversal / absolute / drive / UNC shapes", () => {
    const badTargets = [
      "../escape.md",
      "a/../b.md",
      ".claude/skills/..",
      "/etc/passwd",
      "//host/share/x",
      "C:/windows/x",
      "c:\\windows\\x",
      "a\\b.md",
      "\\\\host\\share",
      "foo//bar.md",
      "./x.md",
      "",
    ];
    for (const target of badTargets) {
      expect(() => setEntries(storage, GLOBAL_SCOPE, [{ name: "x", sha256: SHA_A, target, kind: "file" }]))
        .toThrow(ValidationError);
    }
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(0);
  });

  it("rejects a name that is not a single safe path segment", () => {
    for (const name of ["..", "a/b", "a\\b", ""]) {
      expect(() => setEntries(storage, GLOBAL_SCOPE, [{ name, sha256: SHA_A, target: "ok.md", kind: "file" }]))
        .toThrow(ValidationError);
    }
  });

  it("accepts normal relative multi-segment targets", () => {
    setEntries(storage, GLOBAL_SCOPE, [{ name: "reviewer", sha256: SHA_B, target: ".claude/skills/reviewer", kind: "dir" }]);
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(1);
  });
});

describe("delivery — resolveEntriesForEngine filters by engine", () => {
  it("delivers an engine its own targets + universal ones, never another engine's", () => {
    // A multi-engine standard lands as both a CLAUDE.md and an AGENTS.md row,
    // plus a universal design.md.
    setEntries(storage, GLOBAL_SCOPE, [
      { name: "CLAUDE.md", sha256: SHA_A, target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: SHA_A, target: "AGENTS.md", kind: "file" },
      { name: "design.md", sha256: SHA_D, target: "design.md", kind: "file" },
    ]);

    const forClaude = resolveEntriesForEngine(storage, GLOBAL_SCOPE, "claude");
    expect(forClaude.map(e => e.target).sort()).toEqual(["CLAUDE.md", "design.md"]);

    const forCodex = resolveEntriesForEngine(storage, GLOBAL_SCOPE, "codex");
    expect(forCodex.map(e => e.target).sort()).toEqual(["AGENTS.md", "design.md"]);
  });

  it("returns the wire shape ({name, sha256, target, kind} only)", () => {
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "reviewer", sha256: SHA_B, kind: "dir" }]));
    const [entry] = resolveEntriesForEngine(storage, GLOBAL_SCOPE, "claude");
    expect(Object.keys(entry!).sort()).toEqual(["kind", "name", "sha256", "target"]);
  });
});

describe("delivery — a multi-engine agent receives the UNION of its engines (HOST-M8-03)", () => {
  it("resolveEntriesForEngines unions every declared engine's targets", () => {
    setEntries(storage, GLOBAL_SCOPE, [
      { name: "CLAUDE.md", sha256: SHA_A, target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: SHA_A, target: "AGENTS.md", kind: "file" },
      { name: "design.md", sha256: SHA_D, target: "design.md", kind: "file" },
    ]);

    const both = resolveEntriesForEngines(storage, GLOBAL_SCOPE, ["claude", "codex"]);
    expect(both.map(e => e.target).sort()).toEqual(["AGENTS.md", "CLAUDE.md", "design.md"]);

    // A single-engine list still filters the other engine out.
    const claudeOnly = resolveEntriesForEngines(storage, GLOBAL_SCOPE, ["claude"]);
    expect(claudeOnly.map(e => e.target).sort()).toEqual(["CLAUDE.md", "design.md"]);
  });

  it("an empty capability list falls back to the default engine", () => {
    setEntries(storage, GLOBAL_SCOPE, [
      { name: "CLAUDE.md", sha256: SHA_A, target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: SHA_A, target: "AGENTS.md", kind: "file" },
    ]);
    const fallback = resolveEntriesForEngines(storage, GLOBAL_SCOPE, []);
    expect(fallback.map(e => e.target)).toEqual(["CLAUDE.md"]);
  });
});

describe("task-scope entries — store wiring", () => {
  it("setTaskEntries persists scope=<task_id> resolved by the task's engine", () => {
    const task = createTask(storage, { ownerId: "o", engine: "codex", model: "gpt" });
    setTaskEntries(storage, task.id, [
      { name: "instructions", sha256: SHA_A, kind: "file" },
      { name: "reviewer", sha256: SHA_B, kind: "dir" },
    ]);
    const rows = getEntries(storage, task.id);
    expect(rows.map(r => r.target).sort()).toEqual([".codex/skills/reviewer", "AGENTS.md"]);
  });

  it("createTask persists its initial entries on create", () => {
    const task = createTask(storage, {
      ownerId: "o",
      engine: "claude",
      model: "sonnet",
      entries: [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }],
    });
    expect(getEntries(storage, task.id).map(r => r.target)).toEqual(["CLAUDE.md"]);
  });
});

describe("task lifecycle cleanup — closing a task drops its scope (HOST-M8-10)", () => {
  it("closeTask removes the task's config_entries rows; global is untouched", () => {
    const task = createTask(storage, {
      ownerId: "o",
      engine: "claude",
      model: "sonnet",
      entries: [{ name: "reviewer", sha256: SHA_B, kind: "dir" }],
    });
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }]));
    expect(getEntries(storage, task.id)).toHaveLength(1);

    closeTask(storage, task.id);

    expect(getEntries(storage, task.id)).toHaveLength(0);
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(1);
  });

  it("transitionTaskStatus to 'closed' also drops the scope (never-placed close path)", () => {
    const task = createTask(storage, {
      ownerId: "o",
      engine: "claude",
      model: "sonnet",
      entries: [{ name: "reviewer", sha256: SHA_B, kind: "dir" }],
    });
    transitionTaskStatus(storage, task.id, "closed");
    expect(getEntries(storage, task.id)).toHaveLength(0);
  });

  it("a non-terminal transition keeps the scope's entries", () => {
    const task = createTask(storage, {
      ownerId: "o",
      engine: "claude",
      model: "sonnet",
      entries: [{ name: "reviewer", sha256: SHA_B, kind: "dir" }],
    });
    transitionTaskStatus(storage, task.id, "active");
    expect(getEntries(storage, task.id)).toHaveLength(1);
  });

  it("clearEntries drops exactly one scope", () => {
    const task = createTask(storage, { ownerId: "o", engine: "claude", model: "sonnet" });
    setEntries(storage, task.id, resolveEntries("claude", [{ name: "reviewer", sha256: SHA_B, kind: "dir" }]));
    setEntries(storage, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: SHA_A, kind: "file" }]));

    clearEntries(storage, task.id);
    expect(getEntries(storage, task.id)).toHaveLength(0);
    expect(getEntries(storage, GLOBAL_SCOPE)).toHaveLength(1);
  });
});
