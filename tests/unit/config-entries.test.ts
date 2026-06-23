import type { AppDatabase } from "@/db";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
  getEntries,
  GLOBAL_SCOPE,
  resolveEntries,
  resolveEntriesForEngine,
  resolveTarget,
  setEntries,
  targetEngine,
} from "@/configstore";
import { createDb } from "@/db";
import { createTask, setTaskEntries } from "@/tasks";

// M8 — the per-scope desired set + per-engine `target` resolution. The host
// resolves the landing path; the agent stays engine-agnostic (target/kind only).

let db: AppDatabase;

beforeEach(() => {
  db = createDb(":memory:");
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
      { name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" },
      { name: "reviewer", sha256: "b".repeat(64), kind: "dir" },
    ]);
    setEntries(db, GLOBAL_SCOPE, entries);

    const rows = getEntries(db, GLOBAL_SCOPE);
    expect(rows.map(r => r.target).sort()).toEqual([".claude/skills/reviewer", "CLAUDE.md"]);
  });

  it("REPLACES the desired set (an empty list clears it)", () => {
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" }]));
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "reviewer", sha256: "b".repeat(64), kind: "dir" }]));
    expect(getEntries(db, GLOBAL_SCOPE).map(r => r.target)).toEqual([".claude/skills/reviewer"]);

    setEntries(db, GLOBAL_SCOPE, []);
    expect(getEntries(db, GLOBAL_SCOPE)).toHaveLength(0);
  });

  it("dedups by target (last write wins) — never violates UNIQUE(scope, target)", () => {
    // Two entries resolving to the same instruction file → one row, the later sha256.
    const entries = resolveEntries("claude", [
      { name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" },
      { name: "instructions", sha256: "c".repeat(64), kind: "file" },
    ]);
    setEntries(db, GLOBAL_SCOPE, entries);
    const rows = getEntries(db, GLOBAL_SCOPE);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.target).toBe("CLAUDE.md");
    expect(rows[0]!.sha256).toBe("c".repeat(64));
  });

  it("scopes are independent (global vs a task id)", () => {
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" }]));
    setEntries(db, "task-123", resolveEntries("claude", [{ name: "reviewer", sha256: "b".repeat(64), kind: "dir" }]));
    expect(getEntries(db, GLOBAL_SCOPE)).toHaveLength(1);
    expect(getEntries(db, "task-123")).toHaveLength(1);
    expect(getEntries(db, "task-123")[0]!.target).toBe(".claude/skills/reviewer");
  });
});

describe("delivery — resolveEntriesForEngine filters by engine", () => {
  it("delivers an engine its own targets + universal ones, never another engine's", () => {
    // A multi-engine standard lands as both a CLAUDE.md and an AGENTS.md row,
    // plus a universal design.md.
    setEntries(db, GLOBAL_SCOPE, [
      { name: "CLAUDE.md", sha256: "a".repeat(64), target: "CLAUDE.md", kind: "file" },
      { name: "AGENTS.md", sha256: "a".repeat(64), target: "AGENTS.md", kind: "file" },
      { name: "design.md", sha256: "d".repeat(64), target: "design.md", kind: "file" },
    ]);

    const forClaude = resolveEntriesForEngine(db, GLOBAL_SCOPE, "claude");
    expect(forClaude.map(e => e.target).sort()).toEqual(["CLAUDE.md", "design.md"]);

    const forCodex = resolveEntriesForEngine(db, GLOBAL_SCOPE, "codex");
    expect(forCodex.map(e => e.target).sort()).toEqual(["AGENTS.md", "design.md"]);
  });

  it("returns the wire shape ({name, sha256, target, kind} only)", () => {
    setEntries(db, GLOBAL_SCOPE, resolveEntries("claude", [{ name: "reviewer", sha256: "b".repeat(64), kind: "dir" }]));
    const [entry] = resolveEntriesForEngine(db, GLOBAL_SCOPE, "claude");
    expect(Object.keys(entry!).sort()).toEqual(["kind", "name", "sha256", "target"]);
  });
});

describe("task-scope entries — store wiring", () => {
  it("setTaskEntries persists scope=<task_id> resolved by the task's engine", () => {
    const task = createTask(db, { ownerId: "o", engine: "codex", model: "gpt" });
    setTaskEntries(db, task.id, [
      { name: "instructions", sha256: "a".repeat(64), kind: "file" },
      { name: "reviewer", sha256: "b".repeat(64), kind: "dir" },
    ]);
    const rows = getEntries(db, task.id);
    expect(rows.map(r => r.target).sort()).toEqual([".codex/skills/reviewer", "AGENTS.md"]);
  });

  it("createTask persists its initial entries on create", () => {
    const task = createTask(db, {
      ownerId: "o",
      engine: "claude",
      model: "sonnet",
      entries: [{ name: "CLAUDE.md", sha256: "a".repeat(64), kind: "file" }],
    });
    expect(getEntries(db, task.id).map(r => r.target)).toEqual(["CLAUDE.md"]);
  });
});
