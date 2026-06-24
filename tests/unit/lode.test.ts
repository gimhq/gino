import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { isUnderLode, markReady, readLodeState, requestRestart, requestRollback, requestUpdate } from "@/platform/lode";

// gino's adapter over the vendored lode SDK (docs/host-upgrade-design.md §6): the
// host writes the app-owned state.json fields (ready/target/restart_nonce),
// preserving lode's, and is inert outside the loader. Env is read dynamically, so
// each test sets LODE_DATA_DIR/LODE_INSTANCE against a temp dir.
describe("lode state integration", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "gino-lode-"));
    process.env.LODE_DATA_DIR = dir;
    process.env.LODE_INSTANCE = "pid-nanoid";
    writeFileSync(
      join(dir, "state.json"),
      JSON.stringify({ current: "1.0.0", last_good: "1.0.0", available: "1.1.0", status: "running" }),
    );
  });

  afterEach(() => {
    delete process.env.LODE_DATA_DIR;
    delete process.env.LODE_INSTANCE;
    rmSync(dir, { recursive: true, force: true });
  });

  it("isUnderLode tracks LODE_DATA_DIR (supervision)", () => {
    expect(isUnderLode()).toBe(true);
    delete process.env.LODE_DATA_DIR;
    expect(isUnderLode()).toBe(false);
  });

  it("readLodeState parses lode's fields (camelCase)", () => {
    const s = readLodeState();
    expect(s?.current).toBe("1.0.0");
    expect(s?.lastGood).toBe("1.0.0");
    expect(s?.available).toBe("1.1.0");
    expect(s?.status).toBe("running");
  });

  it("markReady writes ready, preserving lode-owned fields", () => {
    markReady();
    const s = readLodeState();
    expect(s?.ready).toBe("pid-nanoid");
    expect(s?.current).toBe("1.0.0");
    expect(s?.available).toBe("1.1.0");
  });

  it("markReady is a no-op without an instance id", () => {
    delete process.env.LODE_INSTANCE;
    markReady();
    expect(readLodeState()?.ready).toBeUndefined();
  });

  it("requestUpdate sets target", () => {
    requestUpdate("1.1.0");
    expect(readLodeState()?.target).toBe("1.1.0");
  });

  it("requestRestart increments restartNonce monotonically", () => {
    requestRestart();
    expect(readLodeState()?.restartNonce).toBe(1);
    requestRestart();
    expect(readLodeState()?.restartNonce).toBe(2);
  });

  it("requestRollback targets last_good by default", () => {
    expect(requestRollback()).toBe("1.0.0");
    expect(readLodeState()?.target).toBe("1.0.0");
  });

  it("is inert when not supervised (no LODE_DATA_DIR)", () => {
    delete process.env.LODE_DATA_DIR;
    markReady();
    requestUpdate("9.9.9");
    requestRestart();
    const raw = JSON.parse(readFileSync(join(dir, "state.json"), "utf8"));
    expect(raw.ready).toBeUndefined();
    expect(raw.target).toBeUndefined();
    expect(raw.restart_nonce).toBeUndefined();
  });
});
