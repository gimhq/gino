import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { isUnderLode, markReady, readLodeState, requestRestart, requestUpdate } from "@/platform/lode";

// The lode integration (docs/host-upgrade-design.md §6): the host writes the
// app-owned state.json fields (ready/target/restart_nonce), preserving lode's,
// and is a no-op outside the loader. Env vars are read dynamically, so each test
// sets LODE_DATA_DIR/LODE_INSTANCE against a temp dir.
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

  it("isUnderLode requires both injected vars", () => {
    expect(isUnderLode()).toBe(true);
    delete process.env.LODE_INSTANCE;
    expect(isUnderLode()).toBe(false);
  });

  it("markReady writes ready and preserves lode-owned fields", () => {
    markReady();
    const s = readLodeState();
    expect(s?.ready).toBe("pid-nanoid");
    expect(s?.current).toBe("1.0.0");
    expect(s?.available).toBe("1.1.0");
    expect(s?.status).toBe("running");
  });

  it("requestUpdate sets target", () => {
    requestUpdate("1.1.0");
    expect(readLodeState()?.target).toBe("1.1.0");
  });

  it("requestRestart increments restart_nonce monotonically", () => {
    requestRestart();
    expect(readLodeState()?.restart_nonce).toBe(1);
    requestRestart();
    expect(readLodeState()?.restart_nonce).toBe(2);
  });

  it("is a no-op when not under lode (no ready/target written)", () => {
    delete process.env.LODE_INSTANCE;
    markReady();
    requestUpdate("9.9.9");
    requestRestart();
    const raw = JSON.parse(readFileSync(join(dir, "state.json"), "utf8"));
    expect(raw.ready).toBeUndefined();
    expect(raw.target).toBeUndefined();
    expect(raw.restart_nonce).toBeUndefined();
  });
});
