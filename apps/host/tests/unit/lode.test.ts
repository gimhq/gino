import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import {
  captureLodeConfigBaseline,
  getLodeSummary,
  reportLodeServing,
  requestLodeRestart,
  requestLodeRollback,
  requestLodeUpdate,
  setLodeHold,
} from "@/lode";

// gino's app glue over the official lode SDK (src/lode, docs/host-upgrade-design
// §6). Supervision keys on $LODE_DIR (lode's own dir, where state.json lives);
// env is read dynamically, so each test points it at a temp dir.
const LODE_ENV = ["LODE_DIR", "LODE_INSTANCE", "LODE_ACTIVE_VERSION", "LODE_READINESS"] as const;
const saved: Record<string, string | undefined> = Object.fromEntries(LODE_ENV.map(k => [k, process.env[k]]));

let dir: string;

function setEnv(values: Record<string, string | undefined>): void {
  for (const [k, v] of Object.entries(values)) {
    if (v === undefined)
      delete process.env[k];
    else
      process.env[k] = v;
  }
}

function writeState(state: Record<string, unknown>): void {
  writeFileSync(join(dir, "state.json"), JSON.stringify(state));
}

function rawState(): Record<string, unknown> {
  return JSON.parse(readFileSync(join(dir, "state.json"), "utf-8")) as Record<string, unknown>;
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gino-lode-"));
  setEnv({ LODE_DIR: dir, LODE_INSTANCE: "inst-1", LODE_ACTIVE_VERSION: undefined, LODE_READINESS: undefined });
  captureLodeConfigBaseline(); // no state.json yet → baseline null
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
  setEnv(saved);
});

describe("getLodeSummary", () => {
  test("reports not supervised when LODE_DIR is unset", () => {
    setEnv({ LODE_DIR: undefined, LODE_INSTANCE: undefined });
    const s = getLodeSummary();
    expect(s.supervised).toBe(false);
    expect(s.active).toBe(false);
    expect(s.stateAvailable).toBe(false);
    expect(s.ready).toBeNull();
    expect(s.history).toEqual([]);
  });

  test("maps state.json into the summary", () => {
    writeState({
      current: "1.0.0",
      last_good: "0.9.0",
      available: "1.1.0",
      channel: "stable",
      status: "running",
      config_generation: 2,
      last_check: "2026-06-24T00:00:00Z",
      last_error: "boom",
      history: [{ version: "1.0.0", at: "t1", result: "good" }, { version: "0.8.0", at: "t0", result: "bad" }],
      ready: "inst-1-0",
      hold: false,
    });
    const s = getLodeSummary();
    expect(s.supervised).toBe(true);
    expect(s.active).toBe(true);
    expect(s.current).toBe("1.0.0");
    expect(s.lastGood).toBe("0.9.0");
    expect(s.available).toBe("1.1.0");
    expect(s.channel).toBe("stable");
    expect(s.updateAvailable).toBe(true);
    expect(s.rollbackTarget).toBe("0.9.0");
    expect(s.ready).toBe(true);
    expect(s.hold).toBe(false);
    expect(s.history).toHaveLength(2);
    expect(s.lastError).toBe("boom");
  });

  test("flags configChanged once the generation advances past the baseline", () => {
    writeState({ config_generation: 3 });
    captureLodeConfigBaseline();
    expect(getLodeSummary().configChanged).toBe(false);
    writeState({ config_generation: 4 });
    expect(getLodeSummary().configChanged).toBe(true);
  });
});

describe("reportLodeServing", () => {
  test("writes the phase-0 serving token", async () => {
    writeState({ current: "1.0.0" });
    expect(await reportLodeServing()).toBe(true);
    expect(rawState().ready).toBe("inst-1-0");
  });

  test("does not report ready when the probe fails", async () => {
    writeState({ current: "1.0.0" });
    expect(await reportLodeServing({ probe: () => false })).toBe(false);
  });
});

describe("lode actions", () => {
  test("requestLodeRestart bumps restart_nonce and preserves fields", () => {
    writeState({ current: "1.0.0", restart_nonce: 4 });
    expect(requestLodeRestart()).toEqual({ status: "ok", restartNonce: 5 });
    const raw = rawState();
    expect(raw.restart_nonce).toBe(5);
    expect(raw.current).toBe("1.0.0");
  });

  test("requestLodeUpdate sets target", () => {
    writeState({});
    expect(requestLodeUpdate("latest")).toEqual({ status: "ok", target: "latest" });
    expect(rawState().target).toBe("latest");
  });

  test("requestLodeRollback uses last_good when no version is given", () => {
    writeState({ current: "1.1.0", last_good: "1.0.0" });
    expect(requestLodeRollback()).toEqual({ status: "ok", target: "1.0.0" });
    expect(rawState().target).toBe("1.0.0");
  });

  test("requestLodeRollback returns no_target without last_good", () => {
    writeState({ current: "1.1.0" });
    expect(requestLodeRollback()).toEqual({ status: "no_target" });
  });

  test("requestLodeRollback accepts an explicit version", () => {
    writeState({});
    expect(requestLodeRollback("0.9.0")).toEqual({ status: "ok", target: "0.9.0" });
  });

  test("setLodeHold sets and clears the hold flag", () => {
    writeState({});
    expect(setLodeHold(true)).toEqual({ status: "ok", hold: true });
    expect(rawState().hold).toBe(true);
    expect(setLodeHold(false)).toEqual({ status: "ok", hold: false });
    expect(rawState().hold).toBe(false);
  });

  test("all actions no-op (not_active) when LODE_DIR is unset", () => {
    setEnv({ LODE_DIR: undefined });
    expect(requestLodeRestart().status).toBe("not_active");
    expect(requestLodeUpdate("latest").status).toBe("not_active");
    expect(requestLodeRollback().status).toBe("not_active");
    expect(setLodeHold(true).status).toBe("not_active");
  });
});
