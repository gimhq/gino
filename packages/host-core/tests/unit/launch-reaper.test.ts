import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Launcher } from "../../src/agents/launchers/types";
import type { Storage } from "../../src/host/ports/storage";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { reapPendingLaunches } from "../../src/agents/launch-reaper";
import {
  correlateLaunch,
  enrollAgent,
  getAgent,
  hasPendingLaunch,
  recordPendingLaunch,
} from "../../src/agents/registry";

// HOST-M3-05 — pending launches get a TTL + reaper (runtime torn down on
// expiry, entry dropped) and are PERSISTED so a host restart orphans nothing.

let db: AppDatabase;
let storage: Storage;

beforeEach(() => {
  db = createDb(":memory:");
  storage = sqliteStorage(db);
});

afterEach(() => {
  db.close();
});

/** A recording launcher whose `remove` can be made to fail. */
function fakeLauncher(opts: { failRemove?: boolean } = {}) {
  const removed: string[] = [];
  const launcher: Launcher = {
    kind: "docker",
    capabilities: { canLaunch: true, canStop: true, canRestart: true, canInspect: true },
    launch: async () => ({ ref: "unused" }),
    stop: async () => {},
    restart: async () => {},
    remove: async (ref: string) => {
      if (opts.failRemove) {
        throw new Error("docker daemon wedged");
      }
      removed.push(ref);
    },
    inspect: async () => "unknown" as const,
  };
  return { launcher, removed };
}

describe("HOST-M3-05 — pending-launch TTL reaper", () => {
  it("tears down the runtime and drops the entry once the TTL expires", async () => {
    const { launcher, removed } = fakeLauncher();
    recordPendingLaunch(storage, "nonce-old", { launcherKind: "docker", launchRef: "ctr-orphan" });

    // Inside the TTL nothing is touched.
    let clock = Date.now();
    const deps = { storage, ttlMs: 1000, now: () => clock, resolveLauncher: () => launcher };
    expect(await reapPendingLaunches(deps)).toBe(0);
    expect(hasPendingLaunch(storage, "nonce-old")).toBe(true);
    expect(removed).toEqual([]);

    // Past the TTL the runtime is removed and the entry dropped.
    clock += 5000;
    expect(await reapPendingLaunches(deps)).toBe(1);
    expect(hasPendingLaunch(storage, "nonce-old")).toBe(false);
    expect(removed).toEqual(["ctr-orphan"]);
  });

  it("a consumed (correlated) launch is never reaped", async () => {
    const { launcher, removed } = fakeLauncher();
    const { agent } = enrollAgent(storage, { source: "docker", capabilities: { engines: ["claude"] } });
    recordPendingLaunch(storage, "nonce-used", { launcherKind: "docker", launchRef: "ctr-live" });
    expect(correlateLaunch(storage, agent.id, "nonce-used")).not.toBeNull();

    const clock = Date.now() + 60_000;
    expect(await reapPendingLaunches({ storage, ttlMs: 1000, now: () => clock, resolveLauncher: () => launcher })).toBe(0);
    expect(removed).toEqual([]); // the live agent's container is untouched
  });

  it("a teardown failure (wedged daemon) still drops the entry — pending state stays bounded", async () => {
    const { launcher } = fakeLauncher({ failRemove: true });
    recordPendingLaunch(storage, "nonce-bad", { launcherKind: "docker", launchRef: "ctr-bad" });

    const clock = Date.now() + 60_000;
    expect(await reapPendingLaunches({ storage, ttlMs: 1000, now: () => clock, resolveLauncher: () => launcher })).toBe(1);
    expect(hasPendingLaunch(storage, "nonce-bad")).toBe(false);
  });

  it("a launch that started no runtime (null ref) is dropped without a launcher call", async () => {
    const { launcher, removed } = fakeLauncher();
    recordPendingLaunch(storage, "nonce-noref", { launcherKind: "docker", launchRef: null });

    const clock = Date.now() + 60_000;
    expect(await reapPendingLaunches({ storage, ttlMs: 1000, now: () => clock, resolveLauncher: () => launcher })).toBe(1);
    expect(removed).toEqual([]);
    expect(hasPendingLaunch(storage, "nonce-noref")).toBe(false);
  });
});

describe("HOST-M3-05 — pending launches survive a host restart", () => {
  const dbPath = resolve(tmpdir(), `gino-launch-reaper-${process.pid}-${Date.now()}.db`);

  afterEach(() => {
    for (const suffix of ["", "-wal", "-shm"]) {
      try {
        rmSync(`${dbPath}${suffix}`, { force: true });
      }
      catch {}
    }
  });

  it("a late registration after a restart still correlates; a never-registered one is reapable", async () => {
    // "Before the restart": two launches recorded, nobody registered yet.
    let fileDb = createDb(dbPath);
    let fileStorage = sqliteStorage(fileDb);
    recordPendingLaunch(fileStorage, "nonce-late", { launcherKind: "docker", launchRef: "ctr-late" });
    recordPendingLaunch(fileStorage, "nonce-never", { launcherKind: "docker", launchRef: "ctr-never" });
    fileDb.close();

    // "After the restart": the persisted rows are all still there.
    fileDb = createDb(dbPath);
    fileStorage = sqliteStorage(fileDb);
    try {
      expect(hasPendingLaunch(fileStorage, "nonce-late")).toBe(true);

      // The late register correlates as if nothing happened…
      const { agent } = enrollAgent(fileStorage, { source: "docker", capabilities: { engines: ["claude"] } });
      const bound = correlateLaunch(fileStorage, agent.id, "nonce-late");
      expect(bound?.launchRef).toBe("ctr-late");
      expect(getAgent(fileStorage, agent.id)?.launchRef).toBe("ctr-late");

      // …and the never-registered orphan is torn down by the reaper.
      const { launcher, removed } = fakeLauncher();
      const clock = Date.now() + 3_600_000;
      expect(await reapPendingLaunches({ storage: fileStorage, ttlMs: 1000, now: () => clock, resolveLauncher: () => launcher })).toBe(1);
      expect(removed).toEqual(["ctr-never"]);
      expect(hasPendingLaunch(fileStorage, "nonce-never")).toBe(false);
    }
    finally {
      fileDb.close();
    }
  });
});
