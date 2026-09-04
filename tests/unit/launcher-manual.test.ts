import { describe, expect, it } from "bun:test";
import { LauncherRegistry, manualLauncher } from "@/agents/launchers";
import { UnsupportedLauncherOp } from "@/agents/launchers/types";

// M3 launchers — the manual (default) launcher + the kind→launcher registry.

describe("M3 manual launcher — safe by default", () => {
  it("is the 'manual' kind with all capabilities false", () => {
    expect(manualLauncher.kind).toBe("manual");
    expect(manualLauncher.capabilities).toEqual({
      canLaunch: false,
      canStop: false,
      canRestart: false,
      canInspect: false,
    });
  });

  it("launch starts nothing (ref null)", async () => {
    expect(await manualLauncher.launch({
      hostUrl: "http://host",
      registerToken: "tok",
      workload: {},
    })).toEqual({ ref: null });
  });

  it("inspect → 'unknown' (host knows no runtime)", async () => {
    expect(await manualLauncher.inspect("any-ref")).toBe("unknown");
  });

  it("stop/restart/remove throw UnsupportedLauncherOp (direct misuse is loud)", async () => {
    await expect(manualLauncher.stop("r")).rejects.toBeInstanceOf(UnsupportedLauncherOp);
    await expect(manualLauncher.restart("r")).rejects.toBeInstanceOf(UnsupportedLauncherOp);
    await expect(manualLauncher.remove("r")).rejects.toBeInstanceOf(UnsupportedLauncherOp);
  });
});

describe("M3 launcher registry — default + lookup", () => {
  it("unset/unknown kind resolves to the manual default", () => {
    const registry = new LauncherRegistry([manualLauncher]);
    expect(registry.resolve().kind).toBe("manual");
    expect(registry.resolve(null).kind).toBe("manual");
    expect(registry.resolve("manual").kind).toBe("manual");
    expect(registry.resolve("no-such-launcher").kind).toBe("manual");
  });

  it("an empty registry still resolves manual (the fallback is not a registration)", () => {
    expect(new LauncherRegistry().resolve("manual").kind).toBe("manual");
    expect(new LauncherRegistry().resolve().kind).toBe("manual");
  });

  it("a registered launcher is selected by its kind", () => {
    const fake = {
      kind: "fake-vm",
      capabilities: { canLaunch: true, canStop: true, canRestart: true, canInspect: true },
      launch: async () => ({ ref: "vm-1" }),
      stop: async () => {},
      restart: async () => {},
      remove: async () => {},
      inspect: async () => "running" as const,
    };
    const registry = new LauncherRegistry([manualLauncher]);
    registry.register(fake);
    expect(registry.resolve("fake-vm")).toBe(fake);
    // The manual default is still intact for unknown kinds.
    expect(registry.resolve("still-unknown").kind).toBe("manual");
  });
});
