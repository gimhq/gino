import type { WsLike } from "@/agents/sessions";
import { describe, expect, it } from "bun:test";
import { SessionRegistry } from "@/agents/sessions";

// M3 sessions — the supersede/detach contract the WS `onClose` status guard
// builds on (HOST-M1M2-05 / HOST-M3-02): a superseded socket's late close must
// report "not evicted" so the caller skips the `offline` write.

function fakeWs(): WsLike & { closed: Array<{ code?: number | undefined; reason?: string | undefined }> } {
  const closed: Array<{ code?: number | undefined; reason?: string | undefined }> = [];
  return {
    closed,
    send: () => {},
    close: (code?: number, reason?: string) => {
      closed.push({ code, reason });
    },
  };
}

describe("M3 sessions — detach eviction contract", () => {
  it("detach of the current socket evicts and returns true", () => {
    const registry = new SessionRegistry();
    const ws = fakeWs();
    registry.attach("a1", ws);

    expect(registry.detach("a1", ws)).toBe(true);
    expect(registry.isOnline("a1")).toBe(false);
  });

  it("a superseded socket's detach returns false and keeps the live session", () => {
    const registry = new SessionRegistry();
    const stale = fakeWs();
    const fresh = fakeWs();
    registry.attach("a1", stale);
    // Dirty reconnect: the new socket supersedes (closes) the stale one.
    registry.attach("a1", fresh);
    expect(stale.closed).toHaveLength(1);

    // The stale socket's late close must NOT evict the fresh session.
    expect(registry.detach("a1", stale)).toBe(false);
    expect(registry.isOnline("a1")).toBe(true);
    expect(registry.get("a1")?.ws).toBe(fresh);
  });

  it("detach of an unknown agent returns false", () => {
    const registry = new SessionRegistry();
    expect(registry.detach("nobody")).toBe(false);
  });

  it("compares wrapper handles by their raw socket (per-event WSContext rewrap)", () => {
    const registry = new SessionRegistry();
    const rawSocket = { tag: "bun-server-ws" };
    // The hono/bun adapter wraps the SAME raw socket in a FRESH context per
    // event — onOpen's and onClose's handles differ by identity but not by raw.
    const openHandle = { ...fakeWs(), raw: rawSocket };
    const closeHandle = { ...fakeWs(), raw: rawSocket };
    registry.attach("a1", openHandle);

    expect(registry.detach("a1", closeHandle)).toBe(true);
    expect(registry.isOnline("a1")).toBe(false);
  });

  it("detach without a socket evicts unconditionally", () => {
    const registry = new SessionRegistry();
    registry.attach("a1", fakeWs());
    expect(registry.detach("a1")).toBe(true);
    expect(registry.isOnline("a1")).toBe(false);
  });
});
