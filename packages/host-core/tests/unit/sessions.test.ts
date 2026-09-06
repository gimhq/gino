import type { WsLike } from "../../src/agents/sessions";
import { describe, expect, it } from "bun:test";
import { SessionRegistry } from "../../src/agents/sessions";

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

describe("M3 sessions — closeAll (host shutdown)", () => {
  it("detaches every session and closes each socket with the given code/reason", () => {
    const registry = new SessionRegistry();
    const a = fakeWs();
    const b = fakeWs();
    registry.attach("a1", a);
    registry.attach("a2", b);

    expect(registry.closeAll(1001, "host shutting down")).toBe(2);
    expect(registry.isOnline("a1")).toBe(false);
    expect(registry.isOnline("a2")).toBe(false);
    expect(a.closed).toEqual([{ code: 1001, reason: "host shutting down" }]);
    expect(b.closed).toEqual([{ code: 1001, reason: "host shutting down" }]);
  });

  it("a throwing socket does not stop the loop", () => {
    const registry = new SessionRegistry();
    const broken: WsLike = {
      send: () => {},
      close: () => {
        throw new Error("gone");
      },
    };
    const ok = fakeWs();
    registry.attach("a1", broken);
    registry.attach("a2", ok);

    expect(registry.closeAll(1001, "host shutting down")).toBe(2);
    expect(registry.isOnline("a1")).toBe(false);
    expect(registry.isOnline("a2")).toBe(false);
    expect(ok.closed).toEqual([{ code: 1001, reason: "host shutting down" }]);
  });

  it("an empty registry closes nothing", () => {
    expect(new SessionRegistry().closeAll(1001, "host shutting down")).toBe(0);
  });
});

describe("M3 sessions — closeAgent (AgentTransport port)", () => {
  it("detaches the agent and closes its socket with the given code/reason", () => {
    const registry = new SessionRegistry();
    const ws = fakeWs();
    registry.attach("a1", ws);

    expect(registry.closeAgent("a1", 4001, "removed")).toBe(true);
    expect(registry.isOnline("a1")).toBe(false);
    expect(ws.closed).toEqual([{ code: 4001, reason: "removed" }]);
  });

  it("closeAgent of an unknown agent returns false", () => {
    const registry = new SessionRegistry();
    expect(registry.closeAgent("nobody", 4001, "removed")).toBe(false);
  });

  it("a throwing socket is swallowed and the session is still detached", () => {
    const registry = new SessionRegistry();
    const broken: WsLike = {
      send: () => {},
      close: () => {
        throw new Error("gone");
      },
    };
    registry.attach("a1", broken);

    expect(registry.closeAgent("a1", 4001, "removed")).toBe(true);
    expect(registry.isOnline("a1")).toBe(false);
  });
});
