// HOST-M9-05 — with a proxy allow-list configured, an unresolvable peer
// address must fail CLOSED (ignore forwarding headers), not fall through to
// trusting X-Forwarded-For from an unverified peer.
import type { Context } from "hono";
import { describe, expect, it } from "bun:test";
import { getClientIp, isSpoofableProxyConfig } from "@/shared/lib/client-ip";

function fakeCtx(peerIp: string | undefined, headers: Record<string, string> = {}): Context {
  return {
    env: peerIp ? { IP: { address: peerIp } } : {},
    req: { header: () => headers },
  } as unknown as Context;
}

describe("getClientIp allow-list gate (HOST-M9-05)", () => {
  const config = { TRUST_PROXY: true, TRUSTED_PROXY_IPS: "10.0.0.5" };

  it("fails closed when the peer IP is unavailable", () => {
    const c = fakeCtx(undefined, { "x-forwarded-for": "6.6.6.6" });
    expect(getClientIp(c, config)).toBe("unknown");
  });

  it("ignores forwarding headers from a peer outside the allow-list", () => {
    const c = fakeCtx("9.9.9.9", { "x-forwarded-for": "6.6.6.6" });
    expect(getClientIp(c, config)).toBe("9.9.9.9");
  });

  it("honours XFF from an allow-listed peer", () => {
    const c = fakeCtx("10.0.0.5", { "x-forwarded-for": "203.0.113.7" });
    expect(getClientIp(c, config)).toBe("203.0.113.7");
  });

  it("still returns the peer when TRUST_PROXY is off", () => {
    const c = fakeCtx("172.16.0.9", { "x-forwarded-for": "6.6.6.6" });
    expect(getClientIp(c, { TRUST_PROXY: false })).toBe("172.16.0.9");
  });
});

describe("isSpoofableProxyConfig (HOST-M9-08)", () => {
  it("is true when TRUST_PROXY is set without an allow-list", () => {
    expect(isSpoofableProxyConfig({ TRUST_PROXY: true, TRUSTED_PROXY_IPS: "" })).toBe(true);
  });

  it("is false with an allow-list or with TRUST_PROXY off", () => {
    expect(isSpoofableProxyConfig({ TRUST_PROXY: true, TRUSTED_PROXY_IPS: "10.0.0.5" })).toBe(false);
    expect(isSpoofableProxyConfig({ TRUST_PROXY: false, TRUSTED_PROXY_IPS: "" })).toBe(false);
  });
});
