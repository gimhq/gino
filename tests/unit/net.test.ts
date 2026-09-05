// Shared SSRF guard — `isPrivateUpstreamHost` classifies literal IPs on both
// families (fail-closed on unparseable literals) and ignores hostnames.
import { describe, expect, it } from "bun:test";
import { isPrivateUpstreamHost } from "@/shared/lib/net";

describe("isPrivateUpstreamHost", () => {
  it("rejects loopback addresses", () => {
    expect(isPrivateUpstreamHost("127.0.0.1")).toBe(true);
    expect(isPrivateUpstreamHost("::1")).toBe(true);
  });

  it("rejects private ranges", () => {
    expect(isPrivateUpstreamHost("10.0.0.1")).toBe(true);
    expect(isPrivateUpstreamHost("192.168.1.1")).toBe(true);
    expect(isPrivateUpstreamHost("fc00::1")).toBe(true);
  });

  it("rejects link-local addresses", () => {
    expect(isPrivateUpstreamHost("169.254.1.1")).toBe(true);
    expect(isPrivateUpstreamHost("fe80::1")).toBe(true);
  });

  it("rejects IPv4-mapped loopback", () => {
    expect(isPrivateUpstreamHost("::ffff:127.0.0.1")).toBe(true);
  });

  it("treats an unparseable literal as a non-IP hostname", () => {
    // `net.isIP("::gggg")` is 0, so the guard never reaches the IPv6 parser and
    // defers to the caller's hostname handling (DNS re-check at delivery time).
    expect(isPrivateUpstreamHost("::gggg")).toBe(false);
  });

  it("allows public addresses", () => {
    expect(isPrivateUpstreamHost("93.184.216.34")).toBe(false);
    expect(isPrivateUpstreamHost("2606:2800:220:1:248:1893:25c8:1946")).toBe(false);
  });

  it("ignores hostnames", () => {
    expect(isPrivateUpstreamHost("api.anthropic.com")).toBe(false);
  });
});
