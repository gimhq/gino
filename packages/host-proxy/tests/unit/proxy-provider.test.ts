import { describe, expect, it } from "bun:test";
import {
  buildResponseHeaders,
  buildUpstreamHeaders,
  detectProvider,
  extractProxyCredential,
  isAllowedUpstreamPath,
  isAnthropicPassthroughPath,
  isAnthropicTempKey,
  isPrivateUpstreamHost,
} from "../../src/index";

const h = (init: Record<string, string>) => new Headers(init);

describe("M7 proxy/provider — path allow-list", () => {
  it("accepts the known upstream subpaths", () => {
    for (const p of ["/v1/messages", "/v1/chat/completions", "/v1/responses", "/v1/models", "/api/oauth/token"])
      expect(isAllowedUpstreamPath(p)).toBe(true);
  });

  it("rejects unknown subpaths", () => {
    for (const p of ["/", "/v1/embeddings", "/admin", "/v2/messages", "/api/other"])
      expect(isAllowedUpstreamPath(p)).toBe(false);
  });

  it("pass-through paths are the anthropic-direct ones only (temp-key auth scope)", () => {
    expect(isAnthropicPassthroughPath("/v1/messages")).toBe(true);
    expect(isAnthropicPassthroughPath("/v1/models")).toBe(true);
    // Host-credential-injecting paths must never be temp-key-authenticated.
    expect(isAnthropicPassthroughPath("/api/oauth/token")).toBe(false);
    expect(isAnthropicPassthroughPath("/v1/chat/completions")).toBe(false);
    expect(isAnthropicPassthroughPath("/v1/responses")).toBe(false);
  });
});

describe("M7 proxy/provider — provider detection", () => {
  it("OpenAI-only paths win over an x-api-key header (HOST-M7-05)", () => {
    expect(detectProvider(h({ "x-api-key": "sk-ant-x" }), "/v1/chat/completions")).toBe("openai");
    expect(detectProvider(h({ "x-api-key": "sk-ant-x" }), "/v1/responses")).toBe("openai");
  });

  it("anthropic-only paths → anthropic (with or without headers)", () => {
    expect(detectProvider(h({}), "/v1/messages")).toBe("anthropic");
    expect(detectProvider(h({}), "/api/oauth/token")).toBe("anthropic");
    expect(detectProvider(h({ authorization: "Bearer tok" }), "/v1/messages")).toBe("anthropic");
  });

  it("shared paths: x-api-key header decides anthropic", () => {
    expect(detectProvider(h({ "x-api-key": "sk-ant-x" }), "/v1/models")).toBe("anthropic");
  });

  it("everything else → openai", () => {
    expect(detectProvider(h({}), "/v1/chat/completions")).toBe("openai");
    expect(detectProvider(h({}), "/v1/responses")).toBe("openai");
    expect(detectProvider(h({}), "/v1/models")).toBe("openai");
  });
});

describe("M7 proxy/provider — credential extraction", () => {
  it("prefers x-internal-token", () => {
    expect(extractProxyCredential(h({ "x-internal-token": "tok", "x-api-key": "other" }))).toBe("tok");
  });

  it("strips the sk-ant-proxy- placeholder prefix from x-api-key", () => {
    expect(extractProxyCredential(h({ "x-api-key": "sk-ant-proxy-conn123" }))).toBe("conn123");
  });

  it("falls back to Authorization: Bearer", () => {
    expect(extractProxyCredential(h({ authorization: "Bearer abc" }))).toBe("abc");
  });

  it("returns undefined when nothing is present", () => {
    expect(extractProxyCredential(h({}))).toBeUndefined();
  });

  it("detects a real Anthropic temp key (not the proxy placeholder)", () => {
    expect(isAnthropicTempKey(h({ "x-api-key": "sk-ant-api03-real" }))).toBe(true);
    expect(isAnthropicTempKey(h({ "x-api-key": "sk-ant-proxy-conn" }))).toBe(false);
    expect(isAnthropicTempKey(h({ "x-api-key": "sk-other" }))).toBe(false);
  });
});

describe("M7 proxy/provider — header hygiene", () => {
  it("strips hop-by-hop, internal, and client credential headers", () => {
    const out = buildUpstreamHeaders(h({
      "host": "evil.local",
      "connection": "keep-alive",
      "content-length": "9",
      "x-internal-token": "secret",
      "x-gino-model": "claude-sonnet",
      "x-runner-id": "r1",
      "x-api-key": "sk-ant-proxy-conn",
      "authorization": "Bearer conn",
      "content-type": "application/json",
      "anthropic-version": "2023-06-01",
    }));
    for (const stripped of ["host", "connection", "content-length", "x-internal-token", "x-gino-model", "x-runner-id", "x-api-key", "authorization"])
      expect(out.has(stripped)).toBe(false);
    // Legitimate passthrough headers survive.
    expect(out.get("content-type")).toBe("application/json");
    expect(out.get("anthropic-version")).toBe("2023-06-01");
  });

  it("drops content-encoding/content-length from the response headers", () => {
    const out = buildResponseHeaders(h({ "content-encoding": "gzip", "content-length": "100", "x-keep": "yes" }));
    expect(out.has("content-encoding")).toBe(false);
    expect(out.has("content-length")).toBe(false);
    expect(out.get("x-keep")).toBe("yes");
  });
});

describe("M7 proxy/provider — SSRF guard", () => {
  it("flags private/loopback literal IPs", () => {
    for (const ip of ["127.0.0.1", "10.0.0.5", "172.16.3.4", "192.168.1.1", "169.254.0.1", "::1", "fd00::1", "fe80::1"])
      expect(isPrivateUpstreamHost(ip)).toBe(true);
  });

  it("flags unspecified, CGNAT, multicast, and reserved IPv4 ranges (HOST-M5M6-02)", () => {
    for (const ip of ["0.0.0.0", "0.1.2.3", "100.64.0.1", "100.127.255.255", "192.0.0.1", "192.0.2.1", "198.18.0.1", "198.51.100.7", "203.0.113.9", "224.0.0.1", "240.0.0.1", "255.255.255.255"])
      expect(isPrivateUpstreamHost(ip)).toBe(true);
  });

  it("flags IPv4-embedding and reserved IPv6 forms (HOST-M5M6-02)", () => {
    for (const ip of [
      "::", // unspecified
      "::ffff:127.0.0.1", // IPv4-mapped loopback (dotted tail)
      "::ffff:7f00:1", // IPv4-mapped loopback (hex groups — WHATWG URL serialization)
      "::ffff:169.254.169.254", // IPv4-mapped cloud metadata
      "::ffff:a9fe:a9fe", // same, hex groups
      "::ffff:10.0.0.1", // IPv4-mapped private
      "::127.0.0.1", // deprecated IPv4-compatible
      "64:ff9b::127.0.0.1", // NAT64 wrapping loopback
      "2002:7f00:1::", // 6to4 wrapping loopback
      "100::1", // discard-only
      "ff02::1", // multicast
      "2001:db8::1", // documentation
      "2001::1", // Teredo
    ]) {
      expect(isPrivateUpstreamHost(ip)).toBe(true);
    }
  });

  it("allows public literal IPs and hostnames", () => {
    for (const host of ["8.8.8.8", "1.1.1.1", "9.255.255.255", "100.63.255.255", "100.128.0.0", "223.255.255.255", "api.anthropic.com", "api.openai.com", "2001:4860:4860::8888", "::ffff:8.8.8.8", "::ffff:808:808", "64:ff9b::8.8.8.8", "2002:808:808::"])
      expect(isPrivateUpstreamHost(host)).toBe(false);
  });
});
