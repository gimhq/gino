import type { ProxyEnv } from "../../src/index";
import { describe, expect, it } from "bun:test";
import { buildProxyConfig, parseModelMap } from "../../src/index";

// Mirrors the config/schema.ts defaults for the PROXY_* slice.
const baseEnv: ProxyEnv = {
  PROXY_ALLOW_PRIVATE_UPSTREAM: false,
  PROXY_ANTHROPIC_API_KEY: "",
  PROXY_ANTHROPIC_BASE_URL: "https://api.anthropic.com",
  PROXY_ANTHROPIC_OAUTH_TOKEN: "",
  PROXY_CODEX_ACCOUNT_ID: "",
  PROXY_CODEX_REFRESH_TOKEN: "",
  PROXY_MODEL_MAP: "{}",
  PROXY_OPENAI_API_KEY: "",
  PROXY_OPENAI_BASE_URL: "https://api.openai.com",
  PROXY_OPENAI_OAUTH_TOKEN: "",
  PROXY_STATIC_KEY: "",
};

describe("M7 proxy/config — parseModelMap", () => {
  it("parses a valid JSON map", () => {
    const map = parseModelMap(JSON.stringify({ "gpt-mini": { provider: "openai", model: "gpt-5-codex-mini" } }));
    expect(map["gpt-mini"]).toEqual({ provider: "openai", model: "gpt-5-codex-mini" });
  });

  it("returns {} for empty / default values", () => {
    expect(parseModelMap("")).toEqual({});
    expect(parseModelMap("{}")).toEqual({});
  });

  it("returns {} for invalid JSON", () => {
    expect(parseModelMap("{not json")).toEqual({});
  });

  it("returns {} for a shape mismatch", () => {
    expect(parseModelMap(JSON.stringify({ alias: { provider: "google", model: 5 } }))).toEqual({});
  });
});

describe("M7 proxy/config — buildProxyConfig", () => {
  it("defaults upstream origins and leaves credentials unset", () => {
    const proxy = buildProxyConfig(baseEnv);
    expect(proxy.anthropic.baseUrl.origin).toBe("https://api.anthropic.com");
    expect(proxy.openai.baseUrl.origin).toBe("https://api.openai.com");
    expect(proxy.anthropic.apiKey).toBeUndefined();
    expect(proxy.staticKey).toBeUndefined();
    expect(proxy.modelMap).toEqual({});
  });

  it("projects credentials, static key, codex, and the model map", () => {
    const proxy = buildProxyConfig({
      ...baseEnv,
      PROXY_ANTHROPIC_API_KEY: "sk-ant-real",
      PROXY_OPENAI_OAUTH_TOKEN: "oauth-tok",
      PROXY_STATIC_KEY: "static-secret",
      PROXY_CODEX_REFRESH_TOKEN: "rt",
      PROXY_CODEX_ACCOUNT_ID: "acct",
      PROXY_MODEL_MAP: JSON.stringify({ house: { provider: "anthropic", model: "claude-opus-4-8" } }),
    });
    expect(proxy.anthropic.apiKey).toBe("sk-ant-real");
    expect(proxy.openai.oauthToken).toBe("oauth-tok");
    expect(proxy.staticKey).toBe("static-secret");
    expect(proxy.codex).toEqual({ refreshToken: "rt", accountId: "acct" });
    expect(proxy.modelMap.house).toEqual({ provider: "anthropic", model: "claude-opus-4-8" });
  });

  it("rejects a private-IP upstream by default (SSRF guard)", () => {
    const cfg = { ...baseEnv, PROXY_ANTHROPIC_BASE_URL: "http://127.0.0.1:8080" };
    expect(() => buildProxyConfig(cfg)).toThrow(/private IP/);
  });

  it("allows a private-IP upstream when explicitly opted in", () => {
    const cfg = {
      ...baseEnv,
      PROXY_ANTHROPIC_BASE_URL: "http://127.0.0.1:8080",
      PROXY_ALLOW_PRIVATE_UPSTREAM: true,
    };
    expect(buildProxyConfig(cfg).anthropic.baseUrl.hostname).toBe("127.0.0.1");
  });
});
