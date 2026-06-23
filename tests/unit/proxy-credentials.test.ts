import type { FetchLike, ProxyConfig } from "@/proxy";
import { describe, expect, it } from "bun:test";
import { CredentialStore } from "@/proxy";

function makeConfig(overrides: Partial<ProxyConfig> = {}): ProxyConfig {
  return {
    anthropic: { baseUrl: new URL("https://api.anthropic.com") },
    openai: { baseUrl: new URL("https://api.openai.com") },
    codex: {},
    modelMap: {},
    ...overrides,
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

describe("M7 proxy/credentials — anthropic (static)", () => {
  it("prefers apiKey over oauthToken", () => {
    const store = new CredentialStore(makeConfig({ anthropic: { baseUrl: new URL("https://api.anthropic.com"), apiKey: "sk-key", oauthToken: "oauth" } }));
    expect(store.getAnthropic()).toEqual({ apiKey: "sk-key" });
  });

  it("uses oauthToken when no apiKey", () => {
    const store = new CredentialStore(makeConfig({ anthropic: { baseUrl: new URL("https://api.anthropic.com"), oauthToken: "oauth" } }));
    expect(store.getAnthropic()).toEqual({ oauthToken: "oauth" });
  });

  it("empty when neither configured", () => {
    const store = new CredentialStore(makeConfig());
    expect(store.getAnthropic()).toEqual({});
  });
});

describe("M7 proxy/credentials — openai (static)", () => {
  it("prefers a static apiKey and never refreshes", async () => {
    let calls = 0;
    const fetchImpl: FetchLike = async () => {
      calls++;
      return jsonResponse({});
    };
    const store = new CredentialStore(
      makeConfig({ openai: { baseUrl: new URL("https://api.openai.com"), apiKey: "sk-openai" }, codex: { refreshToken: "rt" } }),
      { fetchImpl },
    );
    expect(await store.getOpenAI()).toEqual({ apiKey: "sk-openai" });
    expect(calls).toBe(0);
  });

  it("returns the static oauth token + accountId when no refresh token", async () => {
    const store = new CredentialStore(makeConfig({
      openai: { baseUrl: new URL("https://api.openai.com"), oauthToken: "static-oauth" },
      codex: { accountId: "acct_1" },
    }));
    expect(await store.getOpenAI()).toEqual({ oauthToken: "static-oauth", accountId: "acct_1" });
  });
});

describe("M7 proxy/credentials — codex OAuth refresh", () => {
  it("refreshes via the injected fetch and forwards the account id", async () => {
    let calls = 0;
    const fetchImpl: FetchLike = async () => {
      calls++;
      return jsonResponse({ access_token: "fresh-token", expires_in: 3600 });
    };
    const store = new CredentialStore(
      makeConfig({ codex: { refreshToken: "rt-1", accountId: "acct_9" } }),
      { fetchImpl, now: () => 1000 },
    );
    const cred = await store.getOpenAI();
    expect(cred).toEqual({ oauthToken: "fresh-token", accountId: "acct_9" });
    expect(calls).toBe(1);
  });

  it("caches the access token until near expiry", async () => {
    let calls = 0;
    let clock = 1000;
    const fetchImpl: FetchLike = async () => {
      calls++;
      return jsonResponse({ access_token: `tok-${calls}`, expires_in: 3600 });
    };
    const store = new CredentialStore(
      makeConfig({ codex: { refreshToken: "rt" } }),
      { fetchImpl, now: () => clock },
    );
    expect((await store.getOpenAI()).oauthToken).toBe("tok-1");
    clock = 1000 + 60_000; // 1 min later — still well within the 1h token
    expect((await store.getOpenAI()).oauthToken).toBe("tok-1");
    expect(calls).toBe(1);
    // Advance past expiry (minus the 5m buffer) → refresh again.
    clock = 1000 + 3600_000;
    expect((await store.getOpenAI()).oauthToken).toBe("tok-2");
    expect(calls).toBe(2);
  });

  it("rotates the refresh token when the response carries a new one", async () => {
    const seen: string[] = [];
    let clock = 0;
    const fetchImpl: FetchLike = async (_url, init) => {
      const body = new URLSearchParams(String(init?.body ?? ""));
      seen.push(body.get("refresh_token") ?? "");
      return jsonResponse({ access_token: `at-${seen.length}`, refresh_token: `rt-${seen.length + 1}`, expires_in: 1 });
    };
    const store = new CredentialStore(makeConfig({ codex: { refreshToken: "rt-1" } }), { fetchImpl, now: () => clock });
    await store.refreshCodex();
    clock = 10_000_000; // force the next call to refresh again
    await store.refreshCodex();
    expect(seen).toEqual(["rt-1", "rt-2"]);
  });

  it("keeps working (returns prior token) when refresh fails", async () => {
    let calls = 0;
    let clock = 0;
    const fetchImpl: FetchLike = async () => {
      calls++;
      return calls === 1 ? jsonResponse({ access_token: "good", expires_in: 1 }) : jsonResponse({ error: "bad" }, 400);
    };
    const store = new CredentialStore(makeConfig({ codex: { refreshToken: "rt" } }), { fetchImpl, now: () => clock });
    expect((await store.getOpenAI()).oauthToken).toBe("good");
    clock = 10_000_000; // expire → second refresh fails (400) → prior token retained
    expect((await store.getOpenAI()).oauthToken).toBe("good");
    expect(calls).toBe(2);
  });
});
