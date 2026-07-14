// HOST-M9-03 — logger redaction must cover the host's own secret vocabulary
// (register/connect tokens, the connect header, proxy credential config
// keys), not just generic names like `password`/`token`.
import { describe, expect, it } from "bun:test";
import { sanitizeLogPayload } from "@/shared/lib/logger";

describe("sanitizeLogPayload (HOST-M9-03)", () => {
  it("redacts the protocol wire token fields", () => {
    const out = sanitizeLogPayload({
      registerToken: "reg-secret",
      connectToken: "conn-secret",
    }) as Record<string, unknown>;
    expect(out.registerToken).toBe("[REDACTED]");
    expect(out.connectToken).toBe("[REDACTED]");
  });

  it("redacts the connect header and generic api-key spellings", () => {
    const out = sanitizeLogPayload({
      "x-internal-token": "conn-secret",
      "apiKey": "sk-live",
      "api_key": "sk-live",
    }) as Record<string, unknown>;
    expect(out["x-internal-token"]).toBe("[REDACTED]");
    expect(out.apiKey).toBe("[REDACTED]");
    expect(out.api_key).toBe("[REDACTED]");
  });

  it("redacts every proxy credential config key (resolved Config logged whole)", () => {
    const out = sanitizeLogPayload({
      PROXY_ANTHROPIC_API_KEY: "sk-ant",
      PROXY_ANTHROPIC_OAUTH_TOKEN: "oauth",
      PROXY_OPENAI_API_KEY: "sk-oai",
      PROXY_OPENAI_OAUTH_TOKEN: "oauth",
      PROXY_CODEX_REFRESH_TOKEN: "refresh",
      PROXY_STATIC_KEY: "static",
      REGISTER_TOKEN: "reg",
      HTTP_CHANNEL_TOKEN: "chan",
    }) as Record<string, unknown>;
    for (const value of Object.values(out)) {
      expect(value).toBe("[REDACTED]");
    }
  });

  it("redacts nested occurrences", () => {
    const out = sanitizeLogPayload({
      req: { headers: { "x-internal-token": "s" } },
    }) as { req: { headers: Record<string, unknown> } };
    expect(out.req.headers["x-internal-token"]).toBe("[REDACTED]");
  });

  it("leaves non-secret fields intact", () => {
    const out = sanitizeLogPayload({
      agentId: "agent_abc",
      status: 200,
      path: "/api/health",
      donkey: "eeyore",
    }) as Record<string, unknown>;
    expect(out.agentId).toBe("agent_abc");
    expect(out.status).toBe(200);
    expect(out.path).toBe("/api/health");
    // "donkey" ends in "key" — a documented false positive the redactor
    // prefers over false negatives.
    expect(out.donkey).toBe("[REDACTED]");
  });
});
