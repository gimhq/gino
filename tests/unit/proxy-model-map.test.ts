import { describe, expect, it } from "bun:test";
import { builtinModelMap, resolveModel } from "@/proxy";

describe("M7 proxy/model-map — resolveModel", () => {
  it("resolves built-in anthropic aliases", () => {
    expect(resolveModel("claude-sonnet")).toEqual({ provider: "anthropic", model: "claude-sonnet-4-6" });
    expect(resolveModel("claude-opus")).toEqual({ provider: "anthropic", model: "claude-opus-4-8" });
    expect(resolveModel("claude-haiku")).toEqual({ provider: "anthropic", model: "claude-haiku-4-5" });
  });

  it("resolves built-in openai aliases", () => {
    expect(resolveModel("gpt-mini")).toEqual({ provider: "openai", model: "gpt-5-codex-mini" });
    expect(resolveModel("gpt-codex")).toEqual({ provider: "openai", model: "gpt-5-codex" });
  });

  it("auto-detects OpenAI model families by prefix", () => {
    expect(resolveModel("gpt-5-codex").provider).toBe("openai");
    expect(resolveModel("o3-mini").provider).toBe("openai");
    expect(resolveModel("chatgpt-4o").provider).toBe("openai");
  });

  it("defaults unknown names to anthropic pass-through (name IS the model)", () => {
    expect(resolveModel("claude-opus-4-8")).toEqual({ provider: "anthropic", model: "claude-opus-4-8" });
    expect(resolveModel("some-future-model")).toEqual({ provider: "anthropic", model: "some-future-model" });
  });

  it("config overrides win over built-ins", () => {
    const overrides = { "claude-sonnet": { provider: "openai" as const, model: "gpt-5" }, "house": { provider: "anthropic" as const, model: "claude-opus-4-8" } };
    expect(resolveModel("claude-sonnet", overrides)).toEqual({ provider: "openai", model: "gpt-5" });
    expect(resolveModel("house", overrides)).toEqual({ provider: "anthropic", model: "claude-opus-4-8" });
    // Non-overridden built-ins still resolve.
    expect(resolveModel("claude-opus", overrides)).toEqual({ provider: "anthropic", model: "claude-opus-4-8" });
  });

  it("exposes the built-in map", () => {
    expect(builtinModelMap()["claude-sonnet"]).toBeDefined();
  });
});
