// M7 — model-map. Resolves an incoming model name (an alias, or a full
// upstream model id) to its upstream provider + concrete model. Two uses:
//   1. Provider routing — a request whose model resolves to `openai` while
//      arriving on the Anthropic `/v1/messages` path is bridged (Claude
//      Messages → OpenAI Responses); same-provider requests forward directly.
//   2. Aliasing — the resolved `model` replaces the body's `model` field so an
//      engine can speak a stable alias while the host points it at whatever
//      concrete upstream model the operator has configured.
//
// The built-in map below is a sensible default; operators extend/override it
// via `GINO_PROXY_MODEL_MAP` (parsed in `config.ts`/`buildProxyConfig`). Ported
// from the v1 proxy (backup/pre-hostv2 src/proxy/model-map.ts), trimmed to the
// routing+alias surface M7 needs (the v1 `sdkModel` field was a container-side
// concern and is dropped — the host never tells the engine which SDK model to
// run).

export type ProxyProvider = "anthropic" | "openai";

export interface ModelMapping {
  readonly provider: ProxyProvider;
  /** The concrete model name sent upstream. */
  readonly model: string;
}

export type ModelMap = Readonly<Record<string, ModelMapping>>;

// Default Claude model ids are current as of 2026-06 (claude-api skill). They
// are only defaults — operators override per deployment via config.
const BUILTIN_MODEL_MAP: ModelMap = {
  // Anthropic aliases (pass-through to a concrete Claude model).
  "claude-opus": { provider: "anthropic", model: "claude-opus-4-8" },
  "claude-sonnet": { provider: "anthropic", model: "claude-sonnet-4-6" },
  "claude-haiku": { provider: "anthropic", model: "claude-haiku-4-5" },
  // OpenAI / Codex aliases (bridge to the Responses API).
  "gpt-mini": { provider: "openai", model: "gpt-5-codex-mini" },
  "gpt-codex": { provider: "openai", model: "gpt-5-codex" },
  "gpt-codex-mini": { provider: "openai", model: "gpt-5-codex-mini" },
  "gpt-5": { provider: "openai", model: "gpt-5" },
};

const RE_OPENAI_MODEL = /^(?:gpt-|o[134](?:-|$)|chatgpt-)/;

/**
 * Resolve a model alias or full model name to its mapping. Resolution order:
 *   1. Exact match in the (config-merged) map.
 *   2. Prefix auto-detect — OpenAI/Codex model families → `openai`.
 *   3. Default — Anthropic pass-through (the name IS the upstream model).
 *
 * `overrides` (config) take precedence over the built-ins.
 */
export function resolveModel(modelOrAlias: string, overrides?: ModelMap): ModelMapping {
  const merged = overrides ? { ...BUILTIN_MODEL_MAP, ...overrides } : BUILTIN_MODEL_MAP;

  const mapped = merged[modelOrAlias];
  if (mapped)
    return mapped;

  if (RE_OPENAI_MODEL.test(modelOrAlias))
    return { provider: "openai", model: modelOrAlias };

  return { provider: "anthropic", model: modelOrAlias };
}

/** Built-in defaults, exposed for tests + config merging. */
export function builtinModelMap(): ModelMap {
  return BUILTIN_MODEL_MAP;
}
