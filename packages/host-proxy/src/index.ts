// M7 — CREDENTIAL PROXY @ /llm/* for the engine's LLM calls. The host holds the
// real upstream credentials; the engine reaches the proxy via its base-url and
// never sees them. Composed of: provider detect + header hygiene (`provider`),
// model-map (`model-map`), credential resolution + Codex OAuth refresh
// (`credentials`), the Claude↔OpenAI translator (`translator`), the Claude→
// OpenAI WS bridge (`bridge`), the HTTP forward path (`handler`), proxy auth
// (`auth`), and the structured config (`config`). The `/llm/*` router lives in
// `src/edge/llm.ts` and delegates here. The raw Codex WS relay (`ws-bridge`)
// is Bun-only and exported from the separate `./bun` entry (`./bun.ts`).

export { verifyProxyCredential } from "./auth";
export type { ProxyAuthOpts } from "./auth";
export { bridgeClaudeToOpenAI, CODEX_WS_URL } from "./bridge";
export type { BridgeDeps, BridgeSocket, WsFactory } from "./bridge";
export { buildProxyConfig, parseModelMap } from "./config";
export type { ProviderUpstream, ProxyConfig, ProxyEnv } from "./config";
export { CredentialStore } from "./credentials";
export type { FetchLike, ResolvedCredential } from "./credentials";
export { createProxyHandler } from "./handler";
export type { ProxyHandler, ProxyHandlerDeps } from "./handler";
export { builtinModelMap, resolveModel } from "./model-map";
export type { ModelMap, ModelMapping, ProxyProvider } from "./model-map";
export {
  buildResponseHeaders,
  buildUpstreamHeaders,
  detectProvider,
  extractProxyCredential,
  isAllowedUpstreamPath,
  isAnthropicPassthroughPath,
  isAnthropicTempKey,
  isPrivateUpstreamHost,
} from "./provider";
export {
  claudeToOpenAIRequest,
  createTranslatorState,
  isOpenAIModel,
  openAIEventToClaudeSSE,
} from "./translator";
export type { ClaudeRequest, OpenAIResponsesRequest, TranslatorState } from "./translator";

export const MODULE = "proxy" as const;
