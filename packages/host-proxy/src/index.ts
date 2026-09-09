// M7 — CREDENTIAL PROXY @ /llm/* for the engine's LLM calls. The host holds the
// real upstream credentials; the engine reaches the proxy via its base-url and
// never sees them. Composed of: provider detect + header hygiene (`provider`),
// model-map (`model-map`), credential resolution + Codex OAuth refresh
// (`credentials`), the Claude↔OpenAI translator (`translator`), the Claude→
// OpenAI WS bridge (`bridge`), the HTTP forward path (`handler`), proxy auth
// (`auth`), and the structured config (`config`). The `/llm/*` router lives in
// `src/hono/llm.ts` and delegates here; it ships from the separate `./hono`
// entry. The raw Codex WS relay (`ws-bridge`) is Bun-only and exported from
// the separate `./bun` entry (`./bun.ts`).
//
// FROZEN as of P3 (docs/host-sdk-design.md §5 step 8). This list is the
// package's API: an explicit allow-list, no wildcards, so the surface cannot
// grow by accident when a module gains an export.
// `tests/unit/api-surface.test.ts` snapshots it — adding or removing a name
// here fails that test until the snapshot is updated deliberately.
//
// The scope is "what a Node embedder needs to mount the forward path itself"
// (§7.5): config, credentials, auth, the model map, `createProxyHandler` and
// the seams those take. The translation machinery behind them — the
// Claude↔OpenAI request/event translator, the WS bridge, the upstream header
// builders and the path guards — is `createProxyHandler`'s business and stays
// internal.
//
// This barrel freezes NAMES ONLY. `ProxyProvider` and `ModelMap` are data
// whose contents are expected to grow as engines and providers are added;
// nothing here treats that set as closed.

// ─── Proxy auth — the engine's placeholder credential ───
export { verifyProxyCredential } from "./auth";
export type { ProxyAuthOpts } from "./auth";

// The WS seam `ProxyHandlerDeps.wsFactory` injects; type-only, so a consumer
// can supply its own socket without depending on the Bun-only `./bun` entry.
export type { BridgeSocket, WsFactory } from "./bridge";

// ─── Structured configuration (env → `ProxyConfig`) ───
export { buildProxyConfig, parseModelMap } from "./config";
export type { ProviderUpstream, ProxyConfig, ProxyEnv } from "./config";

// ─── Credential resolution + Codex OAuth refresh ───
export { CredentialStore } from "./credentials";
export type { CredentialStoreOpts, FetchLike, ResolvedCredential } from "./credentials";

// ─── The HTTP forward path (what a Node embedder mounts itself, §7.5) ───
export { createProxyHandler } from "./handler";
export type { ProxyHandler, ProxyHandlerDeps } from "./handler";

// ─── The model map (alias → provider + upstream model) ───
export { builtinModelMap, resolveModel } from "./model-map";
export type { ModelMap, ModelMapping, ProxyProvider } from "./model-map";

// ─── Request classification the auth path needs ───
export { detectProvider, extractProxyCredential } from "./provider";
