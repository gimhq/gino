// M7 — provider detection, path allow-listing, header hygiene, and the
// credential-extraction helpers shared by the HTTP handler and the auth
// middleware. Pure functions only (no I/O) so they are exhaustively
// unit-testable. Ported + tightened from the v1 proxy (backup/pre-hostv2
// src/api/proxy.ts), scoped to the v2 `/llm/*` subtree (paths here are ALREADY
// `/llm`-stripped — see `src/edge/llm.ts`).

import type { ProxyProvider } from "./model-map";

export { isPrivateUpstreamHost } from "@gino/host-core/domain";

// ─── Allowed upstream subpaths (post `/llm` strip) ───
// Anthropic-only paths.
const ANTHROPIC_PATH_PREFIXES = ["/v1/messages", "/api/oauth/"];
// OpenAI-only paths.
const OPENAI_PATH_PREFIXES = ["/v1/chat/completions", "/v1/responses"];
// Shared (either provider).
const SHARED_PATH_PREFIXES = ["/v1/models"];

const ALL_ALLOWED_PREFIXES = [
  ...ANTHROPIC_PATH_PREFIXES,
  ...OPENAI_PATH_PREFIXES,
  ...SHARED_PATH_PREFIXES,
];

/** True if `pathname` (already `/llm`-stripped) is a known upstream endpoint. */
export function isAllowedUpstreamPath(pathname: string): boolean {
  return ALL_ALLOWED_PREFIXES.some(p => pathname.startsWith(p));
}

// Paths on which a client-presented Anthropic temp key is forwarded AS-IS to
// Anthropic (which validates it). Everywhere else — the OAuth exchange, the
// OpenAI endpoints, the Codex WS relay — the host injects its OWN credential,
// so a bare `sk-ant-` prefix must never count as authentication there
// (a forged key would spend the host's credentials).
const ANTHROPIC_PASSTHROUGH_PREFIXES = ["/v1/messages", "/v1/models"];

/** True if `pathname` forwards a client Anthropic temp key as-is (never injects host credentials). */
export function isAnthropicPassthroughPath(pathname: string): boolean {
  return ANTHROPIC_PASSTHROUGH_PREFIXES.some(p => pathname.startsWith(p));
}

/**
 * Detect which upstream a request targets, given the `/llm`-stripped path and
 * the request headers. Provider-exclusive paths win outright; the `x-api-key`
 * heuristic only decides shared paths, so an OpenAI-only endpoint reached with
 * an `x-api-key` header is not misrouted to anthropic:
 *   - an Anthropic-only path → anthropic
 *   - an OpenAI-only path → openai
 *   - shared paths: `x-api-key` header → anthropic, else openai
 */
export function detectProvider(headers: Headers, pathname: string): ProxyProvider {
  if (ANTHROPIC_PATH_PREFIXES.some(p => pathname.startsWith(p)))
    return "anthropic";
  if (OPENAI_PATH_PREFIXES.some(p => pathname.startsWith(p)))
    return "openai";
  if (headers.get("x-api-key"))
    return "anthropic";
  return "openai";
}

// ─── Credential extraction ───

/** The engine's proxy placeholder key prefix (`sk-ant-proxy-<connectToken>`). */
export const PROXY_KEY_PREFIX = "sk-ant-proxy-";
/** A real Anthropic key prefix (incl. temp keys from the OAuth exchange). */
export const ANTHROPIC_KEY_PREFIX = "sk-ant-";

/**
 * Pull the engine's proxy credential off a request, in priority order:
 *   1. `x-internal-token`
 *   2. `x-api-key` (with the `sk-ant-proxy-` placeholder prefix stripped)
 *   3. `Authorization: Bearer <token>`
 * Returns the bare credential, or undefined if none present.
 */
export function extractProxyCredential(headers: Headers): string | undefined {
  const internal = headers.get("x-internal-token");
  if (internal)
    return internal;

  const apiKey = headers.get("x-api-key");
  if (apiKey)
    return apiKey.startsWith(PROXY_KEY_PREFIX) ? apiKey.slice(PROXY_KEY_PREFIX.length) : apiKey;

  const auth = headers.get("authorization");
  if (auth && auth.toLowerCase().startsWith("bearer "))
    return auth.slice("bearer ".length).trim() || undefined;

  return undefined;
}

/**
 * True for a real Anthropic key that is NOT the proxy placeholder — i.e. a temp
 * `sk-ant-...` API key the engine obtained from the OAuth exchange step. Such a
 * key is only accepted on Anthropic pass-through paths (where Anthropic itself
 * validates it) and never authorizes host-credential injection — see
 * `verifyProxyCredential` and the bridge guard in `handler.ts`.
 */
export function isAnthropicTempKey(headers: Headers): boolean {
  const apiKey = headers.get("x-api-key");
  return !!apiKey && apiKey.startsWith(ANTHROPIC_KEY_PREFIX) && !apiKey.startsWith(PROXY_KEY_PREFIX);
}

// ─── Header hygiene ───
// Hop-by-hop headers (never forwarded), and host-internal headers that must not
// leak upstream. The client's own `x-api-key` / `authorization` are dropped
// here too — the handler injects the real upstream credential afterwards.
const HOP_BY_HOP = new Set(["connection", "keep-alive", "transfer-encoding", "host", "content-length"]);
const INTERNAL_EXACT = new Set(["x-internal-token", "x-api-key", "authorization"]);
const INTERNAL_PREFIXES = ["x-gino-", "x-runner-"];

function isStrippedHeader(key: string): boolean {
  const k = key.toLowerCase();
  if (HOP_BY_HOP.has(k) || INTERNAL_EXACT.has(k))
    return true;
  return INTERNAL_PREFIXES.some(p => k.startsWith(p));
}

/**
 * Build the upstream request headers from the client headers, dropping
 * hop-by-hop + host-internal + client-credential headers. The caller injects
 * the real upstream credential and sets `host`/`content-length` afterwards.
 */
export function buildUpstreamHeaders(clientHeaders: Headers): Headers {
  const out = new Headers();
  for (const [key, value] of clientHeaders.entries()) {
    if (isStrippedHeader(key))
      continue;
    out.set(key, value);
  }
  return out;
}

/**
 * Strip response headers that would corrupt the relayed body — the upstream's
 * `content-encoding`/`content-length` no longer match once Bun re-streams it.
 */
export function buildResponseHeaders(upstreamHeaders: Headers): Headers {
  const out = new Headers(upstreamHeaders);
  out.delete("content-encoding");
  out.delete("content-length");
  return out;
}
