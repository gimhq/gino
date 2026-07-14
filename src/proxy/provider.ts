// M7 — provider detection, path allow-listing, header hygiene, and the
// credential-extraction helpers shared by the HTTP handler and the auth
// middleware. Pure functions only (no I/O) so they are exhaustively
// unit-testable. Ported + tightened from the v1 proxy (backup/pre-hostv2
// src/api/proxy.ts), scoped to the v2 `/llm/*` subtree (paths here are ALREADY
// `/llm`-stripped — see `src/edge/llm.ts`).

import type { ProxyProvider } from "./model-map";
import net from "node:net";

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
// (HOST-M7-01: a forged key would spend the host's credentials).
const ANTHROPIC_PASSTHROUGH_PREFIXES = ["/v1/messages", "/v1/models"];

/** True if `pathname` forwards a client Anthropic temp key as-is (never injects host credentials). */
export function isAnthropicPassthroughPath(pathname: string): boolean {
  return ANTHROPIC_PASSTHROUGH_PREFIXES.some(p => pathname.startsWith(p));
}

/**
 * Detect which upstream a request targets, given the `/llm`-stripped path and
 * the request headers:
 *   - an `x-api-key` header, or an Anthropic-only path → anthropic
 *   - everything else → openai
 */
export function detectProvider(headers: Headers, pathname: string): ProxyProvider {
  if (headers.get("x-api-key"))
    return "anthropic";
  if (ANTHROPIC_PATH_PREFIXES.some(p => pathname.startsWith(p)))
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

// ─── SSRF guard ───

/** Bytes of a `net.isIP === 4` dotted-quad (syntax already vetted by net.isIP). */
function ipv4Bytes(host: string): number[] {
  return host.split(".").map(Number);
}

/**
 * Parse a `net.isIP === 6` literal into its 16 bytes (handles `::` compression
 * and a dotted-IPv4 tail). Returns null if it somehow does not fit — the caller
 * fails closed on that.
 */
function ipv6Bytes(host: string): number[] | null {
  let s = host;
  // Dotted-IPv4 tail → two trailing 16-bit groups.
  const lastColon = s.lastIndexOf(":");
  const tail = s.slice(lastColon + 1);
  if (tail.includes(".")) {
    if (net.isIP(tail) !== 4)
      return null;
    const [a, b, c, d] = ipv4Bytes(tail) as [number, number, number, number];
    s = `${s.slice(0, lastColon + 1)}${((a << 8) | b).toString(16)}:${((c << 8) | d).toString(16)}`;
  }
  const halves = s.split("::");
  if (halves.length > 2)
    return null;
  const head = halves[0] ? halves[0].split(":") : [];
  const rest = halves.length === 2 && halves[1] ? halves[1].split(":") : [];
  const missing = 8 - head.length - rest.length;
  if (halves.length === 2 ? missing < 1 : missing !== 0)
    return null;
  const groups = halves.length === 2 ? [...head, ...Array.from<string>({ length: missing }).fill("0"), ...rest] : head;
  const bytes: number[] = [];
  for (const g of groups) {
    const v = Number.parseInt(g, 16);
    if (!Number.isInteger(v) || v < 0 || v > 0xFFFF)
      return null;
    bytes.push(v >> 8, v & 0xFF);
  }
  return bytes;
}

/** True if the IPv4 address (as 4 bytes) is not provably public (private/loopback/link-local/reserved). */
function isNonPublicIPv4(bytes: readonly number[]): boolean {
  const [a, b, c] = bytes as unknown as [number, number, number, number];
  return (
    a === 0 // 0.0.0.0/8 — unspecified / "this network"
    || a === 10 // 10/8 private
    || a === 127 // loopback
    || (a === 100 && b >= 64 && b <= 127) // 100.64/10 CGNAT
    || (a === 169 && b === 254) // link-local (incl. cloud metadata)
    || (a === 172 && b >= 16 && b <= 31) // 172.16/12 private
    || (a === 192 && b === 0 && c === 0) // 192.0.0/24 protocol assignments
    || (a === 192 && b === 0 && c === 2) // 192.0.2/24 TEST-NET-1
    || (a === 192 && b === 168) // 192.168/16 private
    || (a === 198 && (b === 18 || b === 19)) // 198.18/15 benchmarking
    || (a === 198 && b === 51 && c === 100) // 198.51.100/24 TEST-NET-2
    || (a === 203 && b === 0 && c === 113) // 203.0.113/24 TEST-NET-3
    || a >= 224 // 224/4 multicast + 240/4 reserved + broadcast
  );
}

/** True if the IPv6 address (as 16 bytes) is not provably public; embedded-IPv4 forms classify the inner IPv4. */
function isNonPublicIPv6(b: readonly number[]): boolean {
  const zero = (from: number, to: number): boolean => b.slice(from, to).every(x => x === 0);
  if (zero(0, 12))
    return true; // ::/96 — unspecified (::), loopback (::1), deprecated IPv4-compatible
  if (zero(0, 10) && b[10] === 0xFF && b[11] === 0xFF)
    return isNonPublicIPv4(b.slice(12)); // ::ffff:0:0/96 IPv4-mapped → classify the embedded IPv4
  if (b[0] === 0 && b[1] === 0x64 && b[2] === 0xFF && b[3] === 0x9B && zero(4, 12))
    return isNonPublicIPv4(b.slice(12)); // 64:ff9b::/96 NAT64 → classify the embedded IPv4
  if (b[0] === 0x01 && b[1] === 0 && zero(2, 8))
    return true; // 100::/64 discard-only
  if ((b[0]! & 0xFE) === 0xFC)
    return true; // fc00::/7 unique-local
  if (b[0] === 0xFE && (b[1]! & 0xC0) === 0x80)
    return true; // fe80::/10 link-local
  if (b[0] === 0xFF)
    return true; // ff00::/8 multicast
  if (b[0] === 0x20 && b[1] === 0x01 && b[2] === 0x0D && b[3] === 0xB8)
    return true; // 2001:db8::/32 documentation
  if (b[0] === 0x20 && b[1] === 0x01 && b[2] === 0 && b[3] === 0)
    return true; // 2001::/32 Teredo (tunnels an obfuscated IPv4) — fail closed
  if (b[0] === 0x20 && b[1] === 0x02)
    return isNonPublicIPv4(b.slice(2, 6)); // 2002::/16 6to4 → classify the embedded IPv4
  return false;
}

/**
 * Reject a configured upstream that is a literal IP not provably public —
 * private, loopback, link-local, CGNAT, multicast, reserved, documentation, and
 * the unspecified addresses, on both families (operator misconfiguration / SSRF
 * belt-and-suspenders). IPv4-embedding IPv6 forms (IPv4-mapped `::ffff:a.b.c.d`,
 * NAT64 `64:ff9b::/96`, 6to4 `2002::/16`) are normalized to their inner IPv4
 * before classifying, so a mapped-loopback literal cannot slip through. An IP
 * literal that fails to parse is treated as non-public (fail closed). Only
 * literal IPs are checked — hostnames (the normal case, e.g. api.anthropic.com)
 * always pass; callers that must vet hostnames resolve DNS and re-check each
 * address (see `assertSafeWebhookUrlResolved`). Alternate IPv4 encodings
 * (decimal/octal/hex) reach this guard already canonicalized to dotted-quad by
 * the WHATWG URL parser at every call site.
 */
export function isPrivateUpstreamHost(hostname: string): boolean {
  const family = net.isIP(hostname);
  if (family === 0)
    return false; // not a literal IP → not our concern here
  if (family === 4)
    return isNonPublicIPv4(ipv4Bytes(hostname));
  const bytes = ipv6Bytes(hostname);
  return bytes === null ? true : isNonPublicIPv6(bytes);
}
