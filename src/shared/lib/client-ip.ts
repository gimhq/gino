import type { Context } from "hono";

const RE_COMMA_SPLIT = /\s*,\s*/;
const RE_BAD_PEER = /^(?:unknown|::)$/i;

interface ClientIpConfig {
  readonly TRUST_PROXY: boolean;
  readonly TRUSTED_PROXY_IPS?: string;
}

/**
 * Get the real client IP address from a Hono context.
 *
 * Default behaviour (`TRUST_PROXY=false`): forwarding headers are
 * IGNORED to prevent header-spoofing attacks; only the connection peer
 * IP from the Bun runtime (`c.env.IP.address`) is used.
 *
 * When `TRUST_PROXY=true` the function honours the rightmost entry of
 * `X-Forwarded-For` (the hop closest to our process — the one controlled
 * by the trusted proxy). `X-Real-IP` is read only as a fallback, behind
 * XFF, because in most production stacks the proxy explicitly sets XFF
 * and `X-Real-IP` is either absent or operator-defined.
 *
 * If `TRUSTED_PROXY_IPS` is set (a comma-separated list of CIDR / IP
 * literals — IPv4 only at this layer), forwarding headers are accepted
 * only when the immediate peer matches one of those ranges. Empty
 * (default) means "any peer is trusted" — preserves the pre-flag
 * behaviour.
 */
export function getClientIp(c: Context, config?: ClientIpConfig): string {
  const peerIp = c.env?.IP?.address;

  if (!config?.TRUST_PROXY) {
    return peerIp ?? "unknown";
  }

  // Per-peer gate: when the operator has supplied an allow-list,
  // forwarding headers from an unknown peer are dropped on the floor
  // (returns the peer itself). This stops a misconfigured `TRUST_PROXY`
  // (e.g. exposed to the open internet without a proxy in front of it)
  // from letting any caller forge a client IP.
  const proxyAllowList = parseProxyAllowList(config.TRUSTED_PROXY_IPS);
  if (proxyAllowList.length > 0 && peerIp && !isAllowedPeer(peerIp, proxyAllowList)) {
    return peerIp;
  }

  const headers = c.req.header();
  const lowered: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(headers)) {
    lowered[key.toLowerCase()] = value;
  }

  // Prefer XFF (right-most). The rightmost hop is the one our trusted
  // proxy actually controls; intermediate entries are client-supplied
  // and untrustworthy.
  const xff = lowered["x-forwarded-for"];
  if (xff && xff.trim()) {
    const parts = xff.split(RE_COMMA_SPLIT).map(s => s.trim()).filter(Boolean);
    const rightmost = parts.at(-1);
    if (rightmost && !isSentinel(rightmost))
      return rightmost;
  }

  const realIp = lowered["x-real-ip"];
  if (realIp && realIp.trim() && !isSentinel(realIp.trim())) {
    return realIp.trim();
  }

  return peerIp ?? "unknown";
}

/**
 * True when the runtime is in the spoofable configuration: forwarding
 * headers are trusted (`TRUST_PROXY=true`) but no proxy-peer allow-list
 * narrows *which* peers may set them. In this state any client that can
 * reach the process directly can forge its IP and defeat every IP-keyed
 * rate limiter. The app should log a startup warning when this is true
 * (it does not change request behaviour — kept for backward compat).
 */
export function isSpoofableProxyConfig(config?: ClientIpConfig): boolean {
  return Boolean(config?.TRUST_PROXY) && parseProxyAllowList(config?.TRUSTED_PROXY_IPS).length === 0;
}

function isSentinel(v: string): boolean {
  return RE_BAD_PEER.test(v);
}

interface ParsedCidr {
  readonly ipBits: number;
  readonly mask: number;
}

function parseProxyAllowList(raw: string | undefined): readonly ParsedCidr[] {
  if (!raw)
    return [];
  const out: ParsedCidr[] = [];
  for (const part of raw.split(",").map(s => s.trim()).filter(Boolean)) {
    const parsed = parseCidr(part);
    if (parsed)
      out.push(parsed);
  }
  return out;
}

function parseCidr(entry: string): ParsedCidr | undefined {
  const slash = entry.indexOf("/");
  const ipStr = slash === -1 ? entry : entry.slice(0, slash);
  const prefix = slash === -1 ? 32 : Number(entry.slice(slash + 1));
  if (!Number.isFinite(prefix) || prefix < 0 || prefix > 32)
    return undefined;
  const ipBits = ipv4ToInt(ipStr);
  if (ipBits === undefined)
    return undefined;
  const mask = prefix === 0 ? 0 : (~0 << (32 - prefix)) >>> 0;
  return { ipBits: ipBits & mask, mask };
}

function ipv4ToInt(ip: string): number | undefined {
  const parts = ip.split(".");
  if (parts.length !== 4)
    return undefined;
  let acc = 0;
  for (const p of parts) {
    const n = Number(p);
    if (!Number.isInteger(n) || n < 0 || n > 255)
      return undefined;
    acc = (acc << 8 | n) >>> 0;
  }
  return acc;
}

function isAllowedPeer(peer: string, allowList: readonly ParsedCidr[]): boolean {
  const bits = ipv4ToInt(peer);
  if (bits === undefined)
    return false;
  for (const entry of allowList) {
    if ((bits & entry.mask) >>> 0 === entry.ipBits)
      return true;
  }
  return false;
}
