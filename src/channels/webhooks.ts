// M6 — WEBHOOK ADAPTER (outbound egress push + SSRF guard).
//
// A channel of type `webhook` delivers egress by POSTing new transcript events
// to a configured URL instead of waiting for a poll. Delivery is best-effort and
// CURSOR-DRIVEN (egress.ts): the cursor only advances after a 2xx, so a failed
// push simply re-reads from the store next time — nothing in flight is lost and a
// distribution bug can never corrupt the record (storage ⟂ distribution).
//
// SSRF posture (two layers):
//   1. REGISTRATION (sync, assertSafeWebhookUrl): scheme + obvious-loopback
//      hostname + literal private/loopback IP, reusing the proxy's per-IP guard.
//      Stays synchronous so the admin registration route stays synchronous.
//   2. DELIVERY (async, assertSafeWebhookUrlResolved): the static checks PLUS DNS
//      resolution — every resolved A/AAAA must be public. This defeats
//      DNS-rebinding / CNAMEs into private space (a host that passed the string
//      check at registration can resolve into private space at delivery time).
//      For plain-http targets the request is PINNED to the vetted address
//      (HOST-M5M6-07): the fetch connects to the resolved-and-validated IP with
//      the Host header preserved, so fetch's own DNS lookup can never be rebound
//      between validation and connect. https targets keep the hostname URL —
//      TLS server-certificate validation binds the connection to the registered
//      host (a rebound private endpoint cannot present a valid certificate for
//      it), and connecting by IP would break SNI/certificate matching.
//      Delivery never follows redirects (`redirect: "manual"`, HOST-M5M6-11): a
//      3xx is a delivery failure, so the shared secret and transcript payload
//      are never re-sent to a `Location` target that was never registered. Every
//      request is bounded by a timeout (AbortSignal, HOST-M5M6-05).
import { lookup as dnsLookup } from "node:dns/promises";
import { isIP } from "node:net";
import { isPrivateUpstreamHost } from "@/proxy";
import { AppError, ValidationError } from "@/shared/lib/errors";

/** A registered outbound webhook on a channel (stored under `channel.config.webhooks`). */
export interface WebhookEndpoint {
  id: string;
  url: string;
  /** Optional shared secret echoed in `x-webhook-secret` so the receiver can verify origin. */
  secret?: string;
}

/** A resolved DNS address (subset of node:dns `LookupAddress`). */
export interface ResolvedAddress {
  address: string;
  family: number;
}

/**
 * Injectable DNS resolver — returns EVERY A/AAAA record for a host (so a single
 * private answer among many is still caught). Defaults to `node:dns`; tests pass a
 * fake so they never touch the network.
 */
export type DnsResolver = (hostname: string) => Promise<ResolvedAddress[]>;

const defaultResolver: DnsResolver = hostname => dnsLookup(hostname, { all: true });

/** The minimal `fetch` response surface webhook delivery needs — structurally a `Response`. */
export interface FetchResponse {
  status: number;
  /** Header accessor (used to read `Location` on a 3xx); a real `Response.headers`. */
  headers?: { get: (name: string) => string | null };
}

/** The minimal `fetch` surface webhook delivery needs (injectable for tests — no real network). */
export type FetchLike = (
  url: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
    redirect?: "manual" | "error" | "follow";
    signal?: AbortSignal;
  },
) => Promise<FetchResponse>;

/** Normalize a URL hostname for the IP/loopback checks (lower-case, IPv6 brackets stripped). */
function guardHost(url: URL): string {
  return url.hostname.toLowerCase().replace(/^\[|\]$/g, "");
}

/**
 * SYNCHRONOUS SSRF guard for an outbound webhook URL. Rejects non-http(s) schemes,
 * obvious loopback hostnames, and literal private/loopback IPs (via the proxy
 * guard). Used at REGISTRATION time (`addWebhook`) — it stays sync so the admin
 * registration route stays sync. It does NOT resolve DNS: a hostname that resolves
 * into private space is caught at delivery time by `assertSafeWebhookUrlResolved`.
 */
export function assertSafeWebhookUrl(raw: string): URL {
  let url: URL;
  try {
    url = new URL(raw);
  }
  catch {
    throw new ValidationError("invalid webhook URL", { formErrors: ["url must be a valid absolute URL"], fieldErrors: {} });
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new ValidationError("invalid webhook URL", { formErrors: ["url must use http(s)"], fieldErrors: {} });
  }
  const host = guardHost(url); // strip IPv6 brackets
  if (host === "" || host === "localhost" || host.endsWith(".localhost")) {
    throw new AppError("webhook URL resolves to a blocked host", 422, "SSRF_BLOCKED");
  }
  if (isPrivateUpstreamHost(host)) {
    throw new AppError("webhook URL resolves to a private/loopback address", 422, "SSRF_BLOCKED");
  }
  return url;
}

/**
 * DELIVERY-TIME SSRF guard: the static checks of `assertSafeWebhookUrl` PLUS DNS
 * resolution. Resolves the hostname and rejects if ANY returned A/AAAA address is
 * private / loopback / link-local / unique-local (per-IP check reuses the proxy's
 * `isPrivateUpstreamHost`) — defeating DNS-rebinding and CNAMEs into private space,
 * which the hostname-string check alone cannot see. A literal-IP host skips the
 * lookup (the sync guard already classified it). Fail-closed: a host that resolves
 * to nothing is rejected (we can't confirm it is public). The resolver is
 * injectable so tests never hit real DNS.
 */
export async function assertSafeWebhookUrlResolved(raw: string, resolver: DnsResolver = defaultResolver): Promise<URL> {
  return (await vetWebhookTarget(raw, resolver)).url;
}

/** A vetted delivery target: the validated URL plus the resolved-and-validated addresses (null for a literal-IP host — nothing was resolved). */
interface VettedTarget {
  url: URL;
  addresses: ResolvedAddress[] | null;
}

/** The implementation behind `assertSafeWebhookUrlResolved`, keeping the vetted addresses so delivery can pin one (HOST-M5M6-07). */
async function vetWebhookTarget(raw: string, resolver: DnsResolver): Promise<VettedTarget> {
  const url = assertSafeWebhookUrl(raw);
  const host = guardHost(url);
  if (isIP(host) !== 0) {
    return { url, addresses: null }; // literal IP — already validated synchronously; nothing to resolve
  }
  const addresses = await resolver(host);
  if (addresses.length === 0) {
    throw new AppError("webhook URL did not resolve to a verifiable address", 422, "SSRF_BLOCKED");
  }
  for (const { address } of addresses) {
    if (isPrivateUpstreamHost(address)) {
      throw new AppError("webhook URL resolves to a private/loopback address", 422, "SSRF_BLOCKED");
    }
  }
  return { url, addresses };
}

export interface WebhookPayload {
  channel: string;
  taskId: string;
  conversationRef?: string;
  messages: unknown[];
}

export interface WebhookResult {
  ok: boolean;
  status: number;
}

export interface DeliverOptions {
  /** Injectable DNS resolver for the delivery-time SSRF re-check (defaults to node:dns). */
  resolver?: DnsResolver;
  /** Per-request delivery timeout in ms (defaults to `DEFAULT_WEBHOOK_TIMEOUT_MS`). */
  timeoutMs?: number;
}

/** Default per-request delivery timeout (HOST-M5M6-05): a hung endpoint can never stall the push loop indefinitely. */
export const DEFAULT_WEBHOOK_TIMEOUT_MS = 10_000;

/**
 * Deliver one payload to one webhook endpoint (SSRF-guarded, pinned, bounded).
 * The target is re-validated WITH DNS resolution before the request; for a
 * plain-http target the request is issued to the vetted resolved IP with the
 * Host header preserved, so fetch's independent DNS lookup can never be rebound
 * into private space between validation and connect (HOST-M5M6-07 — https
 * targets keep the hostname URL: certificate validation pins the identity).
 * Redirects are never followed — a 3xx is a delivery failure, so the shared
 * secret and payload are never re-sent to an unregistered `Location` target
 * (HOST-M5M6-11). Every request carries an abort timeout (HOST-M5M6-05).
 * Returns the HTTP status and whether it was a 2xx — the caller (egress) uses
 * `ok` to decide whether to advance the cursor. An SSRF-blocked / unresolvable /
 * timed-out / unreachable target fails the delivery (`ok: false`, status 0 for
 * transport failures) rather than throwing, so one bad endpoint never aborts a
 * channel's whole push.
 */
export async function deliverWebhook(
  endpoint: WebhookEndpoint,
  payload: WebhookPayload,
  fetchImpl: FetchLike = fetch,
  opts: DeliverOptions = {},
): Promise<WebhookResult> {
  const resolver = opts.resolver ?? defaultResolver;
  let target: VettedTarget;
  try {
    target = await vetWebhookTarget(endpoint.url, resolver);
  }
  catch {
    return { ok: false, status: 0 }; // SSRF-blocked / unresolvable → fail (cursor not advanced)
  }

  const headers: Record<string, string> = {
    "content-type": "application/json",
    ...(endpoint.secret ? { "x-webhook-secret": endpoint.secret } : {}),
  };

  // HOST-M5M6-07 — pin the vetted address for http targets (see module header).
  let requestUrl = target.url.toString();
  const pin = target.addresses?.[0];
  if (target.url.protocol === "http:" && pin) {
    const pinned = new URL(target.url.toString());
    pinned.hostname = pin.family === 6 ? `[${pin.address}]` : pin.address;
    requestUrl = pinned.toString();
    headers.host = target.url.host; // original hostname (+port) for the receiver's vhost routing
  }

  let res: FetchResponse;
  try {
    res = await fetchImpl(requestUrl, {
      method: "POST",
      redirect: "manual", // never auto-follow into an unvalidated host (SSRF redirect bypass)
      headers,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(opts.timeoutMs ?? DEFAULT_WEBHOOK_TIMEOUT_MS),
    });
  }
  catch {
    return { ok: false, status: 0 }; // timeout / network error → failed delivery, never a thrown push
  }

  // HOST-M5M6-11 — a 3xx is a failure: the secret + transcript are never re-sent
  // to a Location target (only "is it public" could ever be checked, not "is it
  // the registered endpoint").
  return { ok: res.status >= 200 && res.status < 300, status: res.status };
}
