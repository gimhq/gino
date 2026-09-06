// M7 — proxy credential authorization (H5). The engine authenticates to
// `/llm/*` with its per-agent connect token (presented as `x-internal-token`,
// or `x-api-key: sk-ant-proxy-<token>`, or `Authorization: Bearer <token>`),
// validated against `agents.connect_token_hash`; OR with a configured static
// proxy key. A real Anthropic temp key (`sk-ant-...`, from a prior OAuth
// exchange) is accepted ONLY on Anthropic pass-through paths, where it is
// forwarded as-is and Anthropic itself validates it (HOST-M7-01: the host
// cannot verify a temp key, so it must never gate a path that injects the
// host's own credentials).
//
// Self-contained: a DIRECT read-only lookup through the `Storage` port
// (`agents.getByConnectTokenHash`, no dependency on the M3 registry).
// Constant-time comparison for the static key; the connect token is matched by
// its stored sha-256 hash.
import type { Storage } from "@gino/host-core";
import { hashToken, safeEqual } from "@gino/host-core/domain";
import { extractProxyCredential, isAnthropicPassthroughPath, isAnthropicTempKey } from "./provider";

export interface ProxyAuthOpts {
  readonly storage: Storage;
  readonly staticKey?: string | undefined;
}

/**
 * True if the request carries a valid proxy credential. `pathname` is the
 * `/llm`-stripped upstream subpath. Resolution order:
 *   1. A real Anthropic temp key (`sk-ant-...`, not the proxy placeholder),
 *      ONLY on an Anthropic pass-through path → accepted (it is forwarded
 *      as-is and authenticates directly against Anthropic). On any other path
 *      the host injects its own credentials, so the unverifiable prefix grants
 *      nothing.
 *   2. The configured static proxy key (constant-time compare).
 *   3. A per-agent connect token whose sha-256 matches a non-removed agent's
 *      `connect_token_hash`.
 */
export function verifyProxyCredential(headers: Headers, opts: ProxyAuthOpts, pathname: string): boolean {
  if (isAnthropicTempKey(headers) && isAnthropicPassthroughPath(pathname))
    return true;

  const cred = extractProxyCredential(headers);
  if (!cred)
    return false;

  if (opts.staticKey && safeEqual(cred, opts.staticKey))
    return true;

  const hash = hashToken(cred);
  return opts.storage.agents.getByConnectTokenHash(hash) !== undefined;
}
