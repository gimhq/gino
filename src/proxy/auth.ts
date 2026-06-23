import type { AppDatabase } from "@/db";
// M7 — proxy credential authorization (H5). The engine authenticates to
// `/llm/*` with its per-agent connect token (presented as `x-internal-token`,
// or `x-api-key: sk-ant-proxy-<token>`, or `Authorization: Bearer <token>`),
// validated against `agents.connect_token_hash`; OR with a configured static
// proxy key. A real Anthropic temp key (`sk-ant-...`, from a prior OAuth
// exchange) is also accepted and forwarded as-is.
//
// Self-contained: a DIRECT read-only query on the `agents` table (no dependency
// on the M3 registry, which isn't built yet). Constant-time comparison for the
// static key; the connect token is matched by its stored sha-256 hash.
import { and, eq, ne } from "drizzle-orm";
import { agents } from "@/db/schema";
import { hashToken, safeEqual } from "@/platform";
import { extractProxyCredential, isAnthropicTempKey } from "./provider";

export interface ProxyAuthOpts {
  readonly db: AppDatabase;
  readonly staticKey?: string | undefined;
}

/**
 * True if the request carries a valid proxy credential. Resolution order:
 *   1. A real Anthropic temp key (`sk-ant-...`, not the proxy placeholder) →
 *      accepted (it authenticates directly against Anthropic; forwarded as-is).
 *   2. The configured static proxy key (constant-time compare).
 *   3. A per-agent connect token whose sha-256 matches a non-removed agent's
 *      `connect_token_hash`.
 */
export function verifyProxyCredential(headers: Headers, opts: ProxyAuthOpts): boolean {
  if (isAnthropicTempKey(headers))
    return true;

  const cred = extractProxyCredential(headers);
  if (!cred)
    return false;

  if (opts.staticKey && safeEqual(cred, opts.staticKey))
    return true;

  const hash = hashToken(cred);
  const row = opts.db
    .select({ id: agents.id })
    .from(agents)
    .where(and(eq(agents.connectTokenHash, hash), ne(agents.status, "removed")))
    .limit(1)
    .get();

  return row !== undefined;
}
