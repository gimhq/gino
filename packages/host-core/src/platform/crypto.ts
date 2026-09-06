// M9 — PLATFORM / crypto. Constant-time comparison + token hashing helpers
// used by the three-token auth middleware (M1) and, from B2, by the per-agent
// connect-token store (M3 `agents.connect_token_hash`).
//
// These ADD to the platform surface; the existing `src/shared/lib` is left
// untouched (the platform barrel re-exports its logger/errors/ids).
import { createHash, timingSafeEqual } from "node:crypto";

/**
 * Constant-time string equality. Both inputs are SHA-256 digested first so the
 * comparison is over fixed-length (32-byte) buffers — this hides any length
 * difference and lets `timingSafeEqual` run without throwing on unequal
 * lengths. Use for every secret/token comparison; never `===`.
 */
export function safeEqual(a: string, b: string): boolean {
  const ha = createHash("sha256").update(a).digest();
  const hb = createHash("sha256").update(b).digest();
  return timingSafeEqual(ha, hb);
}

/** SHA-256 hex digest of a token. Stored (never the plaintext) as the at-rest form. */
export function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

/**
 * Verify a plaintext token against a stored `hashToken(...)` digest, in
 * constant time. Returns false on any mismatch (including malformed hashes).
 */
export function verifyToken(token: string, hash: string): boolean {
  return safeEqual(hashToken(token), hash);
}
