import type { AppEnv } from "@/shared/lib/types";
import { createMiddleware } from "hono/factory";
import { getClientIp } from "@/shared/lib/client-ip";

interface Bucket {
  count: number;
  resetAt: number;
}

export interface RateLimitOptions {
  /** Window length in milliseconds. */
  readonly windowMs: number;
  /** Max requests per IP per window. */
  readonly max: number;
  /** Logical bucket id; share between routes that should drain the same budget. */
  readonly bucket: string;
}

/**
 * Hard cap on entries per bucket; on overflow the entry with the smallest
 *  `resetAt` is evicted so legitimate active sessions are preserved.
 */
const MAX_ENTRIES_PER_BUCKET = 10_000;

const buckets = new Map<string, Map<string, Bucket>>();
let smallestWindowMs = Number.POSITIVE_INFINITY;
let gcTimer: ReturnType<typeof setInterval> | undefined;

function getBucketMap(name: string): Map<string, Bucket> {
  let map = buckets.get(name);
  if (!map) {
    map = new Map();
    buckets.set(name, map);
  }
  return map;
}

/** Drop expired entries from every bucket. Run by the background GC timer. */
function pruneExpired(now: number): void {
  for (const map of buckets.values()) {
    for (const [key, val] of map) {
      if (now >= val.resetAt)
        map.delete(key);
    }
  }
}

/**
 * Evict the entry with the smallest `resetAt` (closest to expiry) to make
 *  room for a new one when a bucket has hit `MAX_ENTRIES_PER_BUCKET`.
 */
function evictOldest(map: Map<string, Bucket>): void {
  let oldestKey: string | undefined;
  let oldestResetAt = Number.POSITIVE_INFINITY;
  for (const [key, val] of map) {
    if (val.resetAt < oldestResetAt) {
      oldestResetAt = val.resetAt;
      oldestKey = key;
    }
  }
  if (oldestKey !== undefined)
    map.delete(oldestKey);
}

/**
 * Lazily start (or re-tune) the background GC sweep. The interval is
 *  bounded by the smallest configured `windowMs` across all buckets so an
 *  expired entry is never alive for more than one window past its reset.
 */
function ensureGcTimer(windowMs: number): void {
  if (windowMs >= smallestWindowMs && gcTimer)
    return;

  smallestWindowMs = Math.min(smallestWindowMs, windowMs);

  if (gcTimer)
    clearInterval(gcTimer);

  gcTimer = setInterval(() => pruneExpired(Date.now()), smallestWindowMs);
  // Don't keep the event loop alive solely for the GC sweep.
  gcTimer.unref?.();
}

/**
 * Per-IP rate limiter. Uses the resolved client IP (peer IP by default, or
 * sanitised proxy headers when `config.TRUST_PROXY` is true); unresolved
 * peers share a single `anon` bucket to prevent header churn from evading
 * the gate.
 *
 * Pruning is performed by a single background `setInterval` (one timer total,
 * `unref()`'d, period bounded by the smallest configured window) instead of
 * an inline O(n) sweep on the request path.
 */
export function rateLimit(opts: RateLimitOptions) {
  const { windowMs, max, bucket } = opts;
  const map = getBucketMap(bucket);
  ensureGcTimer(windowMs);

  return createMiddleware<AppEnv>(async (c, next) => {
    const ip = getClientIp(c, c.var.config) ?? "anon";
    const now = Date.now();
    const entry = map.get(ip);

    if (entry && now < entry.resetAt) {
      if (entry.count >= max) {
        const retryAfter = Math.max(1, Math.ceil((entry.resetAt - now) / 1000));
        c.header("Retry-After", String(retryAfter));
        return c.json(
          { success: false, error: { code: "RATE_LIMITED", message: "Too many requests. Try again later." } },
          429,
        );
      }
      entry.count++;
    }
    else {
      if (map.size >= MAX_ENTRIES_PER_BUCKET)
        evictOldest(map);
      map.set(ip, { count: 1, resetAt: now + windowMs });
    }

    return next();
  });
}

/**
 * Test-only: drop all in-memory bucket state. Call from `beforeEach` in tests
 * that exercise rate-limited routes so leftover hits from the previous case
 * do not bleed into the next one (the `getClientIp("anon")` fallback shares
 * a bucket across all synthetic Requests).
 */
export function __resetRateLimitForTests(): void {
  buckets.clear();
  if (gcTimer) {
    clearInterval(gcTimer);
    gcTimer = undefined;
  }
  smallestWindowMs = Number.POSITIVE_INFINITY;
}
