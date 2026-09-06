import type { Context } from "hono";
import type { Config } from "@/config";
import type { AppEnv } from "@/shared/lib/types";
import { createMiddleware } from "hono/factory";
import { getClientIp } from "@/shared/lib/client-ip";

interface Bucket {
  count: number;
  resetAt: number;
}

export interface RateLimitOptions {
  /** Logical bucket id; share between routes that should drain the same budget. */
  readonly bucket: string;
  /**
   * Resolve `{ windowMs, max }` from the per-request config, so limits follow
   * the repo's env-driven config pattern instead of being frozen at mount time.
   */
  readonly limits: (config: Config) => { readonly windowMs: number; readonly max: number };
  /**
   * Resolve the principal key for a request (e.g. a VERIFIED token's hash —
   * never the raw secret). Return null/undefined to fall back to the per-IP
   * key; unverified/absent credentials should fall back so failed-auth floods
   * cannot rotate keys to evade the limit. Default: always per client IP.
   */
  readonly key?: (c: Context<AppEnv>) => string | null | undefined;
  /**
   * Render the over-limit response (e.g. the protocol §3 `register_error`
   * envelope). The `Retry-After` header is already set on the context.
   * Default: 429 with the standard error envelope.
   */
  readonly onLimit?: (c: Context<AppEnv>, retryAfterSeconds: number) => Response;
}

/**
 * Hard cap on entries per bucket; on overflow expired entries are pruned and,
 * if the bucket is still full, the entry with the smallest `resetAt` is
 * evicted so legitimate active sessions are preserved.
 */
const MAX_ENTRIES_PER_BUCKET = 10_000;

/**
 * Bucket state is keyed by the app's resolved `Config` object identity (one
 * per process in production, one per constructed app in tests — so parallel
 * test apps never bleed hits into each other), then by the logical bucket id.
 */
const stores = new WeakMap<Config, Map<string, Map<string, Bucket>>>();

function getBucketMap(config: Config, name: string): Map<string, Bucket> {
  let byBucket = stores.get(config);
  if (!byBucket) {
    byBucket = new Map();
    stores.set(config, byBucket);
  }
  let map = byBucket.get(name);
  if (!map) {
    map = new Map();
    byBucket.set(name, map);
  }
  return map;
}

/** Drop expired entries. Run lazily when a bucket hits its entry cap. */
function pruneExpired(map: Map<string, Bucket>, now: number): void {
  for (const [key, val] of map) {
    if (now >= val.resetAt)
      map.delete(key);
  }
}

/**
 * Evict the entry with the smallest `resetAt` (closest to expiry) to make
 * room for a new one when a bucket is full even after pruning.
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
 * Fixed-window rate limiter. Keyed per principal when the `key` hook resolves
 * one, otherwise per resolved client IP (peer IP by default, or sanitised
 * proxy headers when `config.TRUST_PROXY` is true); unresolved peers share a
 * single `unknown` bucket to prevent header churn from evading the gate.
 *
 * Expired entries are pruned lazily when a bucket reaches its entry cap, so
 * memory stays bounded without a background timer on the request path.
 */
export function rateLimit(opts: RateLimitOptions) {
  const { bucket, limits, key, onLimit } = opts;

  return createMiddleware<AppEnv>(async (c, next) => {
    const config = c.var.config;
    const { windowMs, max } = limits(config);
    const map = getBucketMap(config, bucket);
    const id = key?.(c) ?? `ip:${getClientIp(c, config)}`;
    const now = Date.now();
    const entry = map.get(id);

    if (entry && now < entry.resetAt) {
      if (entry.count >= max) {
        const retryAfter = Math.max(1, Math.ceil((entry.resetAt - now) / 1000));
        c.header("Retry-After", String(retryAfter));
        if (onLimit)
          return onLimit(c, retryAfter);
        return c.json(
          { success: false, error: { code: "RATE_LIMITED", message: "Too many requests. Try again later." } },
          429,
        );
      }
      entry.count++;
    }
    else {
      if (map.size >= MAX_ENTRIES_PER_BUCKET) {
        pruneExpired(map, now);
        if (map.size >= MAX_ENTRIES_PER_BUCKET)
          evictOldest(map);
      }
      map.set(id, { count: 1, resetAt: now + windowMs });
    }

    return next();
  });
}
