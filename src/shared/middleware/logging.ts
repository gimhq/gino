import type { AppEnv } from "@/shared/lib/types";
import { createMiddleware } from "hono/factory";
import { counterAdd, histogramObserve } from "@/shared/lib/metrics";

// Health probes (k8s/Docker) and CORS preflights run several times a second.
// Logging them adds tens of MB/day of useless lines and starves the more
// useful request logs of attention. Skip them — operators get probe outcomes
// from the orchestrator's own health UI instead.
const SKIP_LOGGING_RE = /\/health(?:\/\w+)?$/;
const SKIP_METRICS_RE = /\/(?:health|metrics)(?:\/\w+)?$/;

// Coarsen path labels so high-cardinality ids don't blow up the metrics
// registry. Match id-shaped segments only — alpha-only route names like
// `callback` are preserved:
//   - 32+ hex chars (UUID / SHA digest / encrypted-blob hash)
//   - 8+ chars that mix letters AND digits (nanoid / ULID / unguessable suffix)
//   - All-digit segments of 4+ chars (numeric ids)
const RE_HEX_TOKEN = /\/[0-9a-f]{32,}(?=\/|$)/gi;
const RE_ALL_DIGITS = /\/\d{4,}(?=\/|$)/g;
const RE_MIXED_TOKEN = /\/(?=[\w-]{8,}(?:\/|$))(?=[\w-]*\d)(?=[\w-]*[A-Z])[\w-]+(?=\/|$)/gi;

export function coarsenPath(path: string): string {
  return path
    .replace(RE_HEX_TOKEN, "/:id")
    .replace(RE_ALL_DIGITS, "/:id")
    .replace(RE_MIXED_TOKEN, "/:id");
}

export function loggingMiddleware() {
  return createMiddleware<AppEnv>(async (c, next) => {
    if (c.req.method === "OPTIONS" || SKIP_LOGGING_RE.test(c.req.path)) {
      return next();
    }
    const start = performance.now();
    try {
      await next();
    }
    finally {
      const durationMs = performance.now() - start;
      const status = c.res.status;
      const route = coarsenPath(c.req.path);
      c.get("logger").info({
        method: c.req.method,
        path: c.req.path,
        status,
        duration: Math.round(durationMs),
        requestId: c.get("requestId"),
      }, "request completed");
      // Skip /health + /metrics from prom output too — they would otherwise
      // dominate the histogram and dilute signal.
      if (!SKIP_METRICS_RE.test(c.req.path)) {
        counterAdd(
          "http_requests_total",
          "Total HTTP requests served, partitioned by method/route/status.",
          1,
          { method: c.req.method, route, status },
        );
        histogramObserve(
          "http_request_duration_seconds",
          "HTTP request duration in seconds.",
          durationMs / 1000,
          { method: c.req.method, route },
        );
      }
    }
  });
}
