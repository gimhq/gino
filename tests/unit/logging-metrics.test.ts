// HOST-M9-04 — metrics label cardinality must be bounded: unmatched (404)
// paths collapse into a single "other" label and a defensive cap stops
// distinct-label growth, so unauthenticated path spraying cannot grow the
// no-eviction registry without bound.
import type { AppEnv } from "@/shared/lib/types";
import { beforeEach, describe, expect, it } from "bun:test";
import { Hono } from "hono";
import { __resetMetricsForTests, renderPrometheus } from "@/shared/lib/metrics";
import { __resetRouteLabelsForTests, loggingMiddleware, routeLabel } from "@/shared/middleware/logging";

const noopLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  fatal: () => {},
};

function makeApp() {
  const app = new Hono<AppEnv>();
  app.use("*", (c, next) => {
    c.set("logger", noopLogger as unknown as AppEnv["Variables"]["logger"]);
    return next();
  });
  app.use("*", loggingMiddleware());
  app.get("/api/health-check-route", c => c.json({ ok: true }));
  return app;
}

beforeEach(() => {
  __resetMetricsForTests();
  __resetRouteLabelsForTests();
});

describe("routeLabel (HOST-M9-04)", () => {
  it("buckets unmatched paths into \"other\"", () => {
    expect(routeLabel("/api/sprayed-alpha-path", false)).toBe("other");
  });

  it("caps distinct route labels defensively", () => {
    for (let i = 0; i < 256; i++) {
      expect(routeLabel(`/api/r${i}x`, true)).toBe(`/api/r${i}x`);
    }
    // Cap reached — new labels collapse, already-seen labels survive.
    expect(routeLabel("/api/one-more-route", true)).toBe("other");
    expect(routeLabel("/api/r0x", true)).toBe("/api/r0x");
  });
});

describe("loggingMiddleware metrics (HOST-M9-04)", () => {
  it("sprayed 404 paths share one \"other\" registry entry", async () => {
    const app = makeApp();
    for (const path of ["/api/aaa", "/api/aab", "/api/abc-def"]) {
      const res = await app.fetch(new Request(`http://local${path}`));
      expect(res.status).toBe(404);
    }
    const text = renderPrometheus();
    expect(text).toContain("route=\"other\"");
    expect(text).not.toContain("/api/aaa");
    expect(text).not.toContain("/api/abc-def");
    const match = text.match(/http_requests_total\{method="GET",route="other",status="404"\} (\d+)/);
    expect(match?.[1]).toBe("3");
  });

  it("matched routes keep their real label", async () => {
    const app = makeApp();
    const res = await app.fetch(new Request("http://local/api/health-check-route"));
    expect(res.status).toBe(200);
    expect(renderPrometheus()).toContain("route=\"/api/health-check-route\"");
  });
});
