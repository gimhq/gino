import type { Hono, MiddlewareHandler } from "hono";
import type { Config } from "@/config";
import type { AppEnv } from "@/shared/lib/types";
import { TAGS } from "@gino/host-hono";
import { Scalar } from "@scalar/hono-api-reference";
import { openAPIRouteHandler } from "hono-openapi";
import { BUILD_INFO } from "@/build-info";

// Scalar serves an HTML shell that loads its bundle from a CDN and injects
// inline init scripts. The host's strict global CSP (`script-src 'self'`,
// see buildOuterApp) would block both, leaving a blank page. This relaxed
// policy is scoped to the `/docs` route only.
//
// `secureHeaders` writes its CSP *after* `await next()`, so an inner handler
// cannot override it. `docsCspRelax` therefore has to be registered on the
// outer app *before* `secureHeaders` — that way its own post-`next()` body
// runs last and wins. See app.ts.
const DOCS_CSP = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
  "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
  "img-src 'self' data: https://cdn.jsdelivr.net",
  "font-src 'self' data: https://cdn.jsdelivr.net",
  "connect-src 'self'",
  "worker-src 'self' blob:",
].join("; ");

export const docsCspRelax: MiddlewareHandler = async (c, next) => {
  await next();
  c.header("Content-Security-Policy", DOCS_CSP);
};

/**
 * Mount the OpenAPI spec (`/openapi.json`) and the Scalar API reference
 * (`/docs`) onto the host app. Call this *after* every module's routes are
 * mounted on `api`, since `openAPIRouteHandler` walks `api`'s route table to
 * build the spec (routes without `describeRoute` are omitted).
 */
export function mountDocs(api: Hono<AppEnv>, config: Config): void {
  const base = `${config.BASE_PATH}/api`;

  api.get(
    "/openapi.json",
    openAPIRouteHandler(api, {
      documentation: {
        openapi: "3.1.0",
        info: {
          title: `${config.APP_NAME} host API`,
          version: BUILD_INFO.version,
          description: "Gino host control plane. HTTP API generated from the in-process Hono route definitions.",
        },
        servers: [{ url: base, description: "This deployment" }],
        components: {
          securitySchemes: {
            bearerToken: {
              type: "http",
              scheme: "bearer",
              description: "Bearer token (admin / channel / per-agent connect token) via `Authorization: Bearer <token>`.",
            },
          },
        },
        tags: Object.values(TAGS).map(name => ({ name })),
      },
    }),
  );

  api.get(
    "/docs",
    Scalar({ url: `${base}/openapi.json`, pageTitle: `${config.APP_NAME} host API` }),
  );
}
