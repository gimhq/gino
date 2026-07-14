import type { WebhookPump } from "./channels";
import type { Config } from "./config";
import type { AppDatabase } from "./db";
import type { Logger } from "./shared/lib/logger";
import type { AppEnv } from "./shared/lib/types";
import { Hono } from "hono";
import { websocket } from "hono/bun";
import { cors } from "hono/cors";
import { requestId } from "hono/request-id";
import { secureHeaders } from "hono/secure-headers";
import { startLaunchReaper } from "./agents/launch-reaper";
import { startLivenessSweep } from "./agents/liveness";
import { sessions } from "./agents/sessions";
import { startWebhookPump } from "./channels";
import { loadConfig } from "./config";
import { createDb } from "./db";
import { edgeRoutes } from "./edge";
import { docsCspRelax, mountDocs } from "./modules/docs";
import { startOutboxGc } from "./protocol/outbox";
import { publicRoutes } from "./routes";
import { isSpoofableProxyConfig } from "./shared/lib/client-ip";
import { createLogger } from "./shared/lib/logger";
import { renderPrometheus } from "./shared/lib/metrics";
import { adminTokenAuth } from "./shared/middleware/auth";
import { errorHandler } from "./shared/middleware/error-handler";
import { loggingMiddleware } from "./shared/middleware/logging";
import { propagateRequestId } from "./shared/middleware/request-id";

// ─── Types ───

interface AppDeps {
  readonly config: Config;
  readonly db: AppDatabase;
  readonly logger: Logger;
  /** M6 webhook egress pump (HOST-M5M6-04) — started by bootstrap; absent in tests that compose the app directly. */
  readonly webhookPump?: WebhookPump;
}

export interface BootstrapResult {
  readonly fetch: (req: Request, env?: Record<string, unknown>) => Response | Promise<Response>;
  /**
   * The Bun WebSocket handler (M2/M3 frame pipe). Pass to `Bun.serve({ websocket })`
   * so `/agent/ws/:agentId` upgrades; the `server` must be threaded into the
   * fetch `env` (see `index.ts`) for the hono/bun adapter to upgrade.
   */
  readonly websocket: typeof websocket;
  readonly config: Config;
  readonly logger: Logger;
  /**
   * The open database handle. Exposed so the boot entrypoint can ping it for the
   * lode readiness probe and checkpoint the WAL on the staged-update prepare
   * prompt (src/index.ts). The request pipeline uses the per-request `db` from
   * the hono context, not this.
   */
  readonly db: AppDatabase;
  /** Close the DB connection. Call on shutdown. */
  readonly closeDb: () => void;
}

// ─── App composer ───

// CORS_ORIGIN may be a comma-separated list. With no value, dev allows any
// origin and production locks it down (empty = same-origin only).
function resolveCorsOrigin(config: Config): string | string[] {
  if (config.CORS_ORIGIN) {
    const list = config.CORS_ORIGIN.split(",").map(s => s.trim()).filter(Boolean);
    return list.length === 1 ? list[0]! : list;
  }
  return config.NODE_ENV === "production" ? "" : "*";
}

/**
 * Build the host's single `Hono<AppEnv>` API app. Owns transport (request-id,
 * CORS, structured logging), threads the per-request context (db / config /
 * logger), mounts the OpenAPI docs and the route modules, then wraps it in
 * the security-header outer shell.
 *
 * Exported so tests can construct the app with injected deps (no env / no
 * Bun.serve). The data-plane middleware + routers land here as M1–M8 ship.
 */
export function createApp({ config, db, logger, webhookPump }: AppDeps) {
  // HOST-M9-08 — surface the spoofable proxy configuration at composition
  // time: TRUST_PROXY without an allow-list means any direct client can
  // forge X-Forwarded-For and evade every IP-keyed control.
  if (isSpoofableProxyConfig(config)) {
    logger.warn(
      "TRUST_PROXY is enabled without TRUSTED_PROXY_IPS — forwarding headers are accepted from ANY peer, so client IPs are forgeable; set GINO_TRUSTED_PROXY_IPS to the proxy addresses",
    );
  }

  const api = new Hono<AppEnv>();

  // Hono's `requestId()` accepts a well-formed inbound `X-Request-Id` and
  // otherwise mints one; `propagateRequestId` echoes it on the response so a
  // user-reported failure can be matched against the log line.
  api.use("*", requestId());
  api.use("*", propagateRequestId);
  api.use("*", cors({ origin: resolveCorsOrigin(config) }));
  api.use("*", (c, next) => {
    c.set("db", db);
    c.set("config", config);
    c.set("logger", logger);
    if (webhookPump) {
      c.set("webhookPump", webhookPump);
    }
    return next();
  });
  api.use("*", loggingMiddleware());

  // OpenAPI spec (/openapi.json) + Scalar UI (/docs). Mounted BEFORE the
  // route modules so the docs sit outside any future module-level guards;
  // `openAPIRouteHandler` still walks the full route table lazily at request
  // time, so routes registered below are included in the spec.
  mountDocs(api, config);

  // HOST-M9-09 — Prometheus scrape endpoint for the in-process registry.
  // Guarded by the admin bearer (same credential as /admin/*, HOST-XC-02); an
  // unconfigured token fails closed, so this is never an open surface. The
  // logging middleware's SKIP_METRICS_RE keeps scrapes out of the metrics.
  api.get("/metrics", adminTokenAuth, c => c.text(renderPrometheus()));

  api.route("/", publicRoutes());

  // M1 edge — the explicit v2 prefix topology (`/agent/*`, `/admin/*`,
  // `/messages/*`), each router carrying its own token auth. Mounted after the
  // public system routes; `/llm/*` (M7) lands with B5. No catch-all → unknown
  // paths fall through to the clean 404.
  api.route("/", edgeRoutes());

  api.onError(errorHandler);

  return buildOuterApp(api, config);
}

// ─── Outer shell ───

function buildOuterApp(api: Hono<AppEnv>, config: Config) {
  const app = new Hono<AppEnv>();
  const base = config.BASE_PATH;

  // Scoped CSP relaxation for the Scalar docs page. Registered before
  // `secureHeaders` so its post-`next()` override wins over the strict global
  // policy (which would otherwise block Scalar's CDN bundle).
  app.use(`${base}/api/docs`, docsCspRelax);

  // Security headers for every response.
  app.use("*", secureHeaders({
    referrerPolicy: "strict-origin-when-cross-origin",
    crossOriginOpenerPolicy: "same-origin",
    crossOriginResourcePolicy: "same-origin",
    xFrameOptions: "SAMEORIGIN",
    xContentTypeOptions: "nosniff",
    xDownloadOptions: "noopen",
    permissionsPolicy: {
      camera: [],
      microphone: [],
      geolocation: [],
      payment: [],
    },
    contentSecurityPolicy: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "blob:"],
      fontSrc: ["'self'", "data:"],
      connectSrc: ["'self'"],
      frameAncestors: ["'self'"],
      baseUri: ["'self'"],
      formAction: ["'self'"],
      objectSrc: ["'none'"],
    },
  }));

  app.route(`${base}/api`, api);

  return app;
}

// ─── Bootstrap (production entry) ───

/**
 * Load config, create the logger + DB, and compose the app. Used by
 * `index.ts`. Synchronous: config + bun:sqlite both resolve without I/O
 * awaits.
 */
export function bootstrap(): BootstrapResult {
  const config = loadConfig();
  const logger = createLogger(config);
  const db = createDb(config.DB_PATH);

  // M6 webhook egress pump (HOST-M5M6-04) — the production driver of outbound
  // webhook delivery: pushed on inbound transcript writes, retried on a tick.
  // Started here (not in `createApp`) so tests that build the app directly
  // never spin a background timer.
  const webhookPump = startWebhookPump({
    db,
    logger,
    timeoutMs: config.WEBHOOK_TIMEOUT_MS,
    maxAttempts: config.WEBHOOK_MAX_ATTEMPTS,
    intervalMs: config.WEBHOOK_RETRY_INTERVAL_MS,
  });

  const app = createApp({ config, db, logger, webhookPump });

  // M3 liveness — periodic offline-grace sweep (DP4). The timer is `unref`'d so
  // it never holds the process open; `stop()` is folded into `closeDb` so clean
  // shutdown halts it before the DB closes. Started here (not in `createApp`)
  // so tests that build the app directly never spin a background timer.
  const liveness = startLivenessSweep({ db, logger, sessions });

  // M2 outbox retention — periodic GC of acked rows past the retention window
  // (unacked rows are never touched). Same lifecycle as the liveness sweep.
  const outboxGc = startOutboxGc({ db, logger });

  // M3 pending-launch reaper (HOST-M3-05) — tears down runtimes whose agent
  // never registered inside the TTL. The `pending_launches` table is persisted,
  // so this also reaps orphans left by a host restart.
  const launchReaper = startLaunchReaper({ db, logger });

  return {
    fetch: (req, env) => app.fetch(req, env),
    websocket,
    config,
    logger,
    db,
    closeDb: () => {
      liveness.stop();
      outboxGc.stop();
      launchReaper.stop();
      webhookPump.stop();
      db.close();
    },
  };
}
