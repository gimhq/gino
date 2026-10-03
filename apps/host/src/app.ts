import type { Host, HostOptions, Launcher } from "@gino/host-core";
import type { AppDatabase } from "@gino/host-store-sqlite";
import type { Config } from "./config";
import type { Logger } from "./shared/lib/logger";
import type { AppEnv } from "./shared/lib/types";
import { startWebhookPump } from "@gino/host-channels";
import { createDockerLauncher, createHost, manualLauncher, resolveHostOptions } from "@gino/host-core";
import { errorHandler } from "@gino/host-hono";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { Hono } from "hono";
import { websocket } from "hono/bun";
import { cors } from "hono/cors";
import { requestId } from "hono/request-id";
import { secureHeaders } from "hono/secure-headers";
import { bunSpawn } from "./agents-bun-spawn";
import { BUILD_INFO } from "./build-info";
import { loadConfig } from "./config";
import { edgeRoutes } from "./edge";
import { docsCspRelax, mountDocs } from "./modules/docs";
import { publicRoutes } from "./routes";
import { isSpoofableProxyConfig } from "./shared/lib/client-ip";
import { createLogger } from "./shared/lib/logger";
import { registryMetrics, renderPrometheus } from "./shared/lib/metrics";
import { adminTokenAuth } from "./shared/middleware/auth";
import { loggingMiddleware } from "./shared/middleware/logging";
import { propagateRequestId } from "./shared/middleware/request-id";

// ─── Types ───

interface AppDeps {
  readonly config: Config;
  /** The per-instance host (storage, logger, sessions, launchers, metrics). */
  readonly host: Host;
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
  /** The host the app was composed with. */
  readonly host: Host;
  /**
   * The open database handle. Exposed so the boot entrypoint can checkpoint the
   * WAL on the lode staged-update prepare prompt (src/index.ts). The request
   * pipeline reaches the database only through `host.storage`, not this.
   */
  readonly db: AppDatabase;
}

// ─── Host composition ───

export interface AppHostDeps {
  readonly config: Config;
  readonly db: AppDatabase;
  readonly logger: Logger;
  readonly launchers?: readonly Launcher[];
}

/** The single place env config becomes SDK options (docs/host-sdk-design.md §5.2). */
export function hostOptionsFromConfig(config: Config): HostOptions {
  return resolveHostOptions({
    storeDir: config.STORE_DIR,
    maxUploadBytes: config.MAX_UPLOAD_BYTES,
    registerToken: config.REGISTER_TOKEN,
    registrationEnabled: config.REGISTER_ENABLED,
    configGc: { enabled: config.CONFIG_GC_ENABLED, intervalMs: config.CONFIG_GC_INTERVAL_MS, maxAgeMs: config.CONFIG_GC_MAX_AGE_MS },
    webhooks: { timeoutMs: config.WEBHOOK_TIMEOUT_MS, maxAttempts: config.WEBHOOK_MAX_ATTEMPTS, retryIntervalMs: config.WEBHOOK_RETRY_INTERVAL_MS },
  });
}

/** The standalone app's default launchers: `manual` (safe default) + `docker` shelling out via `Bun.spawn`. */
export function defaultAppLaunchers(): readonly Launcher[] {
  return [manualLauncher, createDockerLauncher({ spawn: bunSpawn })];
}

/**
 * Build the host the way the standalone app does: the sqlite adapter over `db`
 * with the blob store at `STORE_DIR`, the global Prometheus registry as the
 * metrics sink, and the manual + docker launchers unless overridden.
 */
export function createAppHost({ config, db, logger, launchers }: AppHostDeps): Host {
  const options = hostOptionsFromConfig(config);
  return createHost({
    storage: sqliteStorage(db, { storeDir: options.storeDir }),
    logger,
    metrics: registryMetrics,
    launchers: launchers ?? defaultAppLaunchers(),
    storeDir: options.storeDir,
    agents: {
      registerToken: options.registerToken,
      registrationEnabled: options.registrationEnabled,
      heartbeatIntervalMs: options.heartbeatIntervalMs,
      offlineGraceMs: options.offlineGraceMs,
    },
    attachments: { maxUploadBytes: options.maxUploadBytes },
    configGc: options.configGc,
    webhooks: options.webhooks,
  });
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
 * CORS, structured logging), threads the per-request context (host / config /
 * logger), mounts the OpenAPI docs and the route modules, then wraps it in
 * the security-header outer shell.
 *
 * Exported so tests can construct the app with injected deps (no env / no
 * Bun.serve). The data-plane middleware + routers land here as M1–M8 ship.
 */
export function createApp({ config, host }: AppDeps) {
  // Surface the spoofable proxy configuration at composition
  // time: TRUST_PROXY without an allow-list means any direct client can
  // forge X-Forwarded-For and evade every IP-keyed control.
  if (isSpoofableProxyConfig(config)) {
    host.logger.warn(
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
    c.set("host", host);
    c.set("config", config);
    c.set("logger", host.logger);
    return next();
  });
  api.use("*", loggingMiddleware());

  // OpenAPI spec (/openapi.json) + Scalar UI (/docs). Mounted BEFORE the
  // route modules so the docs sit outside any future module-level guards;
  // `openAPIRouteHandler` still walks the full route table lazily at request
  // time, so routes registered below are included in the spec.
  mountDocs(api, config);

  // Prometheus scrape endpoint for the in-process registry.
  // Guarded by the admin bearer (same credential as /admin/*); an
  // unconfigured token fails closed, so this is never an open surface. The
  // logging middleware's SKIP_METRICS_RE keeps scrapes out of the metrics.
  api.get("/metrics", adminTokenAuth, c => c.text(renderPrometheus()));

  api.route("/", publicRoutes());

  // M1 edge — the explicit v2 prefix topology (`/agent/*`, `/admin/*`,
  // `/messages/*`, `/llm/*`): the package routers with the app's rate limits
  // and bearer policy composed in (src/edge). Mounted after the public system
  // routes. No catch-all → unknown paths fall through to the clean 404.
  api.route("/", edgeRoutes({ config, host }));

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
  const db = createDb(config.DB_PATH, { version: BUILD_INFO.version });

  const host = createAppHost({ config, db, logger });
  const app = createApp({ config, host });

  // Background loops start here (not in `createApp`) so tests that build the
  // app directly never spin a background timer. `host.start()` owns the core
  // loops — M3 liveness sweep, M2 outbox retention GC, M3 pending-launch
  // reaper, M8 store GC — every timer `unref`'d so
  // none holds the process open; `host.stop()` (index.ts) halts them, closes
  // the live sockets and then the DB.
  host.start();

  // M6 webhook egress pump — the production driver of outbound
  // webhook delivery: pushed on the `transcript.message` host event, retried
  // on a tick. App-owned, so it is registered with the host and stopped first.
  host.registerService(startWebhookPump({
    storage: host.storage,
    logger,
    events: host.events,
    timeoutMs: host.options.webhooks.timeoutMs,
    maxAttempts: host.options.webhooks.maxAttempts,
    intervalMs: host.options.webhooks.retryIntervalMs,
  }));

  return {
    fetch: (req, env) => app.fetch(req, env),
    websocket,
    config,
    logger,
    host,
    db,
  };
}
