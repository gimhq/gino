// M1 — EDGE (transport + auth + route mounting; HTTP/WS termination only).
//
// The edge owns the explicit v2 prefix topology. `edgeRoutes()` composes the
// package routers (`@gino/host-hono` for `/agent/*`, `/admin/*`, `/messages/*`;
// `@gino/host-proxy/hono` for `/llm/*`) with the standalone app's policy: the
// per-surface rate limits are applied on the exact paths BEFORE the mounts
// (D10) and the operator bearers are injected as `authorize` callbacks.
// `app.ts` mounts the result onto the single `Hono<AppEnv>` app after the
// public (system) routes.
//
// No catch-all: an unknown path falls through to the scaffold's clean 404.
import type { Host } from "@gino/host-core";
import type { Config } from "@/config";
import type { AppEnv } from "@/shared/lib/types";
import { hashToken, registerError, safeEqual } from "@gino/host-core/domain";
import { adminRouter, agentRouter, bearerToken, messagesRouter } from "@gino/host-hono";
import { bunWebSocket } from "@gino/host-hono/bun";
import { extractProxyCredential, verifyProxyCredential } from "@gino/host-proxy";
import { llmRouter, stripLlmPrefix } from "@gino/host-proxy/hono";
import { Hono } from "hono";
import { getClientIp } from "@/shared/lib/client-ip";
import { adminTokenAuthorize, channelTokenAuthorize } from "@/shared/middleware/auth";
import { rateLimit } from "@/shared/middleware/rate-limit";

export interface EdgeDeps {
  readonly config: Config;
  readonly host: Host;
}

export function edgeRoutes({ config, host }: EdgeDeps) {
  const router = new Hono<AppEnv>();

  // ── Rate limits (app policy, ahead of every mount) ──────────────────────────

  // HOST-M1M2-10 / HOST-M9-02: throttle enrollment per client IP BEFORE the
  // token check, so shared-secret brute force and register-retry loops are
  // bounded. Over-limit renders the protocol §3 `register_error{rate_limited}`
  // envelope (Retry-After is set by the middleware).
  router.post("/agent/register", rateLimit({
    bucket: "agent-register",
    limits: config => ({ windowMs: config.RATE_LIMIT_REGISTER_WINDOW_MS, max: config.RATE_LIMIT_REGISTER_MAX }),
    onLimit: c => c.json(registerError("rate_limited"), 429),
  }));

  // HOST-M9-02: throttle the channel I/O surface ahead of auth. A VALID
  // channel bearer drains the principal budget (keyed by token hash, never
  // the raw secret); an absent or invalid bearer falls back to the client IP,
  // so brute-force guessing of the shared token cannot rotate keys to evade
  // the limit.
  router.use("/messages/*", rateLimit({
    bucket: "messages",
    limits: config => ({ windowMs: config.RATE_LIMIT_MESSAGES_WINDOW_MS, max: config.RATE_LIMIT_MESSAGES_MAX }),
    key: (c) => {
      const expected = c.var.config.HTTP_CHANNEL_TOKEN;
      const bearer = bearerToken(c);
      return expected && bearer && safeEqual(bearer, expected) ? `tok:${hashToken(bearer)}` : null;
    },
  }));

  // HOST-M7-03: throttle the whole /llm surface ahead of auth. A VERIFIED
  // proxy credential drains its own per-principal budget (keyed by token
  // hash, never the raw secret) so agents sharing a NAT don't collide; an
  // absent or invalid credential falls back to the client IP, so failed-auth
  // floods cannot rotate guessed tokens to evade the limit.
  router.use("/llm/*", rateLimit({
    bucket: "llm",
    limits: config => ({ windowMs: config.RATE_LIMIT_LLM_WINDOW_MS, max: config.RATE_LIMIT_LLM_MAX }),
    key: (c) => {
      const config = c.var.config;
      const cred = extractProxyCredential(c.req.raw.headers);
      if (!cred)
        return null;
      const { pathname } = stripLlmPrefix(c.req.url, config.BASE_PATH);
      const ok = verifyProxyCredential(c.req.raw.headers, {
        storage: c.var.host.storage,
        staticKey: config.PROXY_STATIC_KEY || undefined,
      }, pathname);
      return ok ? `cred:${hashToken(cred)}` : null;
    },
  }));

  // ── Routers ─────────────────────────────────────────────────────────────────

  // The shared `hono/bun` upgrade — its `websocket` handler is registered on
  // `Bun.serve` by `index.ts` (see `bootstrap`).
  const { upgradeWebSocket } = bunWebSocket();
  router.route("/agent", agentRouter(host, {
    upgradeWebSocket,
    clientIp: c => getClientIp(c, config),
    maxUploadBytes: config.MAX_UPLOAD_BYTES,
  }));
  router.route("/admin", adminRouter(host, { authorize: adminTokenAuthorize(config) }));
  router.route("/messages", messagesRouter(host, { authorize: channelTokenAuthorize(config) }));
  router.route("/llm", llmRouter(host, { config, basePath: config.BASE_PATH }));
  return router;
}

export const MODULE = "edge" as const;
