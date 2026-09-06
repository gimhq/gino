import type { Host } from "@gino/host-core";
import type { HostEnv } from "@gino/host-hono";
import type { MiddlewareHandler } from "hono";
import type { WsFactory } from "../bridge";
import type { ProxyEnv } from "../config";
import type { ClientSocket, CodexRelay } from "../ws-bridge";
import { UnauthorizedError } from "@gino/host-core";
import { bindHost, describeRoute, errors, raw, TAGS } from "@gino/host-hono";
import { Hono } from "hono";
import { upgradeWebSocket } from "hono/bun";
import { verifyProxyCredential } from "../auth";
import { buildProxyConfig } from "../config";
import { CredentialStore } from "../credentials";
import { createProxyHandler } from "../handler";
import { createCodexRelay } from "../ws-bridge";

/**
 * M7 — `/llm/*` credential proxy (mount at `/llm`). The engine's LLM traffic
 * enters here (its ANTHROPIC_BASE_URL/OPENAI_BASE_URL = `<hostUrl>/llm`); the
 * host injects the real upstream credential and forwards. Explicit prefixes
 * only — no catch-all, so an unknown `/llm/...` path 404s. Every route is
 * guarded by the proxy credential (per-agent connect token vs
 * `agents.connect_token_hash`, or the static proxy key — see `../auth`).
 *
 * The forward handler (provider detect, credential inject, header strip,
 * model-map, Claude→OpenAI bridge) lives in the package root; this router is
 * the thin transport edge that strips the `/llm` prefix and delegates.
 *
 * BUN-ONLY (`@gino/host-proxy/hono`, host-sdk-design §7.5): `/v1/responses`
 * (the raw Codex WS passthrough) is a real WebSocket UPGRADE over the shared
 * `hono/bun` `upgradeWebSocket` (the same instance whose `websocket` handler
 * the app registers on `Bun.serve`) and hands the upgraded peer to
 * `createCodexRelay` (raw bidirectional Codex passthrough). The
 * proxy-credential gate runs as ordinary `/llm` middleware BEFORE the upgrade,
 * so an unauthenticated handshake is rejected and no socket opens. A plain
 * (non-Upgrade) GET gets a clean `426 Upgrade Required`.
 *
 * Rate limiting (HOST-M7-03, per verified principal) is the embedder's
 * middleware, applied ahead of this mount (D10).
 */
export interface LlmRouterOptions {
  /** The host env's `PROXY_*` settings (the standalone app passes its resolved config). */
  readonly config: ProxyEnv;
  /** The app's base path: the router is expected to be mounted at `${basePath}/api/llm` (see `stripLlmPrefix`). */
  readonly basePath: string;
  /**
   * Override the upstream Codex WS factory for `/v1/responses`. Production uses
   * the real `WebSocket`; tests inject a fake so the relay is exercised with no
   * network. Passed straight through to `createCodexRelay`.
   */
  readonly wsFactory?: WsFactory;
  /** Override the upstream Codex WS URL (tests point at a fake endpoint). */
  readonly upstreamUrl?: string;
}

export function llmRouter(host: Host, opts: LlmRouterOptions) {
  const router = new Hono<HostEnv>();
  const { basePath } = opts;
  const logger = host.logger;

  // The proxy handler + credential store are built once per router so the
  // Codex OAuth refresh state in the CredentialStore persists — and so the WS
  // relay shares the same store as the HTTP forward path.
  const proxyConfig = buildProxyConfig(opts.config, logger);
  const credentials = new CredentialStore(proxyConfig, { logger });
  const handler = createProxyHandler({ config: proxyConfig, credentials, logger });
  const staticKey = opts.config.PROXY_STATIC_KEY || undefined;

  /**
   * Validate the engine's proxy credential; 401 (UnauthorizedError) on failure.
   * Auth is path-aware: an Anthropic temp key only clears the pass-through
   * paths, never the host-credential-injecting ones (HOST-M7-01).
   */
  const proxyAuth: MiddlewareHandler<HostEnv> = async (c, next) => {
    const { pathname } = stripLlmPrefix(c.req.url, basePath);
    const ok = verifyProxyCredential(c.req.raw.headers, { storage: host.storage, staticKey }, pathname);
    if (!ok)
      throw new UnauthorizedError();
    await next();
  };

  router.use("*", bindHost(host));

  // Proxy-credential auth on every /llm route — including the WS handshake, so
  // an unauthenticated upgrade is rejected (401) before any socket opens.
  router.use("*", proxyAuth);

  const forward = (summary: string) => [
    describeRoute({
      tags: [TAGS.Proxy],
      summary,
      responses: { ...raw(200, "application/json", "Upstream response (passthrough / SSE)"), ...errors(401, 404, 502) },
    }),
    (c: Parameters<MiddlewareHandler<HostEnv>>[0]) => {
      const { pathname, search } = stripLlmPrefix(c.req.url, basePath);
      return handler.handle(c.req.raw, pathname, search);
    },
  ] as const;

  router.post("/v1/messages", ...forward("Anthropic Messages (proxied; bridged to OpenAI when the model maps to OpenAI)"));
  router.post("/v1/chat/completions", ...forward("OpenAI Chat Completions (proxied)"));
  router.get("/v1/models", ...forward("List models (proxied)"));
  router.all("/api/oauth/*", ...forward("OAuth exchange (proxied; host injects the real OAuth token)"));

  // Raw Codex Responses WS passthrough. The shared `upgradeWebSocket` builds the
  // event handlers; on open we mint a relay bound to this peer + the proxy's
  // CredentialStore (which injects the upstream OAuth creds). Client frames are
  // buffered until upstream is ready, then relayed both ways (see ws-bridge).
  const wsUpgrade = upgradeWebSocket((c) => {
    const { search } = stripLlmPrefix(c.req.url, basePath);
    let relay: CodexRelay | null = null;
    return {
      onOpen(_evt, ws) {
        const client: ClientSocket = {
          send: data => ws.send(data),
          close: (code, reason) => ws.close(code, reason),
        };
        relay = createCodexRelay(client, {
          store: credentials,
          logger,
          search,
          ...(opts.wsFactory ? { wsFactory: opts.wsFactory } : {}),
          ...(opts.upstreamUrl ? { upstreamUrl: opts.upstreamUrl } : {}),
        });
        void relay.start();
      },
      onMessage(evt, _ws) {
        const data = evt.data;
        if (typeof data === "string" || data instanceof ArrayBuffer)
          relay?.onClientMessage(data);
      },
      onClose() {
        relay?.onClientClose();
      },
    };
  });

  router.get(
    "/v1/responses",
    describeRoute({
      tags: [TAGS.Proxy],
      summary: "Codex Responses WebSocket (proxied; raw bidirectional passthrough)",
      responses: { 101: { description: "Switching Protocols (WebSocket)" }, ...errors(401, 426) },
    }),
    async (c, next): Promise<Response> => {
      // A non-Upgrade GET can't become a socket — answer 426 instead of leaking
      // the adapter's "no server in env" error. Auth already ran (proxyAuth).
      const reject = () => c.json({ success: false, error: { code: "UPGRADE_REQUIRED", message: "GET /v1/responses requires a WebSocket upgrade" } }, 426);
      if ((c.req.header("upgrade") ?? "").toLowerCase() !== "websocket")
        return reject();
      // On a real WS request the adapter upgrades and returns the 101 Response;
      // it only yields void if the upgrade itself fails (then surface 426).
      return (await wsUpgrade(c, next)) ?? reject();
    },
  );

  return router;
}

/**
 * Strip the `${BASE_PATH}/api/llm` mount prefix from the full request path,
 * yielding the upstream subpath (e.g. `/v1/messages`) + the query string. The
 * handler forwards `${upstreamOrigin}${pathname}${search}`.
 */
export function stripLlmPrefix(requestUrl: string, basePath: string): { pathname: string; search: string } {
  const url = new URL(requestUrl);
  const prefix = `${basePath}/api/llm`;
  const pathname = url.pathname.startsWith(prefix) ? url.pathname.slice(prefix.length) : url.pathname;
  return { pathname: pathname || "/", search: url.search };
}
