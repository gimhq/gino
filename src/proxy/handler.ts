import type { WsFactory } from "./bridge";
import type { ProxyConfig } from "./config";
import type { CredentialStore, FetchLike } from "./credentials";
// M7 — the HTTP forward path. Given an upstream subpath (already `/llm`-stripped
// by the router) and the raw request, it: detects the provider, resolves the
// model (alias → upstream), bridges Claude→OpenAI when a `/v1/messages` request
// targets an OpenAI model, otherwise injects the real upstream credential,
// scrubs hop/internal headers, rewrites the aliased model, and forwards via an
// injectable `fetch`. Tests drive it with a mock `fetchImpl` + `wsFactory` — no
// real network.
import type { Logger } from "@/shared/lib/logger";
import { bridgeClaudeToOpenAI } from "./bridge";
import { resolveModel } from "./model-map";
import {
  ANTHROPIC_KEY_PREFIX,
  buildResponseHeaders,
  buildUpstreamHeaders,
  detectProvider,
  isAllowedUpstreamPath,
  isAnthropicTempKey,
  PROXY_KEY_PREFIX,
} from "./provider";

const ANTHROPIC_OAUTH_BETA = "oauth-2025-04-20";
const MODEL_HEADER = "x-gino-model";

export interface ProxyHandlerDeps {
  readonly config: ProxyConfig;
  readonly credentials: CredentialStore;
  readonly fetchImpl?: FetchLike;
  readonly wsFactory?: WsFactory;
  readonly logger?: Logger;
  /** Override the bridge upstream WS URL (tests). */
  readonly bridgeWsUrl?: string;
}

export interface ProxyHandler {
  /** Forward one request. `pathname`/`search` are already `/llm`-stripped. */
  handle: (req: Request, pathname: string, search: string) => Promise<Response>;
}

/** Build the credential proxy forward handler. */
export function createProxyHandler(deps: ProxyHandlerDeps): ProxyHandler {
  const { config, credentials, logger } = deps;
  const fetchImpl: FetchLike = deps.fetchImpl ?? ((input, init) => fetch(input, init));

  async function handle(req: Request, pathname: string, search: string): Promise<Response> {
    if (!isAllowedUpstreamPath(pathname))
      return jsonError(404, "NOT_FOUND", "Not Found");

    const provider = detectProvider(req.headers, pathname);
    const bodyBytes = await req.arrayBuffer();
    const hasBody = bodyBytes.byteLength > 0;

    // Resolve the request's target model (launcher header wins over the body;
    // an empty header is treated as absent, not as a blank model).
    const headerModel = req.headers.get(MODEL_HEADER) || undefined;
    const bodyModel = hasBody ? readModel(bodyBytes) : undefined;
    const targetModel = headerModel ?? bodyModel;
    const mapping = targetModel ? resolveModel(targetModel, config.modelMap) : undefined;

    // Claude Messages → OpenAI Responses bridge. A caller presenting a real
    // Anthropic temp key is only pass-through-authenticated (Anthropic itself
    // validates the key) — it must never reach the bridge, which injects the
    // host's OpenAI credentials (HOST-M7-01). Such a request stays on the
    // direct anthropic forward below, key passed through as-is.
    if (pathname === "/v1/messages" && req.method === "POST" && hasBody && mapping?.provider === "openai"
      && !isAnthropicTempKey(req.headers)) {
      logger?.debug({ targetModel, upstreamModel: mapping.model }, "proxy: bridging /v1/messages → OpenAI");
      const bridgeDeps: Parameters<typeof bridgeClaudeToOpenAI>[2] = { store: credentials };
      if (deps.wsFactory)
        Object.assign(bridgeDeps, { wsFactory: deps.wsFactory });
      if (logger)
        Object.assign(bridgeDeps, { logger });
      if (deps.bridgeWsUrl)
        Object.assign(bridgeDeps, { wsUrl: deps.bridgeWsUrl });
      return bridgeClaudeToOpenAI(new TextDecoder().decode(bodyBytes), mapping.model, bridgeDeps);
    }

    // Direct forward.
    const headers = buildUpstreamHeaders(req.headers);
    if (provider === "anthropic")
      injectAnthropic(headers, req.headers, pathname, credentials);
    else
      await injectOpenAI(headers, credentials);

    // Rewrite the aliased model in the body when same-provider.
    let outBody: ArrayBuffer | string | undefined = hasBody ? bodyBytes : undefined;
    if (hasBody && mapping && mapping.provider === provider && bodyModel) {
      const rewritten = rewriteModel(bodyBytes, mapping.model);
      if (rewritten !== undefined)
        outBody = rewritten;
    }

    const upstream = config[provider].baseUrl;
    const url = `${upstream.origin}${pathname}${search}`;

    try {
      const init: RequestInit = { method: req.method, headers };
      if (outBody !== undefined)
        init.body = outBody;
      const upstreamRes = await fetchImpl(url, init);
      logger?.debug({ status: upstreamRes.status, pathname, provider }, "proxy: forwarded");
      return new Response(upstreamRes.body, {
        status: upstreamRes.status,
        headers: buildResponseHeaders(upstreamRes.headers),
      });
    }
    catch (err) {
      logger?.error({ err, pathname, provider }, "proxy: upstream error");
      return jsonError(502, "BAD_GATEWAY", "Upstream request failed");
    }
  }

  return { handle };
}

// ─── Credential injection ───

function injectAnthropic(
  headers: Headers,
  clientHeaders: Headers,
  pathname: string,
  credentials: CredentialStore,
): void {
  const cred = credentials.getAnthropic();
  const clientApiKey = clientHeaders.get("x-api-key") ?? "";

  // OAuth exchange (`/api/oauth/*`): inject the real OAuth token (or api key).
  if (pathname.startsWith("/api/oauth/")) {
    if (cred.oauthToken)
      headers.set("authorization", `Bearer ${cred.oauthToken}`);
    else if (cred.apiKey)
      headers.set("x-api-key", cred.apiKey);
    return;
  }

  // A real Anthropic temp key (from a prior OAuth exchange) passes through.
  if (clientApiKey.startsWith(ANTHROPIC_KEY_PREFIX) && !clientApiKey.startsWith(PROXY_KEY_PREFIX)) {
    headers.set("x-api-key", clientApiKey);
    return;
  }

  // Placeholder / no client key → inject the configured credential.
  if (cred.apiKey) {
    headers.set("x-api-key", cred.apiKey);
  }
  else if (cred.oauthToken) {
    headers.set("authorization", `Bearer ${cred.oauthToken}`);
    addAnthropicBeta(headers, ANTHROPIC_OAUTH_BETA);
  }
}

async function injectOpenAI(headers: Headers, credentials: CredentialStore): Promise<void> {
  const cred = await credentials.getOpenAI();
  const token = cred.apiKey ?? cred.oauthToken;
  if (token)
    headers.set("authorization", `Bearer ${token}`);
  if (cred.accountId)
    headers.set("chatgpt-account-id", cred.accountId);
}

function addAnthropicBeta(headers: Headers, flag: string): void {
  const existing = headers.get("anthropic-beta") ?? "";
  if (existing.includes(flag))
    return;
  headers.set("anthropic-beta", existing ? `${existing},${flag}` : flag);
}

// ─── Body helpers ───

/** Read the `model` field from a JSON body without throwing on non-JSON. */
function readModel(bytes: ArrayBuffer): string | undefined {
  try {
    const obj = JSON.parse(new TextDecoder().decode(bytes)) as { model?: unknown };
    return typeof obj.model === "string" ? obj.model : undefined;
  }
  catch {
    return undefined;
  }
}

/** Rewrite the `model` field to the upstream model; undefined if the body is non-JSON or has no model. */
function rewriteModel(bytes: ArrayBuffer, upstreamModel: string): string | undefined {
  try {
    const obj = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
    if (obj && typeof obj === "object" && "model" in obj) {
      obj.model = upstreamModel;
      return JSON.stringify(obj);
    }
  }
  catch { /* non-JSON — forward unchanged */ }
  return undefined;
}

function jsonError(status: number, code: string, message: string): Response {
  return new Response(
    JSON.stringify({ success: false, error: { code, message } }),
    { status, headers: { "content-type": "application/json" } },
  );
}
