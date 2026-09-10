// M7 — credential resolution + OAuth refresh. The host holds the real upstream
// credentials; this store hands the right one to the forward path and refreshes
// the Codex (ChatGPT) OAuth access token on demand from a configured refresh
// token. `fetchImpl` is injectable so the refresh flow is unit-testable with no
// real network. Ported from the v1 credential-manager (backup/pre-hostv2
// src/auth/credential-manager.ts), reshaped for v2: credentials come from host
// config (host = sole config source), refresh is lazy/on-demand (no background
// timer), and nothing is read from `~/.claude` / `~/.codex`.
import type { Logger } from "@gino/host-core";
import type { ProviderUpstream, ProxyConfig } from "./config";

// Codex OAuth constants (from the v1 reference / CLIProxyAPI).
const CODEX_TOKEN_URL = "https://auth.openai.com/oauth/token";
const CODEX_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann";
const TOKEN_EXPIRY_BUFFER_MS = 5 * 60 * 1000; // refresh 5 min before expiry

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface ResolvedCredential {
  /** Static API key (when configured). Mutually exclusive with `oauthToken`. */
  readonly apiKey?: string | undefined;
  /** OAuth bearer (when no apiKey). For codex, the refreshed access token. */
  readonly oauthToken?: string | undefined;
  /** Codex account id, forwarded as `chatgpt-account-id` when present. */
  readonly accountId?: string | undefined;
}

export interface CredentialStoreOpts {
  readonly fetchImpl?: FetchLike;
  readonly logger?: Logger;
  /** Injectable clock for deterministic expiry tests (defaults to Date.now). */
  readonly now?: () => number;
}

export class CredentialStore {
  private readonly config: ProxyConfig;
  private readonly fetchImpl: FetchLike;
  private readonly logger: Logger | undefined;
  private readonly now: () => number;

  // Refreshed-codex state (in-memory; lost on restart, re-refreshed on demand).
  private codexAccessToken: string | undefined;
  private codexExpiry = 0;
  private codexRefreshToken: string | undefined;
  private inflight: Promise<string | undefined> | null = null;

  constructor(config: ProxyConfig, opts: CredentialStoreOpts = {}) {
    this.config = config;
    this.fetchImpl = opts.fetchImpl ?? ((input, init) => fetch(input, init));
    this.logger = opts.logger;
    this.now = opts.now ?? (() => Date.now());
    this.codexRefreshToken = config.codex.refreshToken;
  }

  /** Anthropic credential — static (apiKey > oauthToken from config). Sync. */
  getAnthropic(): ResolvedCredential {
    return staticCredential(this.config.anthropic);
  }

  /**
   * OpenAI credential. Prefers a static apiKey; otherwise resolves the codex
   * OAuth access token, refreshing on demand when a refresh token is configured
   * and the cached access token is missing/expired.
   */
  async getOpenAI(): Promise<ResolvedCredential> {
    const { openai, codex } = this.config;
    if (openai.apiKey)
      return { apiKey: openai.apiKey };

    const token = await this.resolveCodexToken();
    return {
      oauthToken: token ?? openai.oauthToken,
      accountId: codex.accountId,
    };
  }

  /** Force a codex refresh (returns the new access token, or undefined). */
  async refreshCodex(): Promise<string | undefined> {
    return this.doRefresh();
  }

  private async resolveCodexToken(): Promise<string | undefined> {
    // No refresh capability → fall back to the configured static token (if any).
    if (!this.codexRefreshToken)
      return this.codexAccessToken ?? this.config.openai.oauthToken;

    // Cached + still fresh.
    if (this.codexAccessToken && this.now() < this.codexExpiry - TOKEN_EXPIRY_BUFFER_MS)
      return this.codexAccessToken;

    // Coalesce concurrent refreshes onto one in-flight request.
    if (!this.inflight)
      this.inflight = this.doRefresh().finally(() => { this.inflight = null; });
    return this.inflight;
  }

  private async doRefresh(): Promise<string | undefined> {
    if (!this.codexRefreshToken)
      return undefined;
    try {
      const body = new URLSearchParams({
        client_id: CODEX_CLIENT_ID,
        grant_type: "refresh_token",
        refresh_token: this.codexRefreshToken,
        scope: "openid profile email",
      });
      const resp = await this.fetchImpl(CODEX_TOKEN_URL, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: body.toString(),
      });
      if (!resp.ok) {
        const text = await resp.text().catch(() => "");
        this.logger?.error({ status: resp.status, error: oauthErrorCode(text) }, "proxy: codex token refresh failed");
        return this.codexAccessToken; // keep any prior token
      }
      const data = await resp.json() as {
        access_token?: string;
        refresh_token?: string;
        expires_in?: number;
      };
      if (!data.access_token) {
        this.logger?.error("proxy: codex token refresh returned no access_token");
        return this.codexAccessToken;
      }
      this.codexAccessToken = data.access_token;
      if (data.refresh_token)
        this.codexRefreshToken = data.refresh_token; // rotated
      this.codexExpiry = this.now() + (data.expires_in ?? 3600) * 1000;
      this.logger?.info({ expiresIn: data.expires_in ?? 3600 }, "proxy: codex token refreshed");
      return this.codexAccessToken;
    }
    catch (err) {
      this.logger?.error({ err }, "proxy: codex token refresh error");
      return this.codexAccessToken;
    }
  }
}

/**
 * Reduce a token-endpoint error body to its standard OAuth `error` code for
 * logging. The raw body is never logged (HOST-M7-06) — it is secret-adjacent
 * and could echo request material.
 */
function oauthErrorCode(body: string): string {
  try {
    const parsed = JSON.parse(body) as { error?: unknown };
    if (typeof parsed.error === "string")
      return parsed.error.slice(0, 64);
  }
  catch { /* non-JSON body — fall through */ }
  return `<unrecognized body, ${body.length} chars>`;
}

function staticCredential(upstream: ProviderUpstream): ResolvedCredential {
  if (upstream.apiKey)
    return { apiKey: upstream.apiKey };
  if (upstream.oauthToken)
    return { oauthToken: upstream.oauthToken };
  return {};
}
