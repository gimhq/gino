import { z } from "zod";

const RE_APP_NAME = /^[a-z][a-z0-9-]*$/;

/**
 * Single source of truth for env-derived host config. Keep this focused on
 * the *shape*; the env-var → field mapping and path resolution live in
 * `src/config.ts::loadConfig()`. Every host env var is `GINO_`-prefixed at
 * the boundary (see `config.ts`); the internal field names below are the
 * stable surface the rest of `src/**` reads.
 *
 * This is the scaffold's minimal surface (transport + logging + DB). Module
 * config (register/connect/channel tokens, proxy creds, launcher settings)
 * is added by workstreams C/D as those modules land — see
 * docs/host-v2-design.md.
 */
export const configSchema = z.object({
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
  // Bound port for the single OpenAPIHono server (env: GINO_PORT).
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  HOST: z.string().default("0.0.0.0"),
  // Application slug — lowercase letters, digits, dashes.
  APP_NAME: z.string().regex(RE_APP_NAME, "APP_NAME must match /^[a-z][a-z0-9-]*$/").default("gino"),
  // URL prefix the host is mounted under. Empty (default) → API at "/api".
  // "gino", "/gino", and "/gino/" all normalise to "/gino".
  BASE_PATH: z.string().default(""),
  DB_PATH: z.string().default("data/db/gino.db"),
  // ─── M8 — config/skills + attachment byte store (`GINO_STORE_DIR`) ───
  // Root of the content-addressed blob store, unified for config artifacts and
  // attachments (`store/<name>/<sha256[:12]>`). Defaulted under the runtime
  // `data/` dir (gitignored) so existing token-less config still constructs.
  STORE_DIR: z.string().default("data/store"),

  // Three-token auth secrets (M1 / OD12). REGISTER_TOKEN — the shared,
  // long-lived bootstrap secret for POST /agent/register. HTTP_CHANNEL_TOKEN —
  // the admin/user bearer for /admin/*. Both are REQUIRED in real deployments
  // (set them via GINO_REGISTER_TOKEN / GINO_HTTP_CHANNEL_TOKEN — see
  // .env.example). The default "" means "unconfigured": the auth middleware
  // treats an empty expected secret as fail-closed and rejects every request,
  // so a missing token never silently opens the surface. (Kept default-able,
  // not hard-required, so the scaffold's token-less config loads — e.g.
  // health probes — still construct.)
  REGISTER_TOKEN: z.string().default(""),
  HTTP_CHANNEL_TOKEN: z.string().default(""),

  LOG_LEVEL: z.enum(["debug", "info", "warn", "error"]).default("info"),
  LOG_FILE: z.string().default("data/logs/gino.log"),
  // When true, write logs to stdout instead of LOG_FILE — preferred for
  // container deployments that capture stdout/stderr at the runtime level.
  LOG_TO_STDOUT: z.enum(["true", "false"]).default("false").transform(v => v === "true"),
  CORS_ORIGIN: z.string().optional(),

  // When true, honour the rightmost `X-Forwarded-For` (and `X-Real-IP` as a
  // fallback) for client-IP resolution. Default false: forwarding headers are
  // ignored and the connection peer IP is used. Only enable behind a
  // sanitising proxy. `TRUSTED_PROXY_IPS` further restricts which hop
  // addresses may set those headers.
  TRUST_PROXY: z.enum(["true", "false"]).default("false").transform(v => v === "true"),
  // Comma-separated CIDR allow-list of proxy peer addresses. Empty (default)
  // means "any peer is trusted".
  TRUSTED_PROXY_IPS: z.string().default(""),

  // ─── M7 — credential proxy (`/llm/*`) ───
  // The host holds the real upstream LLM credentials; the engine reaches the
  // proxy via ANTHROPIC_BASE_URL/OPENAI_BASE_URL = `<hostUrl>/llm` and never
  // sees them. Every field is defaulted so existing token-less config still
  // constructs (the proxy is simply credential-less and forwards nothing
  // useful until configured). See `src/proxy/**`.
  //
  // Upstream origins. Operator-trusted; a literal private IP is rejected at
  // boot unless PROXY_ALLOW_PRIVATE_UPSTREAM=true (SSRF belt-and-suspenders).
  PROXY_ANTHROPIC_BASE_URL: z.string().default("https://api.anthropic.com"),
  PROXY_OPENAI_BASE_URL: z.string().default("https://api.openai.com"),
  // Upstream credentials. API key takes precedence over OAuth per provider.
  PROXY_ANTHROPIC_API_KEY: z.string().default(""),
  PROXY_ANTHROPIC_OAUTH_TOKEN: z.string().default(""),
  PROXY_OPENAI_API_KEY: z.string().default(""),
  PROXY_OPENAI_OAUTH_TOKEN: z.string().default(""),
  // Codex (OpenAI ChatGPT) OAuth refresh — when a refresh token is set the
  // proxy auto-refreshes the access token (the `chatgpt-account-id` header is
  // forwarded when present). Empty → no refresh; the OAuth token above is used
  // as-is.
  PROXY_CODEX_REFRESH_TOKEN: z.string().default(""),
  PROXY_CODEX_ACCOUNT_ID: z.string().default(""),
  // Static proxy credential (H5) — an engine may authenticate to `/llm/*` with
  // this fixed key instead of its per-agent connect token. Empty → only the
  // connect-token path (validated against `agents.connect_token_hash`) works.
  PROXY_STATIC_KEY: z.string().default(""),
  // model-map overrides: JSON object `{ "<alias>": { "provider": "anthropic"|
  // "openai", "model": "<upstream-model>" } }`, merged over the built-in map.
  // Parsed lazily by the proxy module (an invalid value is logged + ignored).
  PROXY_MODEL_MAP: z.string().default("{}"),
  // Allow a configured upstream that resolves to a literal private IP. Default
  // false (fail closed) — only enable for a trusted same-host mock.
  PROXY_ALLOW_PRIVATE_UPSTREAM: z.enum(["true", "false"]).default("false").transform(v => v === "true"),
});

export type Config = z.infer<typeof configSchema>;
