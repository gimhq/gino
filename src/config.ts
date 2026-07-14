/** Stable surface for the rest of the host: `loadConfig` returns a fully-resolved `Config`. */
import type { Config } from "./config/schema";
import { resolve } from "node:path";
import process from "node:process";
import { configSchema } from "./config/schema";
import { DATA_DIR } from "./dirs";

export type { Config } from "./config/schema";

const RE_SLASH_TRIM = /^\/+|\/+$/g;

// Relative data paths (DB / store / logs) resolve against the persistent
// DATA_DIR (DATA_DIR > LODE_DIR > ROOT_DIR — see src/dirs.ts), so under the lode
// loader they land beside lode's durable state (`$LODE_DIR/data/**`), not the
// swappable version dir. An absolute path is used as-is.
function resolvePath(p: string): string {
  return p.startsWith("/") ? p : resolve(DATA_DIR, p);
}

/**
 * Map the `GINO_`-prefixed environment onto the internal config shape. The
 * host owns every value here (it is the sole config source — see
 * docs/host-v2-design.md); the agent never reads process env directly.
 */
function readEnv(env: Record<string, string | undefined>): Record<string, string | undefined> {
  return {
    NODE_ENV: env.NODE_ENV,
    PORT: env.GINO_PORT,
    HOST: env.GINO_HOST,
    APP_NAME: env.GINO_APP_NAME,
    BASE_PATH: env.GINO_BASE_PATH,
    DB_PATH: env.GINO_DB_PATH,
    STORE_DIR: env.GINO_STORE_DIR,
    REGISTER_TOKEN: env.GINO_REGISTER_TOKEN,
    HTTP_CHANNEL_TOKEN: env.GINO_HTTP_CHANNEL_TOKEN,
    HTTP_ADMIN_TOKEN: env.GINO_HTTP_ADMIN_TOKEN,
    REGISTER_ENABLED: env.GINO_REGISTER_ENABLED,
    MAX_UPLOAD_BYTES: env.GINO_MAX_UPLOAD_BYTES,
    LOG_LEVEL: env.GINO_LOG_LEVEL,
    LOG_FILE: env.GINO_LOG_FILE,
    LOG_TO_STDOUT: env.GINO_LOG_TO_STDOUT,
    CORS_ORIGIN: env.GINO_CORS_ORIGIN,
    TRUST_PROXY: env.GINO_TRUST_PROXY,
    TRUSTED_PROXY_IPS: env.GINO_TRUSTED_PROXY_IPS,
    // Edge rate limits (register / /llm/* / /messages/*).
    RATE_LIMIT_REGISTER_MAX: env.GINO_RATE_LIMIT_REGISTER_MAX,
    RATE_LIMIT_REGISTER_WINDOW_MS: env.GINO_RATE_LIMIT_REGISTER_WINDOW_MS,
    RATE_LIMIT_LLM_MAX: env.GINO_RATE_LIMIT_LLM_MAX,
    RATE_LIMIT_LLM_WINDOW_MS: env.GINO_RATE_LIMIT_LLM_WINDOW_MS,
    RATE_LIMIT_MESSAGES_MAX: env.GINO_RATE_LIMIT_MESSAGES_MAX,
    RATE_LIMIT_MESSAGES_WINDOW_MS: env.GINO_RATE_LIMIT_MESSAGES_WINDOW_MS,
    // M6 — outbound webhook egress.
    WEBHOOK_TIMEOUT_MS: env.GINO_WEBHOOK_TIMEOUT_MS,
    WEBHOOK_MAX_ATTEMPTS: env.GINO_WEBHOOK_MAX_ATTEMPTS,
    WEBHOOK_RETRY_INTERVAL_MS: env.GINO_WEBHOOK_RETRY_INTERVAL_MS,
    // M7 — credential proxy (`/llm/*`).
    PROXY_ANTHROPIC_BASE_URL: env.GINO_PROXY_ANTHROPIC_BASE_URL,
    PROXY_OPENAI_BASE_URL: env.GINO_PROXY_OPENAI_BASE_URL,
    PROXY_ANTHROPIC_API_KEY: env.GINO_PROXY_ANTHROPIC_API_KEY,
    PROXY_ANTHROPIC_OAUTH_TOKEN: env.GINO_PROXY_ANTHROPIC_OAUTH_TOKEN,
    PROXY_OPENAI_API_KEY: env.GINO_PROXY_OPENAI_API_KEY,
    PROXY_OPENAI_OAUTH_TOKEN: env.GINO_PROXY_OPENAI_OAUTH_TOKEN,
    PROXY_CODEX_REFRESH_TOKEN: env.GINO_PROXY_CODEX_REFRESH_TOKEN,
    PROXY_CODEX_ACCOUNT_ID: env.GINO_PROXY_CODEX_ACCOUNT_ID,
    PROXY_STATIC_KEY: env.GINO_PROXY_STATIC_KEY,
    PROXY_MODEL_MAP: env.GINO_PROXY_MODEL_MAP,
    PROXY_ALLOW_PRIVATE_UPSTREAM: env.GINO_PROXY_ALLOW_PRIVATE_UPSTREAM,
  };
}

/**
 * Parse + validate config, throwing on the first failure. Use from tests so
 * failures surface as exceptions. Pass an explicit `env` to test without
 * mutating the process environment.
 */
export function loadConfigStrict(env: Record<string, string | undefined> = Bun.env): Config {
  const result = configSchema.safeParse(readEnv(env));
  if (!result.success) {
    const fieldErrors = result.error.flatten().fieldErrors;
    throw new Error(`Invalid configuration: ${JSON.stringify(fieldErrors)}`);
  }
  const data = result.data;

  const trimmedBase = data.BASE_PATH.replace(RE_SLASH_TRIM, "");
  return {
    ...data,
    BASE_PATH: trimmedBase ? `/${trimmedBase}` : "",
    DB_PATH: resolvePath(data.DB_PATH),
    STORE_DIR: resolvePath(data.STORE_DIR),
    LOG_FILE: resolvePath(data.LOG_FILE),
  };
}

/**
 * Boot entrypoint — wraps `loadConfigStrict` with the production
 * "print + exit" failure surface.
 */
export function loadConfig(): Config {
  try {
    return loadConfigStrict();
  }
  catch (err) {
    console.error(`[config] ${err instanceof Error ? err.message : String(err)}`);
    console.error("Fix: set the GINO_* variables in your environment (or .env), then re-run.");
    process.exit(1);
  }
}
