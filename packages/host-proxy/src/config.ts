import type { Logger } from "@gino/host-core";
import type { ModelMap, ProxyProvider } from "./model-map";
// M7 — proxy config. Projects the host env config (`ProxyEnv`, the PROXY_*
// slice of the app `Config`, satisfied structurally) onto the
// structured shape the proxy module consumes: per-provider upstream origin +
// credentials, the static proxy key (H5), the codex OAuth-refresh inputs, and
// the parsed model-map overrides. The model-map env value is parsed + validated
// here (invalid JSON is logged and ignored — the proxy stays up with built-in
// mappings only).
import { z } from "zod";
import { isPrivateUpstreamHost } from "./provider";

/**
 * The PROXY_* fields the proxy reads from the host config. The app's `Config`
 * (config/schema.ts) satisfies this structurally; embedders may pass a literal.
 */
export interface ProxyEnv {
  readonly PROXY_ALLOW_PRIVATE_UPSTREAM: boolean;
  readonly PROXY_ANTHROPIC_API_KEY: string;
  readonly PROXY_ANTHROPIC_BASE_URL: string;
  readonly PROXY_ANTHROPIC_OAUTH_TOKEN: string;
  readonly PROXY_CODEX_ACCOUNT_ID: string;
  readonly PROXY_CODEX_REFRESH_TOKEN: string;
  readonly PROXY_MODEL_MAP: string;
  readonly PROXY_OPENAI_API_KEY: string;
  readonly PROXY_OPENAI_BASE_URL: string;
  readonly PROXY_OPENAI_OAUTH_TOKEN: string;
  readonly PROXY_STATIC_KEY: string;
}

export interface ProviderUpstream {
  /** Upstream origin (scheme+host[:port]); requests forward to `${origin}${path}`. */
  readonly baseUrl: URL;
  /** Static API key (wins over OAuth when both are set). */
  readonly apiKey?: string | undefined;
  /** OAuth bearer (used when no apiKey). For codex, may be refreshed at runtime. */
  readonly oauthToken?: string | undefined;
}

export interface ProxyConfig {
  readonly anthropic: ProviderUpstream;
  readonly openai: ProviderUpstream;
  /** Static proxy credential (H5) — an alternative to the per-agent connect token. */
  readonly staticKey?: string | undefined;
  /** Codex (ChatGPT) OAuth refresh inputs. */
  readonly codex: {
    readonly refreshToken?: string | undefined;
    readonly accountId?: string | undefined;
  };
  /** Config model-map overrides, merged over the built-ins by `resolveModel`. */
  readonly modelMap: ModelMap;
}

const modelMapEntrySchema = z.object({
  provider: z.enum(["anthropic", "openai"]),
  model: z.string().min(1),
});
const modelMapSchema = z.record(z.string(), modelMapEntrySchema);

/**
 * Parse `GINO_PROXY_MODEL_MAP` (a JSON object string). Invalid JSON or a shape
 * mismatch is logged and treated as "no overrides" — the proxy must not fail to
 * construct over a bad map.
 */
export function parseModelMap(raw: string, logger?: Logger): ModelMap {
  const trimmed = raw.trim();
  if (!trimmed || trimmed === "{}")
    return {};
  try {
    const parsed: unknown = JSON.parse(trimmed);
    const result = modelMapSchema.safeParse(parsed);
    if (!result.success) {
      logger?.warn({ issues: result.error.flatten() }, "proxy: GINO_PROXY_MODEL_MAP shape invalid — ignoring");
      return {};
    }
    // Narrow zod's `string` provider back to the ProxyProvider union.
    const out: Record<string, { provider: ProxyProvider; model: string }> = {};
    for (const [alias, entry] of Object.entries(result.data))
      out[alias] = { provider: entry.provider, model: entry.model };
    return out;
  }
  catch (err) {
    logger?.warn({ err }, "proxy: GINO_PROXY_MODEL_MAP is not valid JSON — ignoring");
    return {};
  }
}

function resolveUpstream(rawUrl: string, label: string, allowPrivate: boolean): URL {
  let url: URL;
  try {
    url = new URL(rawUrl);
  }
  catch {
    throw new Error(`proxy: ${label} is not a valid URL: ${rawUrl}`);
  }
  if (!allowPrivate && isPrivateUpstreamHost(url.hostname))
    throw new Error(`proxy: ${label} resolves to a private IP (${url.hostname}); set GINO_PROXY_ALLOW_PRIVATE_UPSTREAM=true to allow`);
  return url;
}

/**
 * Build the structured proxy config from the host env config (`ProxyEnv`).
 *
 * Upstream credentials (provider API keys, codex OAuth refresh/access tokens)
 * are held as plaintext in this env-derived config and in the in-memory
 * `CredentialStore` — accepted risk by design: the host process
 * is the trust boundary, secrecy relies on host env/process isolation, and the
 * proxy modules never write these values to logs or disk.
 */
export function buildProxyConfig(config: ProxyEnv, logger?: Logger): ProxyConfig {
  const allowPrivate = config.PROXY_ALLOW_PRIVATE_UPSTREAM;
  return {
    anthropic: {
      baseUrl: resolveUpstream(config.PROXY_ANTHROPIC_BASE_URL, "GINO_PROXY_ANTHROPIC_BASE_URL", allowPrivate),
      apiKey: config.PROXY_ANTHROPIC_API_KEY || undefined,
      oauthToken: config.PROXY_ANTHROPIC_OAUTH_TOKEN || undefined,
    },
    openai: {
      baseUrl: resolveUpstream(config.PROXY_OPENAI_BASE_URL, "GINO_PROXY_OPENAI_BASE_URL", allowPrivate),
      apiKey: config.PROXY_OPENAI_API_KEY || undefined,
      oauthToken: config.PROXY_OPENAI_OAUTH_TOKEN || undefined,
    },
    staticKey: config.PROXY_STATIC_KEY || undefined,
    codex: {
      refreshToken: config.PROXY_CODEX_REFRESH_TOKEN || undefined,
      accountId: config.PROXY_CODEX_ACCOUNT_ID || undefined,
    },
    modelMap: parseModelMap(config.PROXY_MODEL_MAP, logger),
  };
}
