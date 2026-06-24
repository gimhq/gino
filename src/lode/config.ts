import { configPath, readConfig } from "./sdk";

// The lode SDK locates and reads the operator's lode.toml (configPath() /
// readConfig(), both read-only) but does not parse it. We parse the raw text
// here and surface the non-secret update/trust/runtime config for the admin UI.
// Secret-bearing tables ([env], [http].headers) are never read.

export type LodeConfigStatus = "not_configured" | "unreadable" | "malformed" | "available";

export interface LodeConfig {
  readonly status: LodeConfigStatus;
  readonly app?: string;
  readonly sourceType?: "github" | "manifest";
  readonly source?: string;
  readonly asset?: string;
  readonly channel?: string;
  readonly policy?: "off" | "check" | "auto";
  readonly checkInterval?: number;
  readonly keepVersions?: number;
  readonly pin?: string;
  readonly requireSignature?: "off" | "auto" | "enforce";
  readonly runtime?: string;
  readonly runtimeVersion?: string;
}

function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function str(value: unknown, max = 256): string | undefined {
  if (typeof value !== "string")
    return undefined;
  const trimmed = value.trim();
  return trimmed && trimmed.length <= max ? trimmed : undefined;
}

function num(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asset(value: unknown): string | undefined {
  const a = str(value);
  return a && !a.includes("/") && !a.includes("\\") ? a : undefined;
}

function policy(value: unknown): LodeConfig["policy"] {
  const p = str(value);
  return p === "off" || p === "check" || p === "auto" ? p : undefined;
}

function signature(value: unknown): LodeConfig["requireSignature"] {
  const s = str(value);
  return s === "off" || s === "auto" || s === "enforce" ? s : undefined;
}

function githubSlug(value: unknown): string | undefined {
  const s = str(value);
  return s && /^[\w.-]+\/[\w.-]+$/.test(s) ? s : undefined;
}

// Show only the manifest host — the full URL may carry a path/token.
function manifestHost(value: unknown): string | undefined {
  const s = str(value, 1024);
  if (!s)
    return undefined;
  try {
    return new URL(s).host || undefined;
  }
  catch {
    return undefined;
  }
}

/** Read the operator's lode.toml (via the SDK) and extract the non-secret config. */
export function readLodeConfig(): LodeConfig {
  if (!configPath())
    return { status: "not_configured" };
  const text = readConfig();
  if (text === undefined)
    return { status: "unreadable" };

  let parsed: Record<string, unknown> | null;
  try {
    parsed = record(Bun.TOML.parse(text));
  }
  catch {
    return { status: "malformed" };
  }
  if (!parsed)
    return { status: "available" };

  const update = record(parsed.update);
  const result: {
    status: LodeConfigStatus;
    app?: string;
    sourceType?: "github" | "manifest";
    source?: string;
    asset?: string;
    channel?: string;
    policy?: "off" | "check" | "auto";
    checkInterval?: number;
    keepVersions?: number;
    pin?: string;
    requireSignature?: "off" | "auto" | "enforce";
    runtime?: string;
    runtimeVersion?: string;
  } = { status: "available" };

  const app = str(record(parsed.global)?.app);
  if (app)
    result.app = app;

  if (update) {
    const github = githubSlug(update.github);
    if (github) {
      result.sourceType = "github";
      result.source = github;
    }
    else {
      const host = manifestHost(update.manifest);
      if (host) {
        result.sourceType = "manifest";
        result.source = host;
      }
    }
    const assetName = asset(update.asset);
    if (assetName)
      result.asset = assetName;
    const channel = str(update.channel);
    if (channel)
      result.channel = channel;
    const updatePolicy = policy(update.policy);
    if (updatePolicy)
      result.policy = updatePolicy;
    const checkInterval = num(update.check_interval);
    if (checkInterval !== undefined)
      result.checkInterval = checkInterval;
    const keepVersions = num(update.keep_versions);
    if (keepVersions !== undefined)
      result.keepVersions = keepVersions;
    const pin = str(update.pin);
    if (pin)
      result.pin = pin;
  }

  const requireSignature = signature(record(parsed.trust)?.require_signature);
  if (requireSignature)
    result.requireSignature = requireSignature;

  const runtime = record(parsed.runtime);
  const runtimeName = str(runtime?.runtime);
  if (runtimeName)
    result.runtime = runtimeName;
  const runtimeVersion = str(runtime?.version);
  if (runtimeVersion)
    result.runtimeVersion = runtimeVersion;

  return result;
}
