// HOST — typed per-instance options (docs/host-sdk-design.md §3.3).
//
// The domain and edge read these from the context instead of the env-backed
// `Config`; the standalone app maps `Config` → `HostOptions` in exactly one
// place (`hostOptionsFromConfig` in src/app.ts). The defaults here mirror the
// env schema defaults (src/config/schema.ts) so an embedding host that passes
// nothing behaves like an unconfigured standalone server.
import { OFFLINE_GRACE_MS } from "../agents/liveness";
import { HEARTBEAT_INTERVAL_MS } from "../protocol/version";

export interface HostOptions {
  /** Root of the content-addressed blob store (config artifacts + attachments). */
  readonly storeDir: string;
  /** Max accepted request body for the byte-ingest endpoints (bytes). */
  readonly maxUploadBytes: number;
  /** Shared register token; "" = unconfigured → POST /agent/register fails closed (unchanged semantics). */
  readonly registerToken: string;
  readonly registrationEnabled: boolean;
  /** Advertised to agents on register/config; also the liveness sweep cadence. */
  readonly heartbeatIntervalMs: number;
  /** DP4 offline grace (silence before an `online` agent is flipped `offline`). */
  readonly offlineGraceMs: number;
  readonly configGc: { readonly enabled: boolean; readonly intervalMs: number; readonly maxAgeMs: number };
  readonly webhooks: { readonly timeoutMs: number; readonly maxAttempts: number; readonly retryIntervalMs: number };
}

export const DEFAULT_HOST_OPTIONS: HostOptions = {
  storeDir: "data/store",
  maxUploadBytes: 64 * 1024 * 1024,
  registerToken: "",
  registrationEnabled: true,
  heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
  offlineGraceMs: OFFLINE_GRACE_MS,
  configGc: { enabled: false, intervalMs: 60 * 60 * 1000, maxAgeMs: 7 * 24 * 60 * 60 * 1000 },
  webhooks: { timeoutMs: 10_000, maxAttempts: 5, retryIntervalMs: 15_000 },
};

export type HostOptionsInput = Partial<Omit<HostOptions, "configGc" | "webhooks">> & {
  configGc?: Partial<HostOptions["configGc"]>;
  webhooks?: Partial<HostOptions["webhooks"]>;
};

/** Shallow-merge `input` over the defaults; the two nested groups merge key-by-key. */
export function resolveHostOptions(input: HostOptionsInput = {}): HostOptions {
  return {
    ...DEFAULT_HOST_OPTIONS,
    ...input,
    configGc: { ...DEFAULT_HOST_OPTIONS.configGc, ...input.configGc },
    webhooks: { ...DEFAULT_HOST_OPTIONS.webhooks, ...input.webhooks },
  };
}
