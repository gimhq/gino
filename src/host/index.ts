// HOST — barrel for the per-instance host context.
export { createHostContext } from "./context";
export type { CreateHostContextOptions, HostContext } from "./context";
export { noopMetrics } from "./metrics";
export type { Metrics } from "./metrics";

export const MODULE = "host" as const;
