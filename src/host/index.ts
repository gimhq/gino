// HOST — barrel for the per-instance host context.
export { createHostContext } from "./context";
export type { CreateHostContextOptions, HostContext } from "./context";
export { createHostEvents } from "./events";
export type { HostEventListener, HostEventMap, HostEventName, HostEvents } from "./events";
export { noopMetrics } from "./metrics";
export type { Metrics } from "./metrics";
export { DEFAULT_HOST_OPTIONS, resolveHostOptions } from "./options";
export type { HostOptions, HostOptionsInput } from "./options";

export const MODULE = "host" as const;
