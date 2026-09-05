// HOST — barrel for the per-instance host context.
export { createHostContext } from "./context";
export type { CreateHostContextOptions, HostContext } from "./context";
export { createHostEvents } from "./events";
export type { HostEventListener, HostEventMap, HostEventName, HostEvents } from "./events";
export { createHost } from "./host";
export type { AgentsApi, ConfigApi, CreateHostOptions, CronsApi, Host, TasksApi, TranscriptApi } from "./host";
export { noopMetrics } from "./metrics";
export type { Metrics } from "./metrics";
export { DEFAULT_HOST_OPTIONS, resolveHostOptions } from "./options";
export type { HostOptions, HostOptionsInput } from "./options";
export type { AgentTransport, WsLike } from "./ports/transport";
export { createWire } from "./wire";
export type { AttachmentIngestInput, AttachmentsWire, HttpResult, Wire, WireContext } from "./wire";

export const MODULE = "host" as const;
