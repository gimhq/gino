// @gino/host-core — the public barrel (docs/host-sdk-design.md §3.1, §5 step 8).
//
// An explicit allow-list: `createHost` + its option/namespace types, the
// events/wire/ports surfaces an adapter needs, the launcher seam, the memory
// storage, the `AppError` family, the protocol frame catalog and the protocol
// version constants. Everything else is reachable through
// `@gino/host-core/domain` (semi-internal, used by the sibling packages).

export { createDockerLauncher, DEFAULT_DOCKER_OP_TIMEOUT_MS, DOCKER_LAUNCHER_KIND, DockerOpTimeoutError } from "./agents/launchers/docker";
export type { DockerLauncherOptions, SpawnFn, SpawnOptions, SpawnResult } from "./agents/launchers/docker";
export { manualLauncher } from "./agents/launchers/manual";
export type { InspectState, Launcher, LauncherCapabilities, LaunchRequest, LaunchResult } from "./agents/launchers/types";
export { createHostContext } from "./host/context";
export type { CreateHostContextOptions, HostContext } from "./host/context";
export { createHostEvents } from "./host/events";
export type { HostEventListener, HostEventMap, HostEventName, HostEvents } from "./host/events";
export { createHost } from "./host/host";
export type { AgentsApi, ConfigApi, CreateHostOptions, CronsApi, Host, TasksApi, TranscriptApi } from "./host/host";
export { noopMetrics } from "./host/metrics";
export type { Metrics } from "./host/metrics";
export { DEFAULT_HOST_OPTIONS, resolveHostOptions } from "./host/options";
export type { HostOptions, HostOptionsInput } from "./host/options";
export type * from "./host/ports/storage";
export type { AgentTransport, WsLike } from "./host/ports/transport";
export type { AttachmentIngestInput, AttachmentsWire, HttpResult, Wire, WireContext } from "./host/wire";
export { noopLogger } from "./logger";
export type { LogFn, Logger } from "./logger";
export * from "./protocol/frames";
export { HEARTBEAT_INTERVAL_MS, isSupportedProtocol, PROTOCOL_VERSION } from "./protocol/version";
export { PayloadTooLargeError } from "./shared/lib/body-limit";
export { AppError, ForbiddenError, NotFoundError, UnauthorizedError, ValidationError } from "./shared/lib/errors";
export { memoryStorage, memoryStorageForTests } from "./store/memory";
export type { MemoryStorageForTests } from "./store/memory";
