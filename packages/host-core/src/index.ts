// @gino/host-core — the public barrel (docs/host-sdk-design.md §3.1, §5 step 8).
//
// An explicit allow-list: `createHost` + its option/namespace types, the
// events/wire/ports surfaces an adapter needs, the launcher seam, the memory
// storage, the `AppError` family, the protocol frame catalog and the protocol
// version constants. Everything else is reachable through
// `@gino/host-core/domain` (semi-internal, used by the sibling packages).

export type * from "./host/ports/storage";
export { createDockerLauncher, DEFAULT_DOCKER_OP_TIMEOUT_MS, DOCKER_LAUNCHER_KIND, DockerOpTimeoutError } from "@gino/host-core/domain";
export type { DockerLauncherOptions, SpawnFn, SpawnOptions, SpawnResult } from "@gino/host-core/domain";
export { manualLauncher } from "@gino/host-core/domain";
export type { InspectState, Launcher, LauncherCapabilities, LaunchRequest, LaunchResult } from "@gino/host-core/domain";
export { createHostContext } from "@gino/host-core/domain";
export type { CreateHostContextOptions, HostContext } from "@gino/host-core/domain";
export { createHostEvents } from "@gino/host-core/domain";
export type { HostEventListener, HostEventMap, HostEventName, HostEvents } from "@gino/host-core/domain";
export { createHost } from "@gino/host-core/domain";
export type { AgentsApi, ConfigApi, CreateHostOptions, CronsApi, Host, TasksApi, TranscriptApi } from "@gino/host-core/domain";
export { noopMetrics } from "@gino/host-core/domain";
export type { Metrics } from "@gino/host-core/domain";
export { DEFAULT_HOST_OPTIONS, resolveHostOptions } from "@gino/host-core/domain";
export type { HostOptions, HostOptionsInput } from "@gino/host-core/domain";
export type { AgentTransport, WsLike } from "@gino/host-core/domain";
export type { AttachmentIngestInput, AttachmentsWire, HttpResult, Wire, WireContext } from "@gino/host-core/domain";
export { noopLogger } from "@gino/host-core/domain";
export type { LogFn, Logger } from "@gino/host-core/domain";
export { AckFrame, AgentFrame, AnyFrame, AttachmentDescriptor, CloseFrame, ConfigEntry, ConfigUpdateData, ConfigUpdateFrame, CronDeleteFrame, CronStateData, CronStateFrame, CronUpsertData, CronUpsertFrame, Envelope, HeartbeatData, HeartbeatFrame, HostFrame, InterruptFrame, MessageData, MessageFrame, MsgAckFrame, RECOGNIZED_MESSAGE_KINDS, TaskDeleteFrame, TaskUpsertData, TaskUpsertFrame, ThreadMessageData, ThreadMessageFrame, ThreadResultData, ThreadResultFrame } from "@gino/host-core/domain";
export { HEARTBEAT_INTERVAL_MS, isSupportedProtocol, PROTOCOL_VERSION } from "@gino/host-core/domain";
export { PayloadTooLargeError } from "@gino/host-core/domain";
export { AppError, ForbiddenError, NotFoundError, UnauthorizedError, ValidationError } from "@gino/host-core/domain";
export { memoryStorage, memoryStorageForTests } from "@gino/host-core/domain";
export type { MemoryStorageForTests } from "@gino/host-core/domain";
