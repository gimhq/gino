// @gino/host-core — the public barrel (docs/host-sdk-design.md §3.1, §5 step 8).
//
// FROZEN as of P3. This list is the package's API: an explicit allow-list, no
// wildcards, so the surface cannot grow by accident when a module gains an
// export. `tests/unit/api-surface.test.ts` snapshots it — adding or removing a
// name here fails that test until the snapshot is updated deliberately.
//
// What belongs here: `createHost` + the namespace interfaces and the plain-data
// types their methods take and return, the events/wire surfaces, the §8 ports
// (`Storage` + its repositories + row types, `AgentTransport`, `WsLike`), the
// launcher seam, `Logger` / `Metrics`, the protocol frame catalog + version
// constants, and the `AppError` family. Everything else — the 90-odd domain
// functions, the session/launcher registries, the module internals — stays on
// `@gino/host-core/domain` (semi-internal, for the sibling `@gino/host-*`
// packages). Every name is re-exported from that one entry so a Node consumer
// gets a single copy of each class no matter which entry it imports from.

// ─── createHost + the namespaces (§3.1) ───
export { createHost } from "@gino/host-core/domain";
export type { AgentsApi, ConfigApi, CreateHostOptions, CronsApi, Host, HostContext, TasksApi, TranscriptApi } from "@gino/host-core/domain";

// The plain-data inputs/outputs of those namespace methods. Without them a
// consumer's own `.d.ts` cannot name what `host.agents.enroll(…)` returns.
export type { AgentCapabilities, EnrollInput, EnrollResult } from "@gino/host-core/domain";
export type { ControlOp, ControlResult, ControlVia, LaunchAgentResult, LaunchInput, RemoveResult } from "@gino/host-core/domain";
export type { CreateTaskInput, DispatchResult, ListTasksFilter } from "@gino/host-core/domain";
export type { CronUpsertInput, CronUpsertResult, MessageInput, MessageResult, SendMessageResult } from "@gino/host-core/domain";
export type { EnqueuedOutbox } from "@gino/host-core/domain";
export type { EntryInput, IngestArtifactInput } from "@gino/host-core/domain";
// `host.transcript.get` — `limit` is OPTIONAL here (it defaults to and is
// clamped at `TRANSCRIPT_READ_LIMIT`). The port-side shape an adapter receives,
// with `limit` already resolved and therefore required, is `TranscriptReadQuery`.
export type { TranscriptReadOptions } from "@gino/host-core/domain";

// `Host` (via `HostContext`) exposes the two per-instance registries, so their
// classes have to be nameable for a consumer's own `.d.ts` to compile.
// Type-only: no constructor joins the runtime surface, and `sessions` is
// usable through the `AgentTransport` port it implements.
export type { LauncherRegistry, SessionRegistry } from "@gino/host-core/domain";

// ─── Per-instance options (§3.1) ───
export { DEFAULT_HOST_OPTIONS, resolveHostOptions } from "@gino/host-core/domain";
export type { HostOptions, HostOptionsInput } from "@gino/host-core/domain";

// ─── Events (§3.3) ───
export { createHostEvents } from "@gino/host-core/domain";
export type { HostEventListener, HostEventMap, HostEventName, HostEvents } from "@gino/host-core/domain";

// ─── The framework-free data plane (§3.2) ───
export type { AttachmentIngestInput, AttachmentsWire, HttpResult, Wire, WireContext } from "@gino/host-core/domain";

// ─── Ports (§8) — agent transport ───
export type { AgentTransport, WsLike } from "@gino/host-core/domain";

// ─── Ports (§8) — storage: the repositories an adapter implements … ───
export type {
  AgentRepo,
  AttachmentRepo,
  BlobStorePort,
  ChannelRepo,
  ConfigRepo,
  CronMirrorRepo,
  OutboxRepo,
  Storage,
  TaskRepo,
  ThreadRunRepo,
  TranscriptRepo,
} from "@gino/host-core/domain";
// … their argument/result shapes …
export type {
  BlobLeaf,
  CronPromptUpsert,
  CronStateUpsert,
  MarkAckedOptions,
  StatusGuard,
  StoredBlobRef,
  TaskListFilter,
  ThreadFirstSeen,
  TranscriptReadQuery,
  TranscriptStats,
} from "@gino/host-core/domain";
// … and the hand-written row types they store and return (no drizzle types
// reach the surface — `ports/rows.ts` mirrors the schema by hand on purpose).
export type {
  AgentCapabilitiesJson,
  AgentGrantRow,
  AgentRow,
  AttachmentRow,
  ChannelBindingRow,
  ChannelCursorRow,
  ChannelRow,
  ConfigArtifactRow,
  ConfigEntryInput,
  ConfigEntryRow,
  CronRow,
  MessageRow,
  NewAgentRow,
  NewAttachmentRow,
  NewChannelBindingRow,
  NewChannelRow,
  NewConfigArtifactRow,
  NewMessageRow,
  NewOutboxRow,
  NewPendingLaunchRow,
  NewTaskRow,
  NewThreadRunRow,
  OutboxRow,
  PendingLaunchRow,
  TaskRow,
  ThreadRunRow,
} from "@gino/host-core/domain";

// ─── The in-memory `Storage` (ships here; the conformance fixture with it) ───
export { memoryStorage, memoryStorageForTests } from "@gino/host-core/domain";
export type { MemoryStorageForTests } from "@gino/host-core/domain";

// ─── The launcher seam ───
export { manualLauncher } from "@gino/host-core/domain";
export type { InspectState, Launcher, LauncherCapabilities, LaunchRequest, LaunchResult } from "@gino/host-core/domain";
export { createDockerLauncher, DEFAULT_DOCKER_OP_TIMEOUT_MS, DOCKER_LAUNCHER_KIND, DockerOpTimeoutError } from "@gino/host-core/domain";
export type { DockerLauncherOptions, SpawnFn, SpawnOptions, SpawnResult } from "@gino/host-core/domain";

// ─── Logging + metrics sinks ───
export { noopLogger } from "@gino/host-core/domain";
export type { LogFn, Logger } from "@gino/host-core/domain";
export { noopMetrics } from "@gino/host-core/domain";
export type { Metrics } from "@gino/host-core/domain";

// ─── Protocol: the frame catalog (zod schema + inferred type per name) … ───
export { AckFrame, AgentFrame, AnyFrame, AttachmentDescriptor, CloseFrame, ConfigEntry, ConfigUpdateData, ConfigUpdateFrame, CronDeleteFrame, CronStateData, CronStateFrame, CronUpsertData, CronUpsertFrame, Envelope, HeartbeatData, HeartbeatFrame, HostFrame, InterruptFrame, MessageData, MessageFrame, MsgAckFrame, RECOGNIZED_MESSAGE_KINDS, TaskDeleteFrame, TaskUpsertData, TaskUpsertFrame, ThreadMessageData, ThreadMessageFrame, ThreadResultData, ThreadResultFrame } from "@gino/host-core/domain";
// … and the version handshake constants.
export { HEARTBEAT_INTERVAL_MS, isSupportedProtocol, PROTOCOL_VERSION } from "@gino/host-core/domain";

// ─── The `AppError` family (what every namespace throws) ───
export { AppError, ForbiddenError, NotFoundError, PayloadTooLargeError, UnauthorizedError, ValidationError } from "@gino/host-core/domain";
