// @gino/host-core/domain — the domain modules re-exported wholesale.
//
// SEMI-INTERNAL: used by the @gino/host-* packages (host-store-sqlite,
// host-hono, host-channels, host-proxy) and the standalone app; NOT part of
// the frozen public API (`./index.ts`). P3 decides what graduates. Leaf
// modules are re-exported directly so the per-module `MODULE` markers stay
// out of the surface.

export * from "./agents/control";
export * from "./agents/launch-reaper";
export { DEFAULT_LAUNCHERS, LauncherRegistry } from "./agents/launchers";
export * from "./agents/launchers/docker";
export * from "./agents/launchers/manual";
export type * from "./agents/launchers/types";
export * from "./agents/liveness";
export * from "./agents/registry";
export * from "./agents/sessions";
export { createStore } from "./configstore";
export * from "./configstore/artifacts";
export * from "./configstore/entries";
export * from "./configstore/gc";
export * from "./configstore/serve";
export * from "./configstore/store";
export { createHostContext } from "./host/context";
export type { CreateHostContextOptions, HostContext } from "./host/context";
export * from "./host/events";
export { createHost } from "./host/host";
export type { AgentsApi, ConfigApi, CreateHostOptions, CronsApi, Host, TasksApi, TranscriptApi } from "./host/host";
export * from "./host/metrics";
export * from "./host/options";
export type * from "./host/ports/storage";
export type * from "./host/ports/transport";
export * from "./host/wire";
export * from "./host/wire/attachments";
export * from "./host/wire/config";
export * from "./host/wire/register";
export * from "./host/wire/socket";
export * from "./logger";
export * from "./msgstore/attachments";
export * from "./msgstore/crons";
export { getTranscript, getTranscriptStats, TRANSCRIPT_READ_LIMIT, writeThreadMessage } from "./msgstore/messages";
export type { ThreadMessageWrite, TranscriptReadOptions } from "./msgstore/messages";
export * from "./msgstore/reads";
export * from "./msgstore/thread-runs";
export * from "./platform/crypto";
export * from "./platform/ids";
export * from "./protocol/envelope";
export * from "./protocol/frames";
export * from "./protocol/inbound";
export * from "./protocol/outbox";
export * from "./protocol/version";
export * from "./shared/lib/body-limit";
export * from "./shared/lib/errors";
export * from "./shared/lib/id";
export * from "./shared/lib/net";
export * from "./store/memory";
export { assignTaskToAgent, DispatchError, placeTask } from "./tasks/dispatch";
export type { DispatchResult } from "./tasks/dispatch";
export * from "./tasks/lifecycle";
export * from "./tasks/orchestrator";
export * from "./tasks/store";
