// M3 — AGENT MANAGEMENT ★ (agents as resources: identity, sessions, health,
// access policy, launch/control).
//
// SKELETON PLACEHOLDER — no logic yet (workstreams C/D). Planned files
// (docs/host-v2-design.md §1):
//   registry.ts   agents table (id = agentId): enroll, capabilities, status, capacity
//   sessions.ts   agentId → WS; reconnect + outbox replay
//   liveness.ts   heartbeat → online/offline + active_threads
//   launchers/    types.ts registry.ts manual.ts docker.ts (start + OOB control)
//   control.ts    launch+correlate · stop/restart/remove/inspect via owning launcher
export const MODULE = "agents" as const;
