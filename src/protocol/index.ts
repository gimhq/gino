// M2 — PROTOCOL v2 ADAPTER (the only code that knows the wire).
//
// SKELETON PLACEHOLDER — no logic yet (workstreams C/D). Planned files
// (docs/host-v2-design.md §1):
//   envelope.ts  codec — extract ids without parsing `data`; rebuild on send
//   inbound.ts   frame → demux by threadId → dispatch by type → M5 writer → ack
//   outbox.ts    persist every host→agent frame before send; unacked replay
//   version.ts   register protocolVersion gate
//   frames.ts    frame type definitions
export const MODULE = "protocol" as const;
