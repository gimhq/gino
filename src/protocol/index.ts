// M2 — PROTOCOL v2 ADAPTER (the only code that knows the wire).
//
// Barrel for the module's public surface (docs/host-v2-design.md §1):
//   envelope.ts  codec — extract ids without parsing `data`; rebuild on send
//   inbound.ts   frame → demux by threadId → dispatch by type → M5 writer → ack
//   outbox.ts    persist every host→agent frame before send; unacked replay
//   version.ts   protocolVersion gate
//   frames.ts    frame type definitions
export * from "@/protocol/envelope";
export * from "@/protocol/frames";
export * from "@/protocol/inbound";
export * from "@/protocol/outbox";
export * from "@/protocol/version";
