// M2 — PROTOCOL VERSION + transport constants (host's own, NOT in src/config).
//
// The protocol version and the heartbeat interval are part of the wire contract
// (protocol §1 + §3), not deployment config — they live with the M2 adapter so
// the only module that knows the wire also owns the wire's invariants. The
// sibling M7 proxy owns `src/config`; nothing protocol-shaped goes there.

/** Current wire protocol (protocol §1). The register gate rejects any other. */
export const PROTOCOL_VERSION = 2 as const;

/**
 * Liveness cadence advertised to the agent in the register/config response
 * (`config.heartbeatIntervalMs`, protocol §3). The agent emits `heartbeat` at
 * this interval; the host's offline-grace window (a multiple of this) is B2.
 */
export const HEARTBEAT_INTERVAL_MS = 30_000;

/** The register-time version gate (protocol §1). Only the exact major matches. */
export function isSupportedProtocol(version: unknown): version is typeof PROTOCOL_VERSION {
  return version === PROTOCOL_VERSION;
}

/** The `register_error` body returned to a peer the host cannot speak to. */
export function registerError(reason: string): { register_error: { reason: string } } {
  return { register_error: { reason } };
}
