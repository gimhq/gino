import type { AgentGrantRow, AgentRow, Storage } from "../host/ports/storage";
// M3 — AGENT REGISTRY (identity, enrollment, the per-agent connect token).
//
// Agents are host resources with an access policy (OD13). The registry owns the
// `agents` table: enroll on `POST /agent/register` (version-gated upstream),
// mint a per-agent connect token (store ONLY its hash — protocol §3 / OD12,
// revoked on remove), persist capabilities/source/access, and drive the
// `enrolled → online ⇄ offline → removed` lifecycle. It NEVER touches the wire
// or parses frame `data` — that is M2's job.
//
// It also owns `agent_grants` (the OD13 access policy) + the capacity (DP3) and
// removal helpers the control layer (M3 `control.ts`) builds on. Storage goes
// through the `Storage` port (`storage.agents` / `storage.tasks`); the token
// hashing, id minting and status policy stay here.
import { randomBytes } from "node:crypto";
import { hashToken, ulid, verifyToken } from "../platform";

export type AgentStatus = "enrolled" | "online" | "offline" | "removed";

/** What an agent can run. `engines` is an open list of engine names — never assume a fixed set. */
export interface AgentCapabilities {
  engines: string[];
  max_threads?: number;
}

/** Everything needed to mint an agent identity. */
export interface EnrollInput {
  name?: string;
  /** How the agent runs (`manual` | `docker` | `vm` | …); defaults to `manual`. */
  source?: string;
  /**
   * Access policy (OD13): `dedicated` (granted owners only) | `public`.
   * TRUSTED CALLERS ONLY (HOST-M3-09) — the register edge must NEVER forward
   * the request body's `access` here: a register-token holder would otherwise
   * self-declare `public` and become a dispatch target for every owner.
   * `public` is set by the admin access mutation or an admin launch allowlist.
   */
  access?: string;
  capabilities: AgentCapabilities;
  /** Which shared register token enrolled it (audit; the token itself is shared). */
  registerTokenRef?: string;
}

/** The enrolled agent plus its connect token — the ONLY time the plaintext token exists. */
export interface EnrollResult {
  agent: AgentRow;
  /** The plaintext connect token — returned to the agent ONCE; only its hash is stored. */
  connectToken: string;
}

/** Mint a high-entropy per-agent connect token (never the shared register token). */
function mintConnectToken(): string {
  return randomBytes(32).toString("hex");
}

/**
 * Enroll a new agent: generate the ULID `agentId`, mint + hash the per-agent
 * connect token, and persist identity/capabilities/access at status
 * `enrolled`. The plaintext token is returned once for the register response;
 * the row keeps only `connect_token_hash`.
 */
export function enrollAgent(storage: Storage, input: EnrollInput): EnrollResult {
  const id = ulid();
  const connectToken = mintConnectToken();

  const agent = storage.agents.insert({
    id,
    name: input.name ?? null,
    source: input.source ?? "manual",
    access: input.access ?? "dedicated",
    connectTokenHash: hashToken(connectToken),
    registerTokenRef: input.registerTokenRef ?? null,
    capabilities: input.capabilities,
    status: "enrolled",
    activeThreads: 0,
  });

  return { agent, connectToken };
}

export function getAgent(storage: Storage, agentId: string): AgentRow | undefined {
  return storage.agents.get(agentId);
}

/** List every agent (admin observe surface), newest first. */
export function listAgents(storage: Storage): AgentRow[] {
  return storage.agents.list();
}

/**
 * Verify a presented connect token against the agent's stored hash (constant
 * time, via platform `verifyToken`). Returns the agent on success, else `null`
 * — an unknown agent, a `removed` agent (token revoked, OD12), or a bad token
 * all fail closed.
 */
export function verifyConnectToken(storage: Storage, agentId: string, token: string): AgentRow | null {
  const agent = getAgent(storage, agentId);
  if (!agent || agent.status === "removed") {
    return null;
  }
  if (!token || !verifyToken(token, agent.connectTokenHash)) {
    return null;
  }
  return agent;
}

/**
 * Resolve the agent that OWNS a presented connect token, by matching the token's
 * sha-256 against `connect_token_hash` (the at-rest form — the plaintext is never
 * stored). This is the token→identity resolver the connect-token gate uses so the
 * requesting agent is taken from the TOKEN, never a client-supplied param (the
 * IDOR fix). A `removed` agent (token revoked, OD12) and an unmatched token both
 * return `null` — fail closed. Mirrors the M7 proxy credential lookup idiom.
 */
export function getAgentByConnectToken(storage: Storage, token: string): AgentRow | null {
  if (!token) {
    return null;
  }
  return storage.agents.getByConnectTokenHash(hashToken(token)) ?? null;
}

/**
 * Drive the agent lifecycle status (`enrolled → online ⇄ offline → removed`).
 * `removed` is TERMINAL: a row already `removed` is never transitioned out —
 * so the late WS close a remove provokes cannot resurrect the agent as
 * `offline` (a resurrected row would again pass the `status === "removed"`
 * fail-closed gates while its connect token stays revoked).
 */
export function setAgentStatus(storage: Storage, agentId: string, status: AgentStatus): void {
  storage.agents.updateStatusIf(agentId, { notFrom: "removed" }, status);
}

/**
 * Liveness touch from a `heartbeat` (or any inbound frame): bump `last_seen`
 * and, when reported, `active_threads`. The full offline-grace sweep that flips
 * a silent agent to `offline` is B2 — this is the light update it builds on.
 *
 * HOST-M3-03: a frame from an `offline` agent proves the socket is alive
 * (sweep-marked offline during heartbeat starvation, socket never dropped), so
 * restore `online`. The transition is a guarded CAS out of `offline` ONLY —
 * `removed` stays terminal and `enrolled` is untouched.
 */
export function touchLiveness(storage: Storage, agentId: string, activeThreads?: number): void {
  storage.agents.touchLiveness(agentId, Date.now(), activeThreads);
  storage.agents.updateStatusIf(agentId, { from: "offline" }, "online");
}

// ─── Access policy (OD13) — agent_grants ───

/**
 * Grant an owner access to a `dedicated` agent (idempotent — a repeat grant is
 * a no-op). For a `public` agent a grant is harmless but unnecessary
 * (`isOwnerPermitted` already returns true for any owner).
 */
export function grantOwner(storage: Storage, agentId: string, ownerId: string): void {
  storage.agents.grant(agentId, ownerId);
}

/** Revoke an owner's grant on an agent (idempotent). */
export function revokeOwner(storage: Storage, agentId: string, ownerId: string): void {
  storage.agents.revoke(agentId, ownerId);
}

/**
 * Set an agent's access policy (OD13). ADMIN-ONLY mutation (HOST-M3-09) —
 * routed like grant/revoke through the control layer; enrollment itself always
 * lands `dedicated` (or the launch config's admin allowlist via
 * `correlateLaunch`), never a register-body value.
 */
export function setAgentAccess(storage: Storage, agentId: string, access: "dedicated" | "public"): void {
  storage.agents.setAccess(agentId, access);
}

/** List the owners granted access to an agent. */
export function listGrants(storage: Storage, agentId: string): AgentGrantRow[] {
  return storage.agents.listGrants(agentId);
}

/**
 * OD13 access check used by M4 dispatch + the admin API: a `public` agent is
 * usable by ANY owner; a `dedicated` agent only by owners present in
 * `agent_grants`. An unknown or `removed` agent is permitted to no one (fails
 * closed).
 */
export function isOwnerPermitted(storage: Storage, agentId: string, ownerId: string): boolean {
  const agent = getAgent(storage, agentId);
  if (!agent || agent.status === "removed") {
    return false;
  }
  if (agent.access === "public") {
    return true;
  }
  return storage.agents.hasGrant(agentId, ownerId);
}

// ─── Capacity (DP3) ───

export interface AgentCapacity {
  /** Agent-reported in-flight threads (from `heartbeat`). */
  activeThreads: number;
  /** Declared/host-overridable ceiling (`capabilities.max_threads`); null = unbounded. */
  maxThreads: number | null;
  /** Whether the agent can take another thread now. */
  hasRoom: boolean;
}

/**
 * DP3 capacity: agent-reported `active_threads` vs declared `max_threads`
 * (host-overridable). `max_threads` unset = unbounded. Returns null for an
 * unknown agent.
 */
export function getCapacity(storage: Storage, agentId: string): AgentCapacity | null {
  const agent = getAgent(storage, agentId);
  if (!agent) {
    return null;
  }
  const activeThreads = agent.activeThreads ?? 0;
  const maxThreads = agent.capabilities.max_threads ?? null;
  return {
    activeThreads,
    maxThreads,
    hasRoom: maxThreads == null || activeThreads < maxThreads,
  };
}

// ─── Removal (OD12 + DP4) ───

export interface RemoveAgentResult {
  /** Number of tasks orphaned (placement cleared; rows kept as history). */
  orphanedTasks: number;
}

/**
 * Permanently remove an agent (admin OOB). Two host-side effects:
 *   1. REVOKE the connect token (OD12): rotate `connect_token_hash` to a fresh,
 *      unmatchable value and flip `status` to `removed`, so any presented token
 *      now fails (`verifyConnectToken` also fails closed on `removed`).
 *   2. ORPHAN its tasks (DP4 / no-migration): clear `tasks.agent_id` so the
 *      mirror is KEPT AS HISTORY — tasks/crons/messages are NOT deleted.
 *      Continuing the work becomes a NEW task on another agent.
 * The runtime teardown (launcher.remove / a WS `close`) is the control layer's
 * job — this is purely the durable bookkeeping.
 */
export function removeAgent(storage: Storage, agentId: string): RemoveAgentResult {
  const revokedHash = hashToken(randomBytes(32).toString("hex"));
  storage.agents.setRemoved(agentId, revokedHash);

  const orphaned = storage.tasks.orphanByAgent(agentId, Date.now());

  return { orphanedTasks: orphaned.length };
}

// ─── Launch↔register correlation (OD17) ───
//
// The register token is shared + long-lived (OD12), so a registration cannot by
// itself say WHICH launch produced it. `control.launchAgent` mints a per-launch
// nonce, injects it into the launch env (`GINO_LAUNCH_NONCE`) and records the
// pending launch here keyed by that nonce; the agent echoes the nonce in `POST
// /agent/register`, and `correlateLaunch` binds the new `agentId` to the launch
// (`launcher_kind` + `launch_ref`) so OOB control (M3 `control.ts`) routes to
// the owning launcher.
//
// HOST-M3-05: the pending set is PERSISTED (`pending_launches`) — a host
// restart must neither orphan a running container (the TTL reaper in
// `launch-reaper.ts` still finds the row and tears the runtime down) nor break
// a late registration's correlation. The launch→register window stays short:
// rows are consumed on correlate or reaped past the TTL.

export interface PendingLaunch {
  readonly launcherKind: string;
  /** The launcher's runtime handle (e.g. a container id); null if it started nothing. */
  readonly launchRef: string | null;
  /** Admin-allowlisted access applied at correlate (HOST-M3-09); defaults to `dedicated`. */
  readonly access?: string;
}

/** A pending launch that outlived the TTL — the reaper tears its runtime down. */
export interface ExpiredPendingLaunch extends PendingLaunch {
  readonly nonce: string;
}

/** Record a pending launch keyed by its per-launch nonce (called by `control.launchAgent`). */
export function recordPendingLaunch(storage: Storage, nonce: string, pending: PendingLaunch): void {
  storage.agents.recordPending({
    nonce,
    launcherKind: pending.launcherKind,
    launchRef: pending.launchRef,
    access: pending.access ?? "dedicated",
  });
}

/**
 * Correlate a registering agent to its launch by nonce (OD17). When the nonce
 * matches a pending launch, bind the agent (`launcher_kind` + `launch_ref` +
 * the nonce itself, the HOST-M3-06 idempotency key) and CONSUME the entry;
 * returns the bound launch. The launch's admin-allowlisted `access` is applied
 * here (HOST-M3-09) — never taken from the register body. An unknown/absent
 * nonce returns `null` and changes nothing — so a register WITHOUT a nonce
 * (manual enroll) is entirely unaffected (backward-compatible).
 */
export function correlateLaunch(storage: Storage, agentId: string, nonce: string): PendingLaunch | null {
  const pending = storage.agents.correlate(agentId, nonce);
  if (!pending) {
    return null;
  }
  return { launcherKind: pending.launcherKind, launchRef: pending.launchRef, access: pending.access };
}

/** Whether a launch nonce is still awaiting its registration (consumed on correlate, reaped past TTL). */
export function hasPendingLaunch(storage: Storage, nonce: string): boolean {
  return storage.agents.hasPending(nonce);
}

/**
 * Pending launches recorded before `cutoff` (epoch ms) — launches whose agent
 * never registered inside the TTL. Read by the reaper (HOST-M3-05), which
 * tears down each runtime and then `deletePendingLaunch`es the row.
 */
export function listExpiredPendingLaunches(storage: Storage, cutoff: number): ExpiredPendingLaunch[] {
  return storage.agents
    .listExpiredPending(cutoff)
    .map(row => ({ nonce: row.nonce, launcherKind: row.launcherKind, launchRef: row.launchRef, access: row.access }));
}

/** Drop a pending launch (reaper teardown done — or the runtime is unreachable). */
export function deletePendingLaunch(storage: Storage, nonce: string): void {
  storage.agents.deletePending(nonce);
}

/**
 * Idempotent re-enroll (HOST-M3-06): a register RETRY carrying a launch nonce
 * that already correlated (the response was lost in transit — the retrying
 * agent never saw its token) returns the EXISTING agent row instead of minting
 * a duplicate. The connect token is ROTATED and returned fresh: only its hash
 * is at rest, and the original plaintext reached nobody. A `removed` agent is
 * never resurrected (returns `null` → the retry enrolls a fresh row).
 */
export function reEnrollByLaunchNonce(storage: Storage, nonce: string): EnrollResult | null {
  const existing = storage.agents.getByLaunchNonce(nonce);
  if (!existing) {
    return null;
  }
  const connectToken = mintConnectToken();
  const agent = storage.agents.setConnectTokenHash(existing.id, hashToken(connectToken));
  return { agent: agent ?? existing, connectToken };
}
