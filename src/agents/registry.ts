import type { AppDatabase } from "@/db";
import type { Agent, AgentGrant } from "@/db/schema";
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
// removal helpers the control layer (M3 `control.ts`) builds on.
import { randomBytes } from "node:crypto";
import { and, desc, eq, ne } from "drizzle-orm";
import { agentGrants, agents, tasks } from "@/db/schema";
import { hashToken, ulid, verifyToken } from "@/platform";

export type AgentStatus = "enrolled" | "online" | "offline" | "removed";

export interface AgentCapabilities {
  engines: string[];
  max_threads?: number;
}

export interface EnrollInput {
  name?: string;
  /** How the agent runs (`manual` | `docker` | `vm` | …); defaults to `manual`. */
  source?: string;
  /** Access policy (OD13): `dedicated` (granted owners only) | `public`. */
  access?: string;
  capabilities: AgentCapabilities;
  /** Which shared register token enrolled it (audit; the token itself is shared). */
  registerTokenRef?: string;
}

export interface EnrollResult {
  agent: Agent;
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
export function enrollAgent(db: AppDatabase, input: EnrollInput): EnrollResult {
  const id = ulid();
  const connectToken = mintConnectToken();

  const agent = db
    .insert(agents)
    .values({
      id,
      name: input.name ?? null,
      source: input.source ?? "manual",
      access: input.access ?? "dedicated",
      connectTokenHash: hashToken(connectToken),
      registerTokenRef: input.registerTokenRef ?? null,
      capabilities: input.capabilities,
      status: "enrolled",
      activeThreads: 0,
    })
    .returning()
    .get();

  return { agent, connectToken };
}

export function getAgent(db: AppDatabase, agentId: string): Agent | undefined {
  return db.select().from(agents).where(eq(agents.id, agentId)).get();
}

/** List every agent (admin observe surface), newest first. */
export function listAgents(db: AppDatabase): Agent[] {
  return db.select().from(agents).orderBy(desc(agents.createdAt)).all();
}

/**
 * Verify a presented connect token against the agent's stored hash (constant
 * time, via platform `verifyToken`). Returns the agent on success, else `null`
 * — an unknown agent, a `removed` agent (token revoked, OD12), or a bad token
 * all fail closed.
 */
export function verifyConnectToken(db: AppDatabase, agentId: string, token: string): Agent | null {
  const agent = getAgent(db, agentId);
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
export function getAgentByConnectToken(db: AppDatabase, token: string): Agent | null {
  if (!token) {
    return null;
  }
  const agent = db
    .select()
    .from(agents)
    .where(and(eq(agents.connectTokenHash, hashToken(token)), ne(agents.status, "removed")))
    .get();
  return agent ?? null;
}

/**
 * Drive the agent lifecycle status (`enrolled → online ⇄ offline → removed`).
 * `removed` is TERMINAL: a row already `removed` is never transitioned out —
 * so the late WS close a remove provokes cannot resurrect the agent as
 * `offline` (a resurrected row would again pass the `status === "removed"`
 * fail-closed gates while its connect token stays revoked).
 */
export function setAgentStatus(db: AppDatabase, agentId: string, status: AgentStatus): void {
  db
    .update(agents)
    .set({ status })
    .where(and(eq(agents.id, agentId), ne(agents.status, "removed")))
    .run();
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
export function touchLiveness(db: AppDatabase, agentId: string, activeThreads?: number): void {
  db
    .update(agents)
    .set({
      lastSeen: Date.now(),
      ...(activeThreads != null ? { activeThreads } : {}),
    })
    .where(eq(agents.id, agentId))
    .run();
  db
    .update(agents)
    .set({ status: "online" })
    .where(and(eq(agents.id, agentId), eq(agents.status, "offline")))
    .run();
}

// ─── Access policy (OD13) — agent_grants ───

/**
 * Grant an owner access to a `dedicated` agent (idempotent — a repeat grant is
 * a no-op). For a `public` agent a grant is harmless but unnecessary
 * (`isOwnerPermitted` already returns true for any owner).
 */
export function grantOwner(db: AppDatabase, agentId: string, ownerId: string): void {
  db.insert(agentGrants).values({ agentId, ownerId }).onConflictDoNothing().run();
}

/** Revoke an owner's grant on an agent (idempotent). */
export function revokeOwner(db: AppDatabase, agentId: string, ownerId: string): void {
  db
    .delete(agentGrants)
    .where(and(eq(agentGrants.agentId, agentId), eq(agentGrants.ownerId, ownerId)))
    .run();
}

/** List the owners granted access to an agent. */
export function listGrants(db: AppDatabase, agentId: string): AgentGrant[] {
  return db.select().from(agentGrants).where(eq(agentGrants.agentId, agentId)).all();
}

/**
 * OD13 access check used by M4 dispatch + the admin API: a `public` agent is
 * usable by ANY owner; a `dedicated` agent only by owners present in
 * `agent_grants`. An unknown or `removed` agent is permitted to no one (fails
 * closed).
 */
export function isOwnerPermitted(db: AppDatabase, agentId: string, ownerId: string): boolean {
  const agent = getAgent(db, agentId);
  if (!agent || agent.status === "removed") {
    return false;
  }
  if (agent.access === "public") {
    return true;
  }
  const grant = db
    .select()
    .from(agentGrants)
    .where(and(eq(agentGrants.agentId, agentId), eq(agentGrants.ownerId, ownerId)))
    .get();
  return grant != null;
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
export function getCapacity(db: AppDatabase, agentId: string): AgentCapacity | null {
  const agent = getAgent(db, agentId);
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
export function removeAgent(db: AppDatabase, agentId: string): RemoveAgentResult {
  const revokedHash = hashToken(randomBytes(32).toString("hex"));
  db
    .update(agents)
    .set({ status: "removed", connectTokenHash: revokedHash })
    .where(eq(agents.id, agentId))
    .run();

  const orphaned = db
    .update(tasks)
    .set({ agentId: null, updatedAt: Date.now() })
    .where(eq(tasks.agentId, agentId))
    .returning({ id: tasks.id })
    .all();

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
// the owning launcher. The launch→register window is short and PROCESS-LOCAL —
// the host that launched is the host the container dials back to — so the
// pending set lives in memory (the v2 data model has no `launches` table).

export interface PendingLaunch {
  readonly launcherKind: string;
  /** The launcher's runtime handle (e.g. a container id); null if it started nothing. */
  readonly launchRef: string | null;
}

const pendingLaunches = new Map<string, PendingLaunch>();

/** Record a pending launch keyed by its per-launch nonce (called by `control.launchAgent`). */
export function recordPendingLaunch(nonce: string, pending: PendingLaunch): void {
  pendingLaunches.set(nonce, pending);
}

/**
 * Correlate a registering agent to its launch by nonce (OD17). When the nonce
 * matches a pending launch, bind the agent (`launcher_kind` + `launch_ref`) and
 * CONSUME the entry; returns the bound launch. An unknown/absent nonce returns
 * `null` and changes nothing — so a register WITHOUT a nonce (manual enroll) is
 * entirely unaffected (backward-compatible).
 */
export function correlateLaunch(db: AppDatabase, agentId: string, nonce: string): PendingLaunch | null {
  const pending = pendingLaunches.get(nonce);
  if (!pending) {
    return null;
  }
  pendingLaunches.delete(nonce);
  db
    .update(agents)
    .set({ launcherKind: pending.launcherKind, launchRef: pending.launchRef })
    .where(eq(agents.id, agentId))
    .run();
  return pending;
}

/** Whether a launch nonce is still awaiting its registration (consumed on correlate). */
export function hasPendingLaunch(nonce: string): boolean {
  return pendingLaunches.has(nonce);
}
