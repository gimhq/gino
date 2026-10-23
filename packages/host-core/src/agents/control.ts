import type { HostEvents } from "../host/events";
// M3 — CONTROL (OOB agent control; the admin lever).
//
// The host commands agents over the protocol (M2), but reaching an agent's
// *runtime* — stop it, restart it, tear it down, inspect it — is out-of-band,
// delegated to the agent's owning Launcher by `launch_ref`. When the launcher
// cannot do an op (the `manual` default, or a launcher that lost its ref), the
// host falls back to a WS `close` frame enqueued on the M2 outbox (delivered
// now if online, replayed on reconnect otherwise).
//
// `remove` additionally does the durable bookkeeping (registry.removeAgent):
// revoke the connect token and ORPHAN the agent's tasks (the
// mirror is kept as history; nothing is deleted or migrated). `launch` is a
// later phase (B2b) — it is a stub here.
//
// Grant/revoke manage the access policy (`agent_grants`) via the registry.
import type { AgentRow, Storage } from "../host/ports/storage";
import type { AgentTransport } from "../host/ports/transport";
import type { Logger } from "../logger";
import type { InspectState, Launcher } from "./launchers/types";
import type { RemoteOpsFlags } from "./registry";
import type { SessionRegistry } from "./sessions";
import { randomBytes } from "node:crypto";
import { enqueue, purgeAgentOutbox } from "../protocol/outbox";
import { AppError, NotFoundError } from "../shared/lib/errors";
import { manualLauncher } from "./launchers/manual";
import { getAgent, grantOwner, recordPendingLaunch, removeAgent, revokeOwner, setAgentAccess, setAgentRemoteOps } from "./registry";

/** Which out-of-band control operation a `ControlResult` describes. */
export type ControlOp = "stop" | "restart" | "remove";

/** How an OOB op was carried out: by the launcher, or via a WS `close` fallback. */
export type ControlVia = "launcher" | "ws-close";

export interface ControlDeps {
  readonly storage: Storage;
  /** The live host→agent push surface (the M3 `SessionRegistry`). */
  readonly sender: AgentTransport;
  /**
   * The session registry's force-close surface: remove severs the
   * live socket host-side so a non-cooperating agent loses its authenticated
   * pipe. Optional — callers without a live registry (some tests) skip it.
   */
  readonly sessions?: Pick<SessionRegistry, "get" | "detach">;
  readonly logger?: Logger;
  /** Host event emitter: `remove` announces `agent.removed` after the durable bookkeeping. */
  readonly events?: HostEvents;
  /** Launcher lookup seam — pass `host.launchers.resolve`; defaults to the manual launcher (controls nothing). */
  readonly resolveLauncher?: (kind: string | null | undefined) => Launcher;
}

/** The outcome of an out-of-band control op: which launcher acted, on which runtime handle, and whether it went through the launcher or degraded to a `close` frame. */
export interface ControlResult {
  readonly agentId: string;
  readonly op: ControlOp;
  readonly via: ControlVia;
  readonly launcherKind: string;
  readonly ref: string | null;
}

/** A `remove` outcome, adding how many tasks lost their placement. */
export interface RemoveResult extends ControlResult {
  readonly op: "remove";
  /** Tasks orphaned (placement cleared; mirror kept as history). */
  readonly orphanedTasks: number;
}

function requireAgent(storage: Storage, agentId: string): AgentRow {
  const agent = getAgent(storage, agentId);
  if (!agent) {
    throw new NotFoundError("agent", agentId);
  }
  return agent;
}

/**
 * Like `requireAgent`, but a `removed` agent is a 410: its token
 * is revoked and it can never reconnect, so stop/restart/grant would only feed
 * undeliverable frames (or grant access to a dead resource). Remove itself
 * stays idempotent and does NOT use this gate.
 */
function requireActiveAgent(storage: Storage, agentId: string): AgentRow {
  const agent = requireAgent(storage, agentId);
  if (agent.status === "removed") {
    throw new AppError(`agent ${agentId} is removed`, 410, "AGENT_REMOVED");
  }
  return agent;
}

/**
 * Force-close the agent's live session host-side: revocation must
 * not depend on the agent honouring a `close` frame — a non-cooperating
 * (compromised) agent would otherwise keep its authenticated pipe until a
 * network drop. Best-effort: the socket may already be gone.
 */
function forceCloseSession(deps: ControlDeps, agentId: string): void {
  const session = deps.sessions?.get(agentId);
  if (!session) {
    return;
  }
  try {
    session.ws.close(1008, "agent removed");
  }
  catch {
    // The socket may already be dead — detaching below is what matters.
  }
  deps.sessions?.detach(agentId);
  deps.logger?.info({ agentId }, "agent session force-closed on remove");
}

/** Whether a launcher error means the runtime is ALREADY gone (remove's goal state). */
function isRuntimeGone(err: unknown): boolean {
  return err instanceof Error && /no such container/i.test(err.message);
}

function resolve(deps: ControlDeps, agent: AgentRow): Launcher {
  return (deps.resolveLauncher ?? (() => manualLauncher))(agent.launcherKind);
}

/** Enqueue a host→agent `close` frame (durable; the OOB fallback when a launcher can't act). */
function closeViaWs(deps: ControlDeps, agentId: string, reason: ControlOp): void {
  enqueue(deps.storage, deps.sender, { agentId, type: "close", data: { reason } });
}

/**
 * OOB stop: if the launcher can stop the runtime and we hold a `launch_ref`,
 * delegate; otherwise fall back to a WS `close`. The agent's `online`/`offline`
 * state follows from the resulting WS close (`onClose`) — we don't force it
 * here. Tasks/crons are untouched: a stopped agent resumes them on
 * reconnect.
 */
export async function stopAgent(deps: ControlDeps, agentId: string): Promise<ControlResult> {
  const agent = requireActiveAgent(deps.storage, agentId);
  const launcher = resolve(deps, agent);

  if (launcher.capabilities.canStop && agent.launchRef) {
    await launcher.stop(agent.launchRef);
    deps.logger?.info({ agentId, launcher: launcher.kind, ref: agent.launchRef }, "agent stop via launcher");
    return { agentId, op: "stop", via: "launcher", launcherKind: launcher.kind, ref: agent.launchRef };
  }

  closeViaWs(deps, agentId, "stop");
  deps.logger?.info({ agentId, launcher: launcher.kind }, "agent stop via ws close (launcher unsupported)");
  return { agentId, op: "stop", via: "ws-close", launcherKind: launcher.kind, ref: agent.launchRef ?? null };
}

/**
 * OOB restart: delegate when the launcher can restart + we hold a `ref`;
 * otherwise fall back to a WS `close` — best-effort, the agent's own supervisor
 * (or the operator, for `manual`) brings it back. Tasks/crons survive.
 */
export async function restartAgent(deps: ControlDeps, agentId: string): Promise<ControlResult> {
  const agent = requireActiveAgent(deps.storage, agentId);
  const launcher = resolve(deps, agent);

  if (launcher.capabilities.canRestart && agent.launchRef) {
    await launcher.restart(agent.launchRef);
    deps.logger?.info({ agentId, launcher: launcher.kind, ref: agent.launchRef }, "agent restart via launcher");
    return { agentId, op: "restart", via: "launcher", launcherKind: launcher.kind, ref: agent.launchRef };
  }

  closeViaWs(deps, agentId, "restart");
  deps.logger?.info({ agentId, launcher: launcher.kind }, "agent restart via ws close (launcher unsupported)");
  return { agentId, op: "restart", via: "ws-close", launcherKind: launcher.kind, ref: agent.launchRef ?? null };
}

/**
 * OOB remove: tear down the runtime (launcher.remove when capable + we hold a
 * `ref`, else a WS `close`), THEN the durable bookkeeping — revoke the connect
 * token, orphan the agent's tasks (mirror kept), purge its now
 * undeliverable outbox rows, and force-close any live session.
 * A launcher's runtime control is gated on `canStop` (a launcher
 * that can stop a runtime can remove it). The close is sent BEFORE the token
 * is revoked so it can still go out on the live socket.
 *
 * The bookkeeping runs UNCONDITIONALLY — a launcher failure
 * (including `DockerOpTimeoutError` on a wedged daemon) degrades to the
 * WS-close path instead of aborting, and a runtime that is already gone
 * ("no such container") counts as launcher success. Remove is idempotent:
 * repeating it on a `removed` agent re-runs only the local cleanup.
 */
export async function removeAgentControl(deps: ControlDeps, agentId: string): Promise<RemoveResult> {
  const agent = requireAgent(deps.storage, agentId);
  const launcher = resolve(deps, agent);

  if (agent.status === "removed") {
    purgeAgentOutbox(deps.storage, agentId);
    forceCloseSession(deps, agentId);
    deps.logger?.info({ agentId }, "agent remove repeated on removed agent (no-op cleanup)");
    return { agentId, op: "remove", via: "ws-close", launcherKind: launcher.kind, ref: agent.launchRef ?? null, orphanedTasks: 0 };
  }

  let via: ControlVia;
  if (launcher.capabilities.canStop && agent.launchRef) {
    try {
      await launcher.remove(agent.launchRef);
      via = "launcher";
    }
    catch (err) {
      if (isRuntimeGone(err)) {
        via = "launcher";
        deps.logger?.info({ agentId, ref: agent.launchRef }, "agent runtime already gone — treating remove as done");
      }
      else {
        closeViaWs(deps, agentId, "remove");
        via = "ws-close";
        deps.logger?.warn({ agentId, ref: agent.launchRef, err }, "launcher remove failed — degrading to ws close; bookkeeping proceeds");
      }
    }
  }
  else {
    closeViaWs(deps, agentId, "remove");
    via = "ws-close";
  }

  const { orphanedTasks } = removeAgent(deps.storage, agentId);
  deps.events?.emit("agent.removed", { agentId });
  const purgedFrames = purgeAgentOutbox(deps.storage, agentId);
  forceCloseSession(deps, agentId);
  deps.logger?.info({ agentId, launcher: launcher.kind, via, orphanedTasks, purgedFrames }, "agent removed (token revoked, tasks orphaned, outbox purged)");
  return { agentId, op: "remove", via, launcherKind: launcher.kind, ref: agent.launchRef ?? null, orphanedTasks };
}

/**
 * OOB inspect: coarse runtime state from the launcher when it can inspect + we
 * hold a `ref`; otherwise `unknown` (the `manual` default — the host only knows
 * the agent's protocol state, surfaced separately as `agents.status`).
 */
export async function inspectAgent(deps: ControlDeps, agentId: string): Promise<InspectState> {
  const agent = requireAgent(deps.storage, agentId);
  const launcher = resolve(deps, agent);
  if (launcher.capabilities.canInspect && agent.launchRef) {
    return launcher.inspect(agent.launchRef);
  }
  return "unknown";
}

/** Grant an owner access to an agent. 404 if unknown; 410 if `removed`. */
export function grantAgentOwner(deps: ControlDeps, agentId: string, ownerId: string): void {
  requireActiveAgent(deps.storage, agentId);
  grantOwner(deps.storage, agentId, ownerId);
}

/** Revoke an owner's access to an agent. 404 if the agent is unknown. */
export function revokeAgentOwner(deps: ControlDeps, agentId: string, ownerId: string): void {
  requireAgent(deps.storage, agentId);
  revokeOwner(deps.storage, agentId, ownerId);
}

/**
 * Set an agent's access policy — the ADMIN-ONLY mutation that makes an
 * agent `public` (enrollment never honours a client-supplied
 * `access`). 404 if unknown; 410 if `removed` (like grant).
 */
export function setAgentAccessControl(deps: ControlDeps, agentId: string, access: "dedicated" | "public"): void {
  requireActiveAgent(deps.storage, agentId);
  setAgentAccess(deps.storage, agentId, access);
  deps.logger?.info({ agentId, access }, "agent access policy set (admin)");
}

/**
 * Enable or disable engine-free operation groups on an agent. Persisted, then
 * pushed as a `config_update` (queued for an offline agent, which does not
 * re-fetch its config on reconnect). 404 if unknown; 410 if `removed`.
 */
export function setAgentRemoteOpsControl(deps: ControlDeps, agentId: string, flags: RemoteOpsFlags): Required<RemoteOpsFlags> {
  const agent = requireActiveAgent(deps.storage, agentId);
  setAgentRemoteOps(deps.storage, agent, flags);
  const remoteOps = {
    fs: flags.fs ?? agent.capabilities.remote_ops?.fs ?? false,
    pty: flags.pty ?? agent.capabilities.remote_ops?.pty ?? false,
  };
  enqueue(deps.storage, deps.sender, { agentId, type: "config_update", data: { remoteOps } });
  deps.logger?.info({ agentId, remoteOps }, "agent remote ops set (admin)");
  return remoteOps;
}

/** A config-driven launch request (B2b). `hostUrl` + `registerToken` are host-supplied (the agent never picks them). */
export interface LaunchInput {
  /** Which launcher runs it (`docker` | …) — selected from the registry. */
  readonly kind: string;
  /** Where the launched agent dials back to register (injected as `GINO_HOST_URL`). */
  readonly hostUrl: string;
  /** The shared register token the agent presents to enroll (`GINO_REGISTER_TOKEN`). */
  readonly registerToken: string;
  /** Launcher-specific image (docker); forwarded via `resources.image`. */
  readonly image?: string;
  /** Optional work the launch is for (carried through to the launcher). */
  readonly workload?: { readonly taskId?: string };
  /** Extra env injected into the runtime (merged under the host-authoritative keys). */
  readonly env?: Record<string, string>;
  /** Launcher-specific resource hints (cpu/mem/image/…). */
  readonly resources?: Record<string, unknown>;
  /**
   * Access policy the launched agent enrolls with: the launch is
   * an ADMIN request, so `public` may be allowlisted here — applied at
   * correlate time, never taken from the agent's register body.
   */
  readonly access?: "dedicated" | "public";
}

/** A started launch, before any agent exists: the correlation nonce the agent must echo, and the launcher's runtime handle. */
export interface LaunchAgentResult {
  readonly launcherKind: string;
  /** The per-launch correlation nonce — the agent echoes it in `register`. */
  readonly nonce: string;
  /** The launcher's runtime handle (e.g. a container id); null if it started nothing. */
  readonly ref: string | null;
}

/** Mint a per-launch correlation nonce — high-entropy, single-use. */
function mintLaunchNonce(): string {
  return randomBytes(24).toString("hex");
}

/**
 * Launch + correlate (config-driven, B2b). Pick the launcher by `kind`,
 * mint a per-launch nonce, inject it (`GINO_LAUNCH_NONCE`) alongside the
 * caller's env, and start the runtime via `launcher.launch`. The pending launch
 * is recorded by nonce so the agent's later `POST /agent/register` (echoing the
 * nonce) binds its new `agentId` to this launch — persisting `launcher_kind` +
 * `launch_ref` so subsequent OOB control reaches the right launcher.
 *
 * A launcher that cannot launch (the `manual` default) is a 422: the host
 * starts nothing it was not explicitly asked to (safe by default).
 */
export async function launchAgent(deps: ControlDeps, input: LaunchInput): Promise<LaunchAgentResult> {
  const launcher = (deps.resolveLauncher ?? (() => manualLauncher))(input.kind);
  if (!launcher.capabilities.canLaunch) {
    throw new AppError(`launcher '${input.kind}' cannot launch agents`, 422, "LAUNCH_UNSUPPORTED");
  }

  const nonce = mintLaunchNonce();
  const resources: Record<string, unknown> = {
    ...input.resources,
    ...(input.image ? { image: input.image } : {}),
  };
  const { ref } = await launcher.launch({
    hostUrl: input.hostUrl,
    registerToken: input.registerToken,
    workload: input.workload ?? {},
    env: { ...input.env, GINO_LAUNCH_NONCE: nonce },
    resources,
  });

  // Record the pending launch so the matching registration correlates.
  // Persisted: survives a host restart and is TTL-reaped when the
  // agent never registers (see launch-reaper.ts).
  recordPendingLaunch(deps.storage, nonce, { launcherKind: launcher.kind, launchRef: ref, ...(input.access ? { access: input.access } : {}) });
  deps.logger?.info({ launcher: launcher.kind, ref, taskId: input.workload?.taskId }, "agent launch (awaiting registration)");
  return { launcherKind: launcher.kind, nonce, ref };
}
