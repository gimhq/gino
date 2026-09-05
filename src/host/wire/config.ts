import type { AgentRow, Storage } from "../ports/storage";
import type { HttpResult, WireContext } from "./index";
import { getAgent } from "@/agents/registry";
import { getGlobalConfigVersion, GLOBAL_SCOPE, resolveEntriesForEngines } from "@/configstore";
import { PROTOCOL_VERSION } from "@/protocol/version";
import { UnauthorizedError } from "@/shared/lib/errors";

/** The host-authoritative agent config object (protocol §3) — register + config mirror it. */
export function agentConfig(agentId: string, heartbeatIntervalMs: number) {
  return {
    agentId,
    wsUrl: `/agent/ws/${agentId}`,
    configUrl: `/agent/config/${agentId}`,
    outputUrl: `/agent/output/${agentId}`,
    heartbeatIntervalMs,
    protocolVersion: PROTOCOL_VERSION,
  };
}

/**
 * M8 — the resolved GLOBAL desired set covering ALL the agent's declared
 * engines (HOST-M8-03 — a multi-engine agent lands every engine's targets, not
 * just its first capability's) plus the host-assigned monotonic `version`,
 * delivered on register / `GET /agent/config` (additive; the agent reconciles
 * only when the version advances — agent-architecture §9b). Empty entries when
 * nothing is declared, so the response stays backward-compatible.
 */
export function globalConfig(storage: Storage, agent: AgentRow) {
  return {
    entries: resolveEntriesForEngines(storage, GLOBAL_SCOPE, agent.capabilities.engines),
    version: getGlobalConfigVersion(storage),
  };
}

/** `GET /agent/config/:agentId` — mirror of the register config (the caller has authorized). */
export function configFor(ctx: WireContext, agentId: string): HttpResult {
  const agent = getAgent(ctx.storage, agentId);
  if (!agent) {
    throw new UnauthorizedError();
  }
  // M8 (B6): mirror the resolved GLOBAL desired set + version alongside the config.
  return { status: 200, body: { ...agentConfig(agentId, ctx.options.heartbeatIntervalMs), ...globalConfig(ctx.storage, agent) } };
}
