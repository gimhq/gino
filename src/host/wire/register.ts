import type { HttpResult, WireContext } from "./index";
import type { AgentCapabilities } from "@/agents/registry";
import { correlateLaunch, enrollAgent, reEnrollByLaunchNonce } from "@/agents/registry";
import { hashToken, safeEqual } from "@/platform";
import { isSupportedProtocol, registerError } from "@/protocol/version";
import { agentConfig, globalConfig } from "./config";

/**
 * Normalise the register body's `capabilities` into the stored shape. Protocol
 * §3 sends an engine-name array (`["claude", …]`); the design stores
 * `{ engines, max_threads? }` (DP3 — `max_threads` host-overridable). Both the
 * array form and an object form are accepted.
 */
function normalizeCapabilities(raw: unknown, maxThreads?: unknown): AgentCapabilities {
  const mt = typeof maxThreads === "number" ? maxThreads : undefined;
  if (Array.isArray(raw)) {
    const engines = raw.filter((x): x is string => typeof x === "string");
    return { engines, ...(mt != null ? { max_threads: mt } : {}) };
  }
  if (raw != null && typeof raw === "object") {
    const obj = raw as { engines?: unknown; max_threads?: unknown };
    const engines = Array.isArray(obj.engines) ? obj.engines.filter((x): x is string => typeof x === "string") : [];
    const objMt = typeof obj.max_threads === "number" ? obj.max_threads : mt;
    return { engines, ...(objMt != null ? { max_threads: objMt } : {}) };
  }
  return { engines: [], ...(mt != null ? { max_threads: mt } : {}) };
}

function isPlainObject(body: unknown): body is Record<string, unknown> {
  return body != null && typeof body === "object" && !Array.isArray(body);
}

/** Enrollment (M3) — register token; version-gated; mints the connect token. */
export function register(ctx: WireContext, rawBody: unknown, auth: { bearer: string | null }): HttpResult {
  // HOST-M1M2-14: EVERY register failure renders the protocol §3
  // `register_error` envelope (an agent parses `reason` to decide between
  // "exit permanently" and "retry") — token validation lives here, in the
  // handler, so a bad token never falls through to the scaffold's generic
  // 401 envelope. The shared register token rides in the body
  // (`{ registerToken }`) with an `Authorization: Bearer` fallback; the
  // comparison is constant-time and fails CLOSED on an unconfigured secret.
  const body = isPlainObject(rawBody) ? rawBody : null;
  const expected = ctx.options.registerToken;
  const bodyToken = body && typeof body.registerToken === "string" ? body.registerToken : null;
  const provided = auth.bearer ?? bodyToken;
  if (!expected || !provided || !safeEqual(provided, expected)) {
    return { status: 401, body: registerError("invalid_register_token") };
  }
  if (!ctx.options.registrationEnabled) {
    return { status: 403, body: registerError("registration_disabled") };
  }
  if (body == null) {
    return { status: 400, body: registerError("invalid_body") };
  }

  // Version gate (protocol §1): reject anything but the current major.
  if (!isSupportedProtocol(body.protocolVersion)) {
    return { status: 400, body: registerError("unsupported_protocol") };
  }

  const storage = ctx.storage;
  const launchNonce = typeof body.launchNonce === "string" && body.launchNonce.length > 0 ? body.launchNonce : null;

  // HOST-M3-06: registration is IDEMPOTENT per launch. A retry carrying a
  // nonce that already correlated (the response was lost — the agent never
  // saw its token) reuses the existing row with a ROTATED connect token
  // instead of minting a duplicate whose launch binding lands on a ghost.
  if (launchNonce) {
    const existing = reEnrollByLaunchNonce(storage, launchNonce);
    if (existing) {
      ctx.logger.info({ agentId: existing.agent.id, source: existing.agent.source }, "agent register retry — existing enrollment reused (token rotated)");
      const retryConfig = { ...agentConfig(existing.agent.id, ctx.options.heartbeatIntervalMs), ...globalConfig(storage, existing.agent) };
      return { status: 200, body: { agentId: existing.agent.id, connectToken: existing.connectToken, config: retryConfig } };
    }
  }

  // HOST-M3-09: the body's `access` is IGNORED — a register-token holder
  // never self-declares `public`. Enrollment always lands `dedicated`; a
  // host launch's admin allowlist applies at correlate, and flipping an
  // agent `public` later is the admin-only access mutation.
  const { agent, connectToken } = enrollAgent(storage, {
    ...(typeof body.name === "string" ? { name: body.name } : {}),
    source: typeof body.source === "string" ? body.source : "manual",
    capabilities: normalizeCapabilities(body.capabilities, body.maxThreads),
    // HOST-M3-15 provenance: which register token enrolled it — the
    // sha-256 at-rest form (the shared plaintext is never stored).
    registerTokenRef: hashToken(ctx.options.registerToken),
  });

  // OD17 launch↔register correlation (OPTIONAL, backward-compatible): when
  // the agent echoes the per-launch nonce it was launched with, bind it to
  // that launch (launcher_kind + launch_ref). No nonce → unchanged: a manual
  // enroll never touches the pending-launch set.
  if (launchNonce) {
    const correlated = correlateLaunch(storage, agent.id, launchNonce);
    if (correlated) {
      ctx.logger.info({ agentId: agent.id, launcher: correlated.launcherKind, ref: correlated.launchRef }, "agent registration correlated to launch");
    }
  }

  ctx.logger.info({ agentId: agent.id, source: agent.source }, "agent enrolled");
  ctx.events.emit("agent.enrolled", { agentId: agent.id });
  // M8 (B6): seed the resolved GLOBAL desired set + version on register (additive).
  const config = { ...agentConfig(agent.id, ctx.options.heartbeatIntervalMs), ...globalConfig(storage, agent) };
  return { status: 200, body: { agentId: agent.id, connectToken, config } };
}
