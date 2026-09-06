// M3 — `/admin/agents` control API (mounted under `/admin`, behind the channel
// token via the parent router). The OOB observe + control surface (§7):
//   GET    /agents                list agents (+ online flag + capacity)
//   GET    /agents/:id            one agent (+ launcher.inspect(ref) + grants)
//   POST   /agents/launch         501 — launcher phase (B2b)
//   POST   /agents/:id/stop       OOB stop  (launcher | WS close)
//   POST   /agents/:id/restart    OOB restart (launcher | WS close)
//   POST   /agents/:id/remove     OOB remove (revoke token + orphan tasks)
//   POST   /agents/:id/grant      grant {owner_id} access  (OD13)
//   POST   /agents/:id/access     set access policy {access} (OD13, HOST-M3-09)
//   POST   /agents/:id/revoke     revoke {owner_id} access (OD13)
//
// Every response uses the shared `{ success, data }` envelope. The stored
// connect-token hash is NEVER serialized.
import type { AgentRow } from "@gino/host-core";
import type { AppEnv } from "@/shared/lib/types";
import { NotFoundError } from "@gino/host-core";
import { getAgent, getCapacity, grantAgentOwner, inspectAgent, launchAgent, listAgents, listGrants, removeAgentControl, restartAgent, revokeAgentOwner, setAgentAccessControl, stopAgent } from "@gino/host-core/domain";
import { Hono } from "hono";
import { z } from "zod";
import { ok } from "@/shared/lib/api-response";
import { describeRoute, errors, jsonOk, TAGS, validator } from "@/shared/lib/openapi";

/** Public projection of an agent — the connect-token hash is never exposed. */
function serializeAgent(agent: AgentRow, online: boolean) {
  return {
    id: agent.id,
    name: agent.name,
    source: agent.source,
    access: agent.access,
    status: agent.status,
    online,
    capabilities: agent.capabilities,
    launcherKind: agent.launcherKind,
    launchRef: agent.launchRef,
    activeThreads: agent.activeThreads,
    lastSeen: agent.lastSeen,
    createdAt: agent.createdAt,
  };
}

const ownerBody = z.object({ owner_id: z.string().min(1) });

/**
 * Launch request body. `kind` selects the launcher (default `docker`);
 * `hostUrl` is where the launched agent dials back to register. The shared
 * register token is NOT accepted here — it is read server-side from config.
 */
const launchBody = z.object({
  kind: z.string().min(1).default("docker"),
  hostUrl: z.string().url(),
  image: z.string().min(1).optional(),
  taskId: z.string().min(1).optional(),
  env: z.record(z.string(), z.string()).optional(),
  resources: z.record(z.string(), z.unknown()).optional(),
  // HOST-M3-09: the launch is an ADMIN request, so `public` may be allowlisted
  // here — applied when the launched agent's registration correlates. The
  // agent's own register body can never set it.
  access: z.enum(["dedicated", "public"]).optional(),
});

const accessBody = z.object({ access: z.enum(["dedicated", "public"]) });

export function adminAgentRoutes() {
  const router = new Hono<AppEnv>();

  // ── List ─────────────────────────────────────────────────────────────────
  router.get(
    "/",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "List agents",
      responses: { ...jsonOk(z.array(z.any()), "Agents"), ...errors(401) },
    }),
    (c) => {
      const host = c.get("host");
      const storage = host.storage;
      const data = listAgents(storage).map(a => ({
        ...serializeAgent(a, host.sessions.isOnline(a.id)),
        capacity: getCapacity(storage, a.id),
      }));
      return c.json(ok(data));
    },
  );

  // ── Launch (B2b — config-driven launcher) ──────────────────────────────────
  // Declared BEFORE `/:id` style routes so `launch` is never read as an `:id`.
  // The shared register token is server-side (config) — never accepted from the
  // client; the launch↔register nonce (OD17) is host-generated in control.
  router.post(
    "/launch",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Launch an agent (config-driven launcher — B2b)",
      responses: { ...jsonOk(z.any(), "Launched (returns launcherKind + nonce + ref)"), ...errors(401, 422) },
    }),
    validator("json", launchBody),
    async (c) => {
      const host = c.get("host");
      const body = c.req.valid("json");
      const result = await launchAgent(
        { storage: host.storage, sender: host.sessions, resolveLauncher: host.launchers.resolve, logger: c.get("logger") },
        {
          kind: body.kind,
          hostUrl: body.hostUrl,
          registerToken: host.options.registerToken,
          ...(body.image ? { image: body.image } : {}),
          ...(body.taskId ? { workload: { taskId: body.taskId } } : {}),
          ...(body.env ? { env: body.env } : {}),
          ...(body.resources ? { resources: body.resources } : {}),
          ...(body.access ? { access: body.access } : {}),
        },
      );
      return c.json(ok(result));
    },
  );

  // ── Get one (+ launcher inspect + grants) ──────────────────────────────────
  router.get(
    "/:id",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Get an agent (+ runtime inspect + grants)",
      responses: { ...jsonOk(z.any(), "Agent"), ...errors(401, 404) },
    }),
    async (c) => {
      const storage = c.get("host").storage;
      const id = c.req.param("id");
      const agent = getAgent(storage, id);
      if (!agent) {
        throw new NotFoundError("agent", id);
      }
      const host = c.get("host");
      const runtime = await inspectAgent({ storage, sender: host.sessions, resolveLauncher: host.launchers.resolve, logger: c.get("logger") }, id);
      return c.json(ok({
        ...serializeAgent(agent, host.sessions.isOnline(id)),
        capacity: getCapacity(storage, id),
        grants: listGrants(storage, id).map(g => g.ownerId),
        runtime,
      }));
    },
  );

  // ── OOB control: stop / restart / remove ──────────────────────────────────
  router.post(
    "/:id/stop",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Stop an agent (OOB — launcher or WS close)",
      responses: { ...jsonOk(z.any(), "Stopped"), ...errors(401, 404, 410) },
    }),
    async (c) => {
      const host = c.get("host");
      const deps = { storage: host.storage, sender: host.sessions, resolveLauncher: host.launchers.resolve, logger: c.get("logger") };
      return c.json(ok(await stopAgent(deps, c.req.param("id"))));
    },
  );

  router.post(
    "/:id/restart",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Restart an agent (OOB — launcher or WS close)",
      responses: { ...jsonOk(z.any(), "Restarted"), ...errors(401, 404, 410) },
    }),
    async (c) => {
      const host = c.get("host");
      const deps = { storage: host.storage, sender: host.sessions, resolveLauncher: host.launchers.resolve, logger: c.get("logger") };
      return c.json(ok(await restartAgent(deps, c.req.param("id"))));
    },
  );

  router.post(
    "/:id/remove",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Remove an agent (OOB — revoke token + orphan tasks; mirror kept)",
      responses: { ...jsonOk(z.any(), "Removed"), ...errors(401, 404) },
    }),
    async (c) => {
      const host = c.get("host");
      const deps = { storage: host.storage, sender: host.sessions, sessions: host.sessions, resolveLauncher: host.launchers.resolve, logger: c.get("logger"), events: host.events };
      return c.json(ok(await removeAgentControl(deps, c.req.param("id"))));
    },
  );

  // ── Access policy: grant / revoke {owner_id} (OD13) ────────────────────────
  router.post(
    "/:id/grant",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Grant an owner access to an agent (OD13)",
      responses: { ...jsonOk(z.any(), "Granted"), ...errors(401, 404, 410, 422) },
    }),
    validator("json", ownerBody),
    (c) => {
      const id = c.req.param("id");
      const { owner_id } = c.req.valid("json");
      grantAgentOwner({ storage: c.get("host").storage, sender: c.get("host").sessions, logger: c.get("logger") }, id, owner_id);
      return c.json(ok({ agentId: id, ownerId: owner_id, granted: true }));
    },
  );

  // HOST-M3-09: making an agent `public` (or back `dedicated`) is an
  // admin-only mutation — enrollment always lands `dedicated` regardless of
  // what the register body claims.
  router.post(
    "/:id/access",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Set an agent's access policy (OD13 — admin-only)",
      responses: { ...jsonOk(z.any(), "Access set"), ...errors(401, 404, 410, 422) },
    }),
    validator("json", accessBody),
    (c) => {
      const id = c.req.param("id");
      const { access } = c.req.valid("json");
      setAgentAccessControl({ storage: c.get("host").storage, sender: c.get("host").sessions, logger: c.get("logger") }, id, access);
      return c.json(ok({ agentId: id, access }));
    },
  );

  router.post(
    "/:id/revoke",
    describeRoute({
      tags: [TAGS.Agents],
      summary: "Revoke an owner's access to an agent (OD13)",
      responses: { ...jsonOk(z.any(), "Revoked"), ...errors(401, 404, 422) },
    }),
    validator("json", ownerBody),
    (c) => {
      const id = c.req.param("id");
      const { owner_id } = c.req.valid("json");
      revokeAgentOwner({ storage: c.get("host").storage, sender: c.get("host").sessions, logger: c.get("logger") }, id, owner_id);
      return c.json(ok({ agentId: id, ownerId: owner_id, granted: false }));
    },
  );

  return router;
}
