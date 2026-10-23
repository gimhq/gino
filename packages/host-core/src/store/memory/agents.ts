import type { AgentGrantRow, AgentRepo, AgentRow, NewAgentRow, NewPendingLaunchRow, PendingLaunchRow, StatusGuard } from "../../host/ports/storage";
import { newestFirst, uniqueViolation } from "./shared";

const REMOVED = "removed";

export function memoryAgentRepo(): AgentRepo {
  const agents = new Map<string, AgentRow>();
  const pending = new Map<string, PendingLaunchRow>();
  const grants: AgentGrantRow[] = [];

  const nonceTaken = (nonce: string, exceptId?: string): boolean =>
    [...agents.values()].some(a => a.launchNonce === nonce && a.id !== exceptId);

  const guardHolds = (status: string, guard: StatusGuard): boolean =>
    (guard.from === undefined || status === guard.from)
    && (guard.notFrom === undefined || status !== guard.notFrom);

  return {
    insert(row: NewAgentRow): AgentRow {
      if (agents.has(row.id)) {
        throw uniqueViolation("agents.id");
      }
      if (row.launchNonce != null && nonceTaken(row.launchNonce)) {
        throw uniqueViolation("agents.launch_nonce");
      }
      const stored: AgentRow = {
        id: row.id,
        name: row.name ?? null,
        source: row.source,
        access: row.access,
        connectTokenHash: row.connectTokenHash,
        registerTokenRef: row.registerTokenRef ?? null,
        capabilities: row.capabilities,
        status: row.status,
        launcherKind: row.launcherKind ?? null,
        launchRef: row.launchRef ?? null,
        launchNonce: row.launchNonce ?? null,
        activeThreads: row.activeThreads === undefined ? 0 : row.activeThreads,
        lastSeen: row.lastSeen ?? null,
        createdAt: row.createdAt ?? Date.now(),
      };
      agents.set(stored.id, stored);
      return { ...stored };
    },
    get(agentId) {
      const row = agents.get(agentId);
      return row ? { ...row } : undefined;
    },
    list() {
      return newestFirst([...agents.values()], a => a.createdAt).map(a => ({ ...a }));
    },
    getByConnectTokenHash(hash) {
      const row = [...agents.values()].find(a => a.connectTokenHash === hash && a.status !== REMOVED);
      return row ? { ...row } : undefined;
    },
    getByLaunchNonce(nonce) {
      const row = [...agents.values()].find(a => a.launchNonce === nonce && a.status !== REMOVED);
      return row ? { ...row } : undefined;
    },
    updateStatusIf(agentId, guard, to) {
      const row = agents.get(agentId);
      if (!row || row.status === REMOVED || !guardHolds(row.status, guard)) {
        return false;
      }
      row.status = to;
      return true;
    },
    touchLiveness(agentId, now, activeThreads) {
      const row = agents.get(agentId);
      if (!row) {
        return;
      }
      row.lastSeen = now;
      if (activeThreads != null) {
        row.activeThreads = activeThreads;
      }
    },
    setAccess(agentId, access) {
      const row = agents.get(agentId);
      if (row && row.status !== REMOVED) {
        row.access = access;
      }
    },
    setCapabilities(agentId, capabilities) {
      const row = agents.get(agentId);
      if (row && row.status !== REMOVED) {
        row.capabilities = capabilities;
      }
    },
    setRemoved(agentId, revokedTokenHash) {
      const row = agents.get(agentId);
      if (row) {
        row.status = REMOVED;
        row.connectTokenHash = revokedTokenHash;
      }
    },
    setConnectTokenHash(agentId, hash) {
      const row = agents.get(agentId);
      if (!row) {
        return undefined;
      }
      row.connectTokenHash = hash;
      return { ...row };
    },
    sweepSilent(before) {
      const ids: string[] = [];
      for (const row of agents.values()) {
        if (row.status === "online" && row.lastSeen != null && row.lastSeen < before) {
          row.status = "offline";
          ids.push(row.id);
        }
      }
      return ids;
    },

    grant(agentId, ownerId) {
      if (!grants.some(g => g.agentId === agentId && g.ownerId === ownerId)) {
        grants.push({ agentId, ownerId, createdAt: Date.now() });
      }
    },
    revoke(agentId, ownerId) {
      const at = grants.findIndex(g => g.agentId === agentId && g.ownerId === ownerId);
      if (at >= 0) {
        grants.splice(at, 1);
      }
    },
    listGrants(agentId) {
      return grants.filter(g => g.agentId === agentId).map(g => ({ ...g }));
    },
    hasGrant(agentId, ownerId) {
      return grants.some(g => g.agentId === agentId && g.ownerId === ownerId);
    },

    recordPending(row: NewPendingLaunchRow) {
      if (pending.has(row.nonce)) {
        throw uniqueViolation("pending_launches.nonce");
      }
      pending.set(row.nonce, {
        nonce: row.nonce,
        launcherKind: row.launcherKind,
        launchRef: row.launchRef ?? null,
        access: row.access ?? "dedicated",
        createdAt: row.createdAt ?? Date.now(),
      });
    },
    correlate(agentId, nonce) {
      const launch = pending.get(nonce);
      if (!launch) {
        return undefined;
      }
      if (nonceTaken(nonce, agentId)) {
        throw uniqueViolation("agents.launch_nonce");
      }
      pending.delete(nonce);
      const row = agents.get(agentId);
      if (row) {
        row.launcherKind = launch.launcherKind;
        row.launchRef = launch.launchRef;
        row.launchNonce = nonce;
        row.access = launch.access;
      }
      return { ...launch };
    },
    hasPending(nonce) {
      return pending.has(nonce);
    },
    listExpiredPending(before) {
      return [...pending.values()].filter(p => p.createdAt < before).map(p => ({ ...p }));
    },
    deletePending(nonce) {
      pending.delete(nonce);
    },
  };
}
