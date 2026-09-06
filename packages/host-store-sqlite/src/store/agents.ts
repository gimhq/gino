import type { AgentRepo, AgentRow, NewAgentRow, NewPendingLaunchRow, PendingLaunchRow, StatusGuard } from "@gino/host-core/domain";
// SQLITE STORE — `AgentRepo` on drizzle + bun:sqlite (agents / pending_launches
// / agent_grants). Each method is the one statement the domain issues today
// (`agents/registry.ts`, `agents/liveness.ts`, `proxy/auth.ts`), unchanged.
import type { AppDatabase } from "../db";
import { and, asc, desc, eq, isNotNull, lt, ne, sql } from "drizzle-orm";
import { agentGrants, agents, pendingLaunches } from "../schema";

const REMOVED = "removed";

export function sqliteAgentRepo(db: AppDatabase): AgentRepo {
  return {
    insert(row: NewAgentRow): AgentRow {
      return db.insert(agents).values(row).returning().get();
    },
    get(agentId) {
      return db.select().from(agents).where(eq(agents.id, agentId)).get();
    },
    list() {
      // `rowid` tiebreak: equal `createdAt` keeps insertion order (port ordering rule).
      return db.select().from(agents).orderBy(desc(agents.createdAt), asc(sql`rowid`)).all();
    },
    getByConnectTokenHash(hash) {
      return db
        .select()
        .from(agents)
        .where(and(eq(agents.connectTokenHash, hash), ne(agents.status, REMOVED)))
        .get();
    },
    getByLaunchNonce(nonce) {
      return db
        .select()
        .from(agents)
        .where(and(eq(agents.launchNonce, nonce), ne(agents.status, REMOVED)))
        .get();
    },
    updateStatusIf(agentId, guard: StatusGuard, to) {
      // `removed` is TERMINAL: the guard never lets a row out of it.
      return db
        .update(agents)
        .set({ status: to })
        .where(and(
          eq(agents.id, agentId),
          ne(agents.status, REMOVED),
          ...(guard.from !== undefined ? [eq(agents.status, guard.from)] : []),
          ...(guard.notFrom !== undefined ? [ne(agents.status, guard.notFrom)] : []),
        ))
        .returning({ id: agents.id })
        .get() != null;
    },
    touchLiveness(agentId, now, activeThreads) {
      db
        .update(agents)
        .set({
          lastSeen: now,
          ...(activeThreads != null ? { activeThreads } : {}),
        })
        .where(eq(agents.id, agentId))
        .run();
    },
    setAccess(agentId, access) {
      db
        .update(agents)
        .set({ access })
        .where(and(eq(agents.id, agentId), ne(agents.status, REMOVED)))
        .run();
    },
    setRemoved(agentId, revokedTokenHash) {
      db
        .update(agents)
        .set({ status: REMOVED, connectTokenHash: revokedTokenHash })
        .where(eq(agents.id, agentId))
        .run();
    },
    setConnectTokenHash(agentId, hash) {
      return db
        .update(agents)
        .set({ connectTokenHash: hash })
        .where(eq(agents.id, agentId))
        .returning()
        .get();
    },
    sweepSilent(before) {
      return db
        .update(agents)
        .set({ status: "offline" })
        .where(and(
          eq(agents.status, "online"),
          isNotNull(agents.lastSeen),
          lt(agents.lastSeen, before),
        ))
        .returning({ id: agents.id })
        .all()
        .map(row => row.id);
    },

    grant(agentId, ownerId) {
      db.insert(agentGrants).values({ agentId, ownerId }).onConflictDoNothing().run();
    },
    revoke(agentId, ownerId) {
      db
        .delete(agentGrants)
        .where(and(eq(agentGrants.agentId, agentId), eq(agentGrants.ownerId, ownerId)))
        .run();
    },
    listGrants(agentId) {
      return db.select().from(agentGrants).where(eq(agentGrants.agentId, agentId)).orderBy(asc(sql`rowid`)).all();
    },
    hasGrant(agentId, ownerId) {
      return db
        .select()
        .from(agentGrants)
        .where(and(eq(agentGrants.agentId, agentId), eq(agentGrants.ownerId, ownerId)))
        .get() != null;
    },

    recordPending(row: NewPendingLaunchRow) {
      db.insert(pendingLaunches).values(row).run();
    },
    correlate(agentId, nonce): PendingLaunchRow | undefined {
      // Consume-then-bind in one transaction: a UNIQUE `launchNonce` violation
      // on the bind rolls the consume back, so the pending row stays in place.
      return db.transaction((tx) => {
        const pending = tx
          .delete(pendingLaunches)
          .where(eq(pendingLaunches.nonce, nonce))
          .returning()
          .get();
        if (!pending) {
          return undefined;
        }
        tx
          .update(agents)
          .set({
            launcherKind: pending.launcherKind,
            launchRef: pending.launchRef,
            launchNonce: nonce,
            access: pending.access,
          })
          .where(eq(agents.id, agentId))
          .run();
        return pending;
      });
    },
    hasPending(nonce) {
      return db.select({ nonce: pendingLaunches.nonce }).from(pendingLaunches).where(eq(pendingLaunches.nonce, nonce)).get() != null;
    },
    listExpiredPending(before) {
      return db
        .select()
        .from(pendingLaunches)
        .where(lt(pendingLaunches.createdAt, before))
        .orderBy(asc(sql`rowid`))
        .all();
    },
    deletePending(nonce) {
      db.delete(pendingLaunches).where(eq(pendingLaunches.nonce, nonce)).run();
    },
  };
}
