import type { ConfigArtifactRow, ConfigRepo, NewConfigArtifactRow } from "@gino/host-core/domain";
// SQLITE STORE — `ConfigRepo` on drizzle + sqlite (config_artifacts /
// config_entries / the `router_state` global config version). Each method is
// the one statement the domain issues today (`configstore/artifacts.ts`,
// `configstore/entries.ts`, `configstore/gc.ts`), unchanged — `replaceScope`
// keeps `setEntries`' single transaction, the one atomic multi-row unit the
// port allows.
import type { AppDatabase } from "../driver";
import { and, asc, desc, eq, sql } from "drizzle-orm";
import { configArtifacts, configEntries, routerState } from "../schema";

/** The per-agent (non-task) scope name. */
const GLOBAL_SCOPE = "global";

/** `router_state` key holding the monotonic GLOBAL config version. */
const GLOBAL_CONFIG_VERSION_KEY = "global_config_version";

/** Either the root handle or the `replaceScope` transaction handle. */
type Executor = Pick<AppDatabase, "select">;

function readGlobalVersion(tx: Executor): number {
  const row = tx.select().from(routerState).where(eq(routerState.key, GLOBAL_CONFIG_VERSION_KEY)).get();
  const parsed = row ? Number.parseInt(row.value, 10) : 0;
  return Number.isFinite(parsed) ? parsed : 0;
}

export function sqliteConfigRepo(db: AppDatabase): ConfigRepo {
  const artifactKey = (name: string, sha256: string) =>
    and(eq(configArtifacts.name, name), eq(configArtifacts.sha256, sha256));

  return {
    upsertArtifact(row: NewConfigArtifactRow): ConfigArtifactRow {
      // The content address is immutable; the metadata OUTSIDE it — `kind` /
      // `content_type` — is last-write-wins (HOST-M8-13).
      db
        .insert(configArtifacts)
        .values(row)
        .onConflictDoUpdate({
          target: [configArtifacts.name, configArtifacts.sha256],
          set: { kind: row.kind, contentType: row.contentType ?? null },
        })
        .run();
      return this.getArtifact(row.name, row.sha256)!;
    },
    getArtifact(name, sha256) {
      return db
        .select()
        .from(configArtifacts)
        .where(artifactKey(name, sha256))
        .get();
    },
    getArtifactBySha(sha256) {
      // Newest first; among equal timestamps the LATEST inserted wins.
      return db
        .select()
        .from(configArtifacts)
        .where(eq(configArtifacts.sha256, sha256))
        .orderBy(desc(configArtifacts.createdAt), desc(sql`rowid`))
        .get();
    },
    listArtifacts() {
      return db.select().from(configArtifacts).orderBy(desc(configArtifacts.createdAt), asc(sql`rowid`)).all();
    },
    setUnreferencedSince(name, sha256, value) {
      db
        .update(configArtifacts)
        .set({ unreferencedSince: value })
        .where(artifactKey(name, sha256))
        .run();
    },
    deleteArtifact(name, sha256) {
      // Row FIRST (HOST-M8-12): the registry never points at deleted bytes.
      db
        .delete(configArtifacts)
        .where(artifactKey(name, sha256))
        .run();
    },

    replaceScope(scope, next) {
      // The delete+insert pair runs in ONE transaction — a crash or insert
      // error can never leave the scope wiped (all-or-nothing). A GLOBAL write
      // also bumps the monotonic config version in the same transaction, so
      // the version advances iff the set actually changed durably.
      return db.transaction((tx) => {
        tx.delete(configEntries).where(eq(configEntries.scope, scope)).run();
        if (next.length > 0) {
          tx
            .insert(configEntries)
            .values(next.map(e => ({ scope, name: e.name, sha256: e.sha256, target: e.target, kind: e.kind })))
            .run();
        }
        if (scope !== GLOBAL_SCOPE) {
          return readGlobalVersion(tx);
        }
        const bumped = readGlobalVersion(tx) + 1;
        const value = String(bumped);
        tx
          .insert(routerState)
          .values({ key: GLOBAL_CONFIG_VERSION_KEY, value })
          .onConflictDoUpdate({ target: routerState.key, set: { value } })
          .run();
        return bumped;
      });
    },
    entriesFor(scope) {
      return db.select().from(configEntries).where(eq(configEntries.scope, scope)).orderBy(asc(sql`rowid`)).all();
    },
    listAllEntries() {
      return db.select().from(configEntries).orderBy(asc(sql`rowid`)).all();
    },
    clearScope(scope) {
      // HOST-M8-10: task scopes carry no config version, so nothing is bumped.
      db.delete(configEntries).where(eq(configEntries.scope, scope)).run();
    },
    scopesWithSha(sha256) {
      return db
        .select({ scope: configEntries.scope })
        .from(configEntries)
        .where(eq(configEntries.sha256, sha256))
        .groupBy(configEntries.scope)
        .orderBy(asc(sql`min(rowid)`))
        .all()
        .map(row => row.scope);
    },
    globalVersion() {
      return readGlobalVersion(db);
    },
  };
}
