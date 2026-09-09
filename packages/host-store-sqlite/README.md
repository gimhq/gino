# @gino/host-store-sqlite

The reference `Storage` adapter for `@gino/host-core`: drizzle over SQLite, with
the schema, the committed migrations and the per-table repositories.

## When you need it

Whenever the host should survive a restart. `memoryStorage()` from host-core is
fine for tests and prototypes; this package is the one you ship. It works under
both runtimes — `bun:sqlite` under Bun, `better-sqlite3` under Node — against the
same schema and the same migrations.

## Entry points

**`@gino/host-store-sqlite`** — the opener, the migrations and the adapter.

```ts
import { openDatabase, sqliteStorage } from "@gino/host-store-sqlite";
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node";

const db = openDatabase({ driver: nodeSqliteDriver, path: "data/gino.db" });
const storage = sqliteStorage(db, { storeDir: "data/store" });
```

`openDatabase` applies the WAL, foreign-key and busy-timeout pragmas and runs
migrations before returning; `:memory:` gives an ephemeral database. Pass
`version` to get a pre-migration snapshot and an on-disk version marker.
`sqliteStorage` without `storeDir` lazily creates a temporary blob directory on
first use, which is what makes `sqliteStorage(db)` cheap in a unit test.

**`/bun` and `/node`** — the two drivers, kept behind subpaths so neither
runtime's binding is loaded by the other.

```ts
import { bunSqliteDriver } from "@gino/host-store-sqlite/bun";   // bun:sqlite
import { nodeSqliteDriver } from "@gino/host-store-sqlite/node"; // better-sqlite3
```

**`/schema`** — the drizzle tables. The single escape hatch, for an embedder
authoring its own migrations against this database.

```ts
import { agents, tasks } from "@gino/host-store-sqlite/schema";
```

## What it does not do

- **No row types cross the barrel.** host-core hand-mirrors them in its ports so
  nothing in the domain depends on drizzle. What crosses is the database handle.
- **No connection pooling, no async.** SQLite is synchronous and so is the whole
  storage port. One host per database file.
- **No encryption at rest.** The host is a credential proxy, not a keystore.
- **No Postgres or libsql.** Those would be sibling adapters implementing the
  same port, verified by the same conformance suite.
