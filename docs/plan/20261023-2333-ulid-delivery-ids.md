# 20261023-2333-ulid-delivery-ids ULID delivery ids for host and agent outboxes

- **status**: completed
- **createdAt**: 2026-10-23 23:33
- **approvedAt**: 2026-10-23 23:50
- **relatedTask**: 20261023-2333-ulid-delivery-ids

## Context

Two delivery ids cross the wire, both SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` row ids:

| Direction | Id | Source | Ack | Receiver dedup |
|---|---|---|---|---|
| host → agent | `msgId` | host `outbox.id` | `msg_ack{msgId}` | agent `inbox.msg_id` UNIQUE (`gino-agent/src/db.rs:262`) |
| agent → host | `outboxId` | agent `outbox.id` | `ack{id}` | none; host writes are idempotent by `(threadId, seq)` |

- Non-`message` host → agent frames also carry `outboxId` (same row id) "for compatibility";
  the agent never acks it (`docs/protocol.md` §4.2). The host still accepts an agent-sent
  `ack{id}` for host frames (`frames.ts` `ack (agent)`), which the agent never sends.
- Replay is `ORDER BY id` on both sides (`packages/host-store-sqlite/src/store/outbox.ts:19`,
  `gino-agent/src/db.rs:816`).
- Failure mode: a recreated or restored host database restarts `outbox.id`; the agent finds
  the new `msgId` in `inbox`, acks it and drops the frame silently. `AUTOINCREMENT` only
  prevents reuse inside one database file.
- The host already has a monotonic ULID generator (`packages/host-core/src/shared/lib/id.ts`),
  used for every other host string PK. The agent has `uuid` (v4) but no ULID; the `ulid`
  crate's latest stable is 3.0.0 (crates.io, 2026-07-16).
- `DeliveryId` is one zod schema shared by `outboxId`, `msgId` and `ack.id`
  (`packages/host-core/src/protocol/frames.ts:32`); `docs/protocol.md` §8 is generated from it.
- Agent schema is versioned by `PRAGMA user_version` (`SCHEMA_VERSION = 4`); host uses
  embedded drizzle migrations (latest `0006`).
- `scripts/mini-host.ts` is a standalone protocol-v2 host guarded by a test.
- Pre-release: no deployed hosts or agents, so no compatibility path is needed.

## Proposal

1. **Wire contract**
   - `DeliveryId` becomes a ULID string (26 chars, Crockford base32). `msgId`, `outboxId`,
     `msg_ack.msgId` and `ack.id` all use it; integers are rejected.
   - Host → agent frames carry `msgId` only; drop the compatibility `outboxId` and the
     unused agent → host `ack` for host frames.
   - Set `protocolVersion` to 1 on both sides (pre-release reset, no version history kept).
   - Update `docs/protocol.md` §4.2, §5, §6 and regenerate §8.
2. **Host**
   - `outbox.id` becomes `text` PK set with `ulid()` at enqueue; `OutboxRow.id`,
     `markAcked(id)`, `ackMessage(msgId)` and `EnqueuedOutbox.msgId` become `string`.
   - Memory adapter and storage conformance suite follow.
   - New migration `0007`: drop and recreate `outbox` (pending rows are discarded; there is
     no deployment to preserve).
   - `encodeOutbox` stops stamping `outboxId`; inbound `ack` from the agent is no longer
     handled for host frames.
   - `scripts/mini-host.ts` and the e2e tester follow.
3. **Agent**
   - Add `ulid = "3"` and mint outbox ids with a process-wide monotonic generator.
   - `SCHEMA_VERSION` 4 → 5: recreate `outbox` with `id TEXT PRIMARY KEY` and `inbox` with
     `msg_id TEXT` (UNIQUE kept); pending rows are discarded. The inbox's own local row id
     stays an integer (never on the wire).
   - `MsgAck { msg_id: String }`, `Ack { id: String }`, inbound `msg_id: String`;
     `stub_host.rs` and the integration tests follow.
4. **Tests (RED first)**
   - Frame contract tests: ULID accepted, integer rejected, for each delivery id.
   - Host outbox: replay order of ULIDs minted in the same millisecond is insertion order.
   - Agent: an inbox that holds `msgId` A from one host DB accepts a different ULID after
     the host DB is recreated (the regression this change exists for); outbox replay order.
   - Docker e2e asserts ULID-shaped ids on the wire.

## Risks

- **Ordering across processes**: ULIDs are monotonic per process. Two host processes
  writing one outbox (lode cutover overlap) order by millisecond only; same-millisecond
  rows from different processes may interleave. Per-thread order still comes from `seq`
  (§6), so this affects only cross-thread delivery order, which the protocol does not
  promise.
- **Clock steps**: the generator holds the last timestamp on backward steps, so order holds
  in-process; a restart after a large backward step could mint ids lower than unacked
  ones. Acceptable for a dev-stage project; noted in `docs/protocol.md`.
- **Breadth**: about 40 host files (mostly test fixtures with integer ids) and 8 agent files.
  The type change makes the compiler and zod find the call sites.
- **Migrations**: dropping `outbox`/`inbox` rows on upgrade loses in-flight frames on any
  existing dev database; documented in the changelog.

## Scope

- `packages/host-core` (protocol, outbox, storage port, memory adapter, tests),
  `packages/host-store-sqlite` (schema, migration 0007, store, tests), `packages/host-hono`,
  `packages/host-channels`, `apps/host` tests, `scripts/mini-host.ts`, `e2e/e2e.ts`.
- `gino-agent` (`Cargo.toml`, `db.rs`, `protocol.rs`, `transport/{mod,ws}.rs`, `chat.rs`,
  tests, `stub_host.rs`).
- `docs/protocol.md`, `docs/agent-architecture.md`, `docs/host-architecture.md`, changelog.
- Estimated diff: a few hundred lines, most of it fixture ids.

## Alternatives

- **Keep integer ids, add a host database instance id** sent at register; the agent resets
  its dedup when it changes. Smaller diff and keeps cheap integer ordering, but adds a new
  concept and fixes only the host → agent direction.
- **UUIDv7 instead of ULID**: the agent already depends on `uuid`, but the host mints ULIDs
  everywhere else; one id format across both sides is simpler to read in logs.
- **Bump protocol version to 3**: rejected; pre-release, so the version restarts at 1.

## Annotations

- 2026-10-23: approved. Unify both directions on ULID and drop the compatibility `outboxId`
  and agent-sent `ack`. Do not bump the protocol version; set `protocolVersion` to 1.
