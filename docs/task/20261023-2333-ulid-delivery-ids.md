# 20261023-2333-ulid-delivery-ids ULID delivery ids for host and agent outboxes

- **status**: completed
- **priority**: P2
- **owner**: station/ulid-delivery-ids-20261023-2333
- **createdAt**: 2026-10-23 23:33

## Description

The delivery ids on the wire are SQLite integer autoincrement row ids: `msgId` (host outbox,
acked by `msg_ack`) and `outboxId` (agent outbox, acked by `ack{id}`). Each receiver dedups on
the id, so a sender database that is recreated or restored from a backup reissues ids the
receiver has already seen, and new frames are acked and dropped without an error. The project
is pre-release, so switch both ids to ULID strings without a compatibility path.

Acceptance criteria:

- Host outbox rows and agent outbox rows are keyed by a monotonic ULID; replay order is
  unchanged (oldest first).
- `msgId`, `outboxId`, `msg_ack.msgId` and `ack.id` are ULID strings on the wire; the frame
  schemas reject integers.
- A receiver that has seen ids from one sender database accepts frames from a recreated one.
- Host `check`, agent gates and the docker e2e pass.

## ActiveForm

Switching delivery ids to ULID

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261023-2333-ulid-delivery-ids.md`.

- complete: Host check, agent unit tests and docker e2e passed; two pre-existing flaky restart integration tests unchanged.
