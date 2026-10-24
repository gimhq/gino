# 20261024-0033-dev-state-reset Drop pre-release compatibility layers

- **status**: completed
- **priority**: P2
- **owner**: station/dev-state-reset-20261024-0033
- **createdAt**: 2026-10-24 00:33

## Description

The project is pre-release; nothing deployed needs an upgrade path. Remove the compatibility
layers left after `20261023-2333-ulid-delivery-ids`:

- Squash the host drizzle migrations into one `0000` baseline and the agent schema ladder into
  a single v1 baseline; drop the tests that exist only for upgrading older databases.
- Drop the `v2` protocol-version naming (`InboundFrameV2`, `OutboundFrameV2`, the `v2` module,
  "protocol v2" in names and comments) now that `protocolVersion` is 1.
- Rename the admin API `outboxId` response field to `msgId`.

Acceptance criteria: host `bun run check`, agent fmt/clippy/tests and the docker e2e pass;
existing development databases must be deleted and recreated.

## ActiveForm

Removing pre-release compatibility layers

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Approved in conversation on 2026-10-24 ("按推荐来"); extends plan
`docs/plan/20261023-2333-ulid-delivery-ids.md`.

- complete: Host check, agent unit tests and docker e2e passed.
