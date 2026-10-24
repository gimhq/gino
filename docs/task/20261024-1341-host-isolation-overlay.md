# 20261024-1341-host-isolation-overlay Host-delivered engine isolation overlay

- **status**: pending
- **priority**: P2
- **owner**: (unassigned)
- **createdAt**: 2026-10-24 13:41

## Description

The host never sends an `engine` overlay (`packages/host-core/src/host/wire/config.ts`
`globalConfig`), so it cannot set the engine sandbox. The agent's local isolation file
(`GINO_ISOLATION_FILE`, task `20261024-0058-engine-sandbox`) is the floor; the host may only
tighten it (decision of 2026-10-24, BKD message 03yz9ib5, option "both").

Add `engine.isolation` (mode and network mode only) to the host's global agent config, with an
admin route to set it, delivered on register / `GET /agent/config` / WS config updates, and
documented in docs/protocol.md as an additive field. The agent already ignores loosening values.

Acceptance criteria: an operator sets a stricter isolation on the host and agents apply it on the
next config delivery; a looser value is ignored by the agent with a warning; host check, agent
gates and the docker e2e pass.

## ActiveForm

Delivering engine isolation from the host

## Dependencies

- **blocked by**: 20261024-0058-engine-sandbox
- **blocks**: (none)
