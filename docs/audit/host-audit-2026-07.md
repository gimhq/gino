# Gino Host Daemon — Source Audit (2026-07)

**Date:** 2026-07
**Scope:** `src/**` of the Gino host daemon at the audited commit, cross-checked
against `docs/host-architecture.md` (module map, §3–§8 invariants) and
`docs/protocol.md` (§3–§6 wire contract). Read-only cross-references to
`docs/audit/agent-audit-2026-07.md` where the same pattern appears on both sides.

**Method:** each module (M1/M2 edge + protocol, M3 agent management, M4 task
management & dispatch, M5/M6 message store & channels, M7 credential proxy, M8
config store, M9 platform/db/shared/bootstrap) was audited line-by-line under the
pma-cr repository-audit policy with correctness / security / concurrency /
idempotency / lifecycle / error-handling lenses. Three cross-cutting lenses (UTC
discipline, auth-surface inventory, at-least-once / idempotency end-to-end trace)
were run over all of `src/**`. Findings are graded P0 (critical) → P3 (minor).
Each finding has a stable id `HOST-<module>-<nn>` (`HOST-XC-<nn>` for cross-cutting).
Where a cross-cutting lens re-reports an issue already owned by a module chapter,
the module entry is canonical and the cross-cutting entry is a cross-reference
(not double-counted); those dedupes are noted inline.

---

## Executive Summary

### Totals by grade

| Grade | Count |
|---|---|
| P0 | 5 |
| P1 | 18 |
| P2 | 41 |
| P3 | 27 |
| **Total** | **91** |

Counts reflect the deduplicated finding set: 89 module findings plus 2 unique
cross-cutting findings (both P2). The other 9 cross-cutting entries duplicate
module findings and are cross-referenced, not counted again.

### Findings per module

| Module | P0 | P1 | P2 | P3 |
|---|---|---|---|---|
| M1/M2 — Edge + Protocol Adapter | 1 | 5 | 6 | 5 |
| M3 — Agent Management | 1 | 3 | 8 | 3 |
| M4 — Task Management & Dispatch | 1 | 4 | 4 | 2 |
| M5/M6 — Message Store & Channels | 0 | 2 | 6 | 3 |
| M7 — Credential Proxy + Lode | 1 | 1 | 2 | 3 |
| M8 — Config Store | 1 | 2 | 7 | 4 |
| M9 — Platform / DB / Bootstrap | 0 | 1 | 6 | 7 |
| XC — Cross-cutting (unique) | 0 | 0 | 2 | 0 |
| **Total** | **5** | **18** | **41** | **27** |

### Top P0 findings

| id | title | location |
|---|---|---|
| HOST-M1M2-01 | `ack`/`msg_ack` not scoped to sending agent — any agent can suppress another's undelivered frames | `src/protocol/inbound.ts:155` |
| HOST-M3-01 | `removed` is not terminal — a late WS close resurrects a removed agent as `offline` | `src/agents/registry.ts:124` |
| HOST-M4-01 | Per-task `close` emits the runner-level exit frame, killing every other task on the agent | `src/tasks/orchestrator.ts:203` |
| HOST-M7-01 | Forged `sk-ant-` key bypasses proxy auth and spends the host's OpenAI/Codex credentials | `src/proxy/auth.ts:31` |
| HOST-M8-01 | Global config carries no `version`; the reconcile gate is broken, so global desired-set changes are never applied | `src/configstore/entries.ts:164` |

### Top P1 findings

| id | title | location |
|---|---|---|
| HOST-M1M2-02 | Host mints thread ids and never exercises the documented fresh-thread handshake | `src/tasks/orchestrator.ts:117` |
| HOST-M1M2-03 | `interrupt`/`close` presented as task-scoped but are agent-global control frames | `src/edge/admin/tasks.ts:244` |
| HOST-M1M2-04 | Permanently unprocessable inbound frames are never acked — poison frames replay forever | `src/protocol/inbound.ts:117` |
| HOST-M1M2-05 | Superseded-socket close marks a live reconnected agent `offline` | `src/edge/agent.ts:267` |
| HOST-M1M2-06 | Every `thread_result` recorded with `source:"chat"` and no `cron_id` | `src/protocol/inbound.ts:130` |
| HOST-M3-02 | Reconnect supersede marks a live agent `offline` | `src/agents/sessions.ts:46` |
| HOST-M3-03 | Sweep-marked `offline` is one-way while the socket stays connected | `src/agents/liveness.ts:51` |
| HOST-M3-04 | `removeAgentControl` aborts durable revocation when the launcher op fails | `src/agents/control.ts:127` |
| HOST-M4-02 | Cron created before the first message is silently lost — task never seeded | `src/tasks/orchestrator.ts:150` |
| HOST-M4-03 | `interrupt` is not task-scoped and stale interrupts replay on reconnect | `src/tasks/orchestrator.ts:197` |
| HOST-M4-04 | Thread-identity contract inverted; agent-minted ids never recorded | `src/tasks/orchestrator.ts:117` |
| HOST-M4-05 | Orphaned task re-placed on a new agent, never re-seeded, drags a dead `current_thread_id` | `src/tasks/dispatch.ts:101` |
| HOST-M5M6-01 | Poll egress advances the cursor on read, before the client receives the bytes | `src/channels/egress.ts:215` |
| HOST-M5M6-02 | Webhook SSRF guard bypassable (IPv4-mapped IPv6, `0.0.0.0`, alternate encodings) | `src/channels/webhooks.ts:88` |
| HOST-M7-02 | Streaming Claude→OpenAI bridge never tears down the upstream WS on client abort | `src/proxy/bridge.ts:78` |
| HOST-M8-02 | `setEntries` delete-then-insert is not transactional — a scope's desired set can be wiped | `src/configstore/entries.ts:113` |
| HOST-M8-03 | Global entries filtered to the agent's primary engine only — multi-engine agents lose config | `src/configstore/entries.ts:164` |
| HOST-M9-01 | Graceful shutdown is inverted: `server.stop(true)` runs first, killing all in-flight work | `src/index.ts:57` |

---
