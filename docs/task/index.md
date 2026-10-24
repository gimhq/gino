# Gino - Task List

> Updated: 2026-10-24

## Usage

Each task is a single line linking to its detail file. All detailed information lives in `docs/task/<timestamp>-<feature-slug>.md`.

### Format

- [ ] [**20260907-1428-add-endpoint Add endpoint**](20260907-1428-add-endpoint.md) `P1`

### Status Markers

| Marker | Meaning |
|--------|---------|
| `[ ]`  | Pending |
| `[-]`  | In progress |
| `[x]`  | Completed |
| `[~]`  | Closed / Won't do |
| `[d]`  | Deleted detail file; index entry retained |

### Priority: P0 (blocking) > P1 (high) > P2 (medium) > P3 (low)

### Rules

- Only update the checkbox marker; never delete the line or change its other content. If the detail file is deleted, mark the entry `[d]`.
- Record change history and deletion reasons in `docs/changelog.md`; update affected dependency and plan references.
- New tasks append to the end.
- See each `<timestamp>-<feature-slug>.md` for full details, except `[d]` entries whose files have been deleted; consult `docs/changelog.md` for their history.

---

## Tasks

- [-] [**20261003-1648-structure-docs-agent-alignment Repository structure, documentation, and gino-agent PMA-Rust alignment**](20261003-1648-structure-docs-agent-alignment.md) `P1`
- [-] [**20261003-1725-agent-direct-ops Agent file and environment operations without an engine**](20261003-1725-agent-direct-ops.md) `P1`

- [x] [**20261012-1616-agent-pty Agent PTY sessions over the live operations channel**](20261012-1616-agent-pty.md) `P1`
- [x] [**20261012-2019-remote-ops-review-fixes Fix remote operations review findings**](20261012-2019-remote-ops-review-fixes.md) `P1`
- [ ] [**20261012-2024-pty-flow-control Terminal flow control**](20261012-2024-pty-flow-control.md) `P2`
- [x] [**20261013-1955-engine-turn-end Engine turn-end detection for claude and codex**](20261013-1955-engine-turn-end.md) `P1`
- [x] [**20261023-2333-ulid-delivery-ids ULID delivery ids for host and agent outboxes**](20261023-2333-ulid-delivery-ids.md) `P2`
- [x] [**20261024-0033-dev-state-reset Drop pre-release compatibility layers**](20261024-0033-dev-state-reset.md) `P2`
- [x] [**20261024-0123-agent-tls-stack Single aws-lc-rs TLS stack on current client crates**](20261024-0123-agent-tls-stack.md) `P2`
- [x] [**20261024-0123-agent-baseline-gaps Close the pma-rust baseline gaps in gino-agent**](20261024-0123-agent-baseline-gaps.md) `P2`
- [x] [**20261024-0245-signed-release-assets Signed lode release assets, agent as tar.gz**](20261024-0245-signed-release-assets.md) `P2`
- [x] [**20261024-0058-engine-sandbox Filesystem and network restrictions for engine runs**](20261024-0058-engine-sandbox.md) `P2`
- [ ] [**20261024-0130-engine-vm-isolation VM isolation backend for engine runs**](20261024-0130-engine-vm-isolation.md) `P2`
- [ ] [**20261024-1341-host-isolation-overlay Host-delivered engine isolation overlay**](20261024-1341-host-isolation-overlay.md) `P2`
- [ ] [**20261024-1527-sandbox-followups Engine sandbox follow-ups**](20261024-1527-sandbox-followups.md) `P1`
- [x] [**20261024-1758-agent-local-api Local agent API socket for in-engine tooling**](20261024-1758-agent-local-api.md) `P1`
- [ ] [**20261024-1855-host-call Forward in-engine host calls over the agent socket**](20261024-1855-host-call.md) `P1`
