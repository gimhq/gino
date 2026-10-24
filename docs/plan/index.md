# Gino - Plan Index

> Updated: 2026-10-24

## Usage

Each plan is a single line linking to its detail file. All detailed information lives in `docs/plan/<timestamp>-<feature-slug>.md`.

### Format

- [ ] [**20260907-1440-add-endpoint Add endpoint**](20260907-1440-add-endpoint.md) `YYYY-MM-DD`

### Status Markers

| Marker | Meaning |
|--------|---------|
| `[ ]`  | Draft / Pending review |
| `[-]`  | Approved / Implementing |
| `[x]`  | Completed |
| `[~]`  | Rejected / Abandoned |
| `[d]`  | Deleted detail file; index entry retained |

### Rules

- Only update the checkbox marker; never delete the line or change its other content. If the detail file is deleted, mark the entry `[d]`.
- Record change history and deletion reasons in `docs/changelog.md`; update affected task and plan references.
- New plans append to the end.
- See each `<timestamp>-<feature-slug>.md` for full details, except `[d]` entries whose files have been deleted; consult `docs/changelog.md` for their history.

---

## Plans

- [-] [**20261003-1650-structure-docs-agent-alignment Repository structure, documentation, and gino-agent PMA-Rust alignment**](20261003-1650-structure-docs-agent-alignment.md) `2026-10-03`
- [-] [**20261003-1726-agent-direct-ops Agent file and environment operations without an engine**](20261003-1726-agent-direct-ops.md) `2026-10-03`

- [x] [**20261012-1616-agent-pty Agent PTY sessions over the live operations channel**](20261012-1616-agent-pty.md) `2026-10-12`
- [x] [**20261012-2019-remote-ops-review-fixes Fix remote operations review findings**](20261012-2019-remote-ops-review-fixes.md) `2026-10-12`
- [x] [**20261013-1955-engine-turn-end Engine turn-end detection for claude and codex**](20261013-1955-engine-turn-end.md) `2026-10-13`
- [x] [**20261023-2333-ulid-delivery-ids ULID delivery ids for host and agent outboxes**](20261023-2333-ulid-delivery-ids.md) `2026-10-23`
- [x] [**20261024-0123-agent-baseline Agent TLS stack and pma-rust baseline gaps**](20261024-0123-agent-baseline.md) `2026-10-24`
- [x] [**20261024-0058-engine-sandbox Filesystem and network restrictions for engine runs**](20261024-0058-engine-sandbox.md) `2026-10-24`
- [x] [**20261024-1758-agent-local-api Local agent API socket for in-engine tooling**](20261024-1758-agent-local-api.md) `2026-10-24`
- [ ] [**20261024-2050-engine-vm-isolation VM isolation backend for engine runs**](20261024-2050-engine-vm-isolation.md) `2026-10-24`
