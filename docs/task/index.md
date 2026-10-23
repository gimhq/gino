# Gino - Task List

> Updated: 2026-10-12

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
