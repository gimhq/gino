# 20261003-1648-structure-docs-agent-alignment Repository structure, documentation, and gino-agent PMA-Rust alignment

- **status**: in_progress
- **priority**: P1
- **owner**: station/session-20261003
- **createdAt**: 2026-10-03 16:48

## Description

Tidy the repository layout, bring the documentation up to date with the post-split
workspace, and align `gino-agent` with the PMA-Rust acceptance baseline.

Acceptance criteria:

- Repository hygiene files exist (`README.md`, `LICENSE`, `.editorconfig`, `.gitattributes`) and the
  runtime versions are pinned in `package.json`.
- `docs/` has an entry point (`docs/architecture.md`), a changelog, and decision records; no document
  refers to the pre-split `src/**` tree as if it were current.
- `gino-agent` passes the PMA-Rust quality gates (fmt, clippy, nextest, doctest, cargo-deny,
  cargo-shear, typos, MSRV) locally and in CI, with every accepted divergence recorded in
  `docs/decisions/`.
- No behavior change to the wire protocol or to the host packages' public API.

## ActiveForm

Aligning repository structure, documentation, and gino-agent with the PMA-Rust baseline

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261003-1650-structure-docs-agent-alignment.md`.

- 2026-10-03: documentation (plan Phase 2) and the comment cleanup are done and verified. Hygiene
  files, agent tooling, runtime hardening and the module split remain; the task stays open.
- Found while verifying, not fixed: `bun run lint` reports 69 errors on `53a3bf6`, and
  `apps/host/tests/unit/host-options.test.ts` fails when run from the repository root.
