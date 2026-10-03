# 20261003-1650-structure-docs-agent-alignment Repository structure, documentation, and gino-agent PMA-Rust alignment

- **status**: implementing
- **createdAt**: 2026-10-03 16:50
- **approvedAt**: 2026-10-03 17:05
- **relatedTask**: 20261003-1648-structure-docs-agent-alignment

## Context

Measured on `main` at `53a3bf6`. Nothing was built or run during investigation.

### Repository hygiene

| item | state |
|---|---|
| `README.md` (root) | missing; the only entry points are `docs/host-architecture.md` and `docs/agent-architecture.md` |
| `LICENSE` | missing; six `package.json` files declare `UNLICENSED`, `gino-agent/Cargo.toml` declares nothing |
| `.editorconfig`, `.gitattributes` | missing |
| runtime pin | Bun `1.4.0` is pinned only in the workflows; no `engines` in `package.json` |
| `.gitignore` | present; lacks `tmp/`, `*.key`, `*.pem` |
| PMA tracking | `docs/task/` and `docs/plan/` created by this plan; `docs/architecture.md`, `docs/changelog.md`, `docs/decisions/` missing |

### Documentation

- `docs/host-architecture.md` does not list `packages/host-serve` (added in `cdacd8e`).
- `.env.example` points at `src/config/schema.ts`; the file is `apps/host/src/config/schema.ts`.
- `docs/host-sdk-design.md` is titled a proposal, but P0–P2 are on `main`. Its P0–P3 status notes are
  four paragraphs of merge-by-merge history (lines 258–264), and its §2 prose names pre-split paths.
- `docs/audit/*.md` list findings with no record of which were fixed.
- `gino-agent` and `apps/host` have no README; the six packages and the example do.
- Rust sources, `Cargo.toml`, and `deny.toml` cite `PLAN-001`…`PLAN-007`, plan files that are not in
  the repository (about 180 references).

### gino-agent against the PMA-Rust baseline

| lock / checklist item | state | gap |
|---|---|---|
| 1 mature crates | ok | `Cargo.toml` comments justify C dependencies under a superseded "pure Rust" rule |
| 2 rustls + aws-lc-rs | ok | `deny.toml` labels the ban "Lock 1"; `reqwest` is held at 0.12 to avoid a CDLA-Permissive-2.0 crate that the baseline allow-list now accepts |
| 3 `forbid(unsafe_code)` | ok | — |
| 4 deny warnings in manifest | ok | `nursery` and `cargo` groups are off with an inline reason only; `print_stdout`/`print_stderr` are not denied (41 call sites in `gino_cli.rs` and `login.rs`) |
| 5 MSRV | partial | `rust-version = 1.96.0` equals the toolchain pin, so the normal CI job is the MSRV check; this is not written down |
| 6 no unwrap/expect/panic | ok | — |
| 7 edition 2024 | ok | — |
| 8 quality gates | partial | CI runs fmt, clippy, `cargo test`, deny, shear. Missing: nextest, typos, `cargo doc`. No `rustfmt.toml`, `.config/nextest.toml`, or `justfile` |
| core dumps suppressed | missing | no `rlimit` call |
| panic hook | missing | no `std::panic::set_hook` |
| JSON logs in production | missing | `init_tracing` is text-only; the `json` feature of `tracing-subscriber` is not enabled |
| graceful shutdown | ok | SIGINT and SIGTERM handled in `runner.rs` |
| secrets | ok | `secrecy` in use |
| typed errors | ok | `thiserror` per module; `anyhow` in `lib.rs`, `gino_cli.rs`, `login.rs`, `runner.rs` (entry-point code) |
| release profiles | missing | no `[profile.release]` or `[profile.dist]`; `release.yml` builds `--release` and passes `+crt-static` through `RUSTFLAGS` |
| signed release artifacts | missing | sha256 only |
| blocking work off the runtime | partial | `Mutex<Db>` over rusqlite and `std::fs` are used in async code; `spawn_blocking` appears in two files |
| module size (~600 lines) | exceeded | `db.rs` 3375, `scheduler.rs` 2303, `runner.rs` 1677, `executor.rs` 1508, `config.rs` 1450, `attachments.rs` 1432, `transport/ws.rs` 1234, `protocol.rs` 1233, `transport/mod.rs` 1133; roughly half of each is an inline `mod tests` |
| `/livez`, `/readyz`, layered file config | n/a | the agent is an outbound-only daemon whose config is host-assigned |

## Proposal

Four phases, each one commit-sized and independently verifiable. No wire or public-API change.

### Phase 1 — repository hygiene and structure

1. Add `.editorconfig` (2-space TS/JSON/YAML/MD, 4-space Rust, LF, final newline) and `.gitattributes`
   (`* text=auto eol=lf`; `bun.lock`, `gino-agent/Cargo.lock`,
   `packages/host-store-sqlite/src/embedded-migrations.ts`, and `docs/agent-data-model.svg` marked
   `linguist-generated`).
2. Add `LICENSE` stating "All rights reserved" to match the `UNLICENSED` manifests. Cargo has no
   `UNLICENSED` identifier, so `gino-agent/Cargo.toml` gets `license-file = "../LICENSE"`.
3. Add `engines.bun` to the root `package.json`; extend `.gitignore` with `tmp/`, `*.key`, `*.pem`.
4. Keep the top-level layout (`packages/`, `apps/`, `examples/`, `gino-agent/`, `e2e/`). Moving
   `gino-agent/` would touch 17 files plus both workflows for no functional gain.

### Phase 2 — documentation

1. Root `README.md`: what gino is, the two planes, a layout table, quick start for host and agent,
   the quality-gate commands, and links into `docs/`.
2. `docs/architecture.md`: a one-page index over `host-architecture.md`, `agent-architecture.md`,
   `protocol.md`, the two design documents, `audit/`, and `multica/`.
3. `docs/changelog.md`: initialized, with the P0–P3 SDK history moved out of `host-sdk-design.md`.
   That document keeps a short as-built status table.
4. `docs/host-architecture.md`: add `packages/host-serve` to the layout and to M1.
5. Fix stale paths in `.env.example`, `docs/protocol.md`, `packages/host-core/README.md`, and
   `examples/embed-node/README.md`. `host-sdk-design.md` §2 stays as the labelled pre-split baseline.
6. `gino-agent/README.md` (build, run, subcommands, bootstrap environment, quality gates) and
   `apps/host/README.md` (run, configuration, CLI, packaging).
7. `docs/decisions/` with one record per accepted divergence from Phase 3.

### Phase 3 — gino-agent tooling and manifest alignment

1. `rustfmt.toml` (`style_edition = "2024"`), `.config/nextest.toml`, `_typos.toml`, and a `justfile`
   whose `ci` recipe is the single gate command.
2. `Cargo.toml`: add `print_stdout`/`print_stderr = "deny"` with a scoped `allow` on `gino_cli` and
   `login`; add `[profile.release]` (`lto`, `codegen-units = 1`, `debug = "line-tables-only"`) and
   `[profile.dist]` (`opt-level = "z"`, `strip`); shorten comments to what each dependency is for.
3. `.cargo/config.toml`: `+crt-static` for `x86_64-unknown-linux-musl`, replacing the `RUSTFLAGS`
   environment in `release.yml`, which then builds with `--profile dist`.
4. `deny.toml`: `[licenses] version = 2`, correct the lock labels, drop the plan references.
5. CI `agent` job: `cargo nextest run` plus `cargo test --doc`, `cargo doc --no-deps`, and typos;
   switch caching to `Swatinem/rust-cache`.
6. Decision records: `nursery`/`cargo` lint groups off; MSRV equal to the toolchain pin; `dist` keeps
   `panic = "unwind"` so `kill_on_drop` still reaps the engine child when a task panics.

### Phase 4 — gino-agent runtime hardening and module split

1. `lib.rs`: suppress core dumps with `rlimit` (0.11.0, verified on crates.io) and install a panic
   hook that logs through `tracing` before the default hook runs. Test first.
2. Logging: `GINO_LOG_FORMAT=json|text` (default `text`), enabling the `json` feature of
   `tracing-subscriber`. Test first.
3. Move-only split of oversized modules, one commit each, tests green before and after:
   inline `mod tests` blocks move to sibling `tests.rs` files (the pattern `executor/tests.rs` and
   `runner/tests.rs` already use), and `db.rs` becomes `db/` with one file per table group.

### Follow-up tasks (recorded, not done here)

- Verify each finding in `docs/audit/*` against current source and annotate its status.
- Move rusqlite and filesystem work onto `spawn_blocking` (audit P2-15).
- Upgrade `reqwest` to 0.13 and allow `CDLA-Permissive-2.0`.
- Sign release artifacts.
- Remove the remaining `PLAN-00x` references from Rust source comments.

## Risks

- **Module split** is the largest diff. Mitigation: move-only commits, no signature changes, the full
  test suite and the docker e2e before and after.
- **`print_stdout` deny** can surface call sites outside the two CLI modules; each gets a scoped allow
  or becomes a `tracing` call.
- **`[profile.release]` with fat LTO** lengthens CI clippy and test runs, which use `--release`
  today; the CI job moves to the dev profile for clippy and tests.
- **`dist` profile** changes the shipped binary; the e2e stack builds its own binary, so the static
  musl artifact needs a separate smoke run.
- **Core-dump suppression** is process-wide and inherited by the engine children; that is intended.
- **License text** is a legal statement; the wording needs the owner's confirmation.

## Scope

About 20 new files (hygiene, docs, tooling), edits to roughly 15 existing files, and move-only
restructuring of 9 Rust modules. Two new Rust dependencies: `rlimit`, and the `json` feature on
`tracing-subscriber`. Host TypeScript source is untouched.

Verification, all in a `rust:1.96` sibling container and the existing compose stack:

- `just ci` in `gino-agent/` (fmt, clippy, nextest, doctest, deny, shear, typos)
- `bun run check` at the root
- `./e2e/run.sh`

## Alternatives

- **Convert `gino-agent` to a multi-crate workspace** (`core`, `db`, `transport`, `engine`, `app`).
  Matches the PMA-Rust default layout, but the baseline permits a single crate for a single binary,
  and the split would force a public API between layers that share one SQLite handle. Rejected for
  now.
- **Rename `gino-agent/` to `agent/` or `apps/agent/`.** Symmetric with `apps/host`, but it is churn
  across e2e, examples, scripts, and both workflows. Rejected.
- **Bump the toolchain to current stable and keep MSRV at 1.96** with a separate `cargo hack` job.
  Closer to the letter of Lock 5; deferred as a routine toolchain bump.

## Annotations

- 2026-10-03 17:05 — The owner approved a narrower scope: refactor comments and documentation.
  Phase 2 is done, and the comment cleanup listed under follow-up tasks was pulled in and extended
  to the TypeScript packages. Phase 1 (`LICENSE`, `.editorconfig`, `.gitattributes`, `engines`),
  Phase 3 and Phase 4 are not approved and not started. `docs/decisions/` was not created because
  no divergence has been accepted yet.
- Comment cleanup result: tracking identifiers were removed from comments in `gino-agent/`,
  `packages/`, `apps/`, `e2e/` and the workflows. Left as-is by decision: test titles, log
  messages, OpenAPI `summary` strings, and SQL comments inside migration strings (SQLite stores
  that text in `sqlite_master`).
- Verification: `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` pass in
  `rust:1.96` (404 unit, 21 integration, 1 doc test). TypeScript typecheck passes. `eslint` reports
  69 errors and `host-options.test.ts` fails when run from the repository root; both are identical
  on the unmodified commit `53a3bf6`.
