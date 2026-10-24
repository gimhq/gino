# 20261024-0123-agent-baseline Agent TLS stack and pma-rust baseline gaps

- **status**: completed
- **createdAt**: 2026-10-24 01:23
- **approvedAt**: 2026-10-24 01:23
- **relatedTask**: 20261024-0123-agent-tls-stack, 20261024-0123-agent-baseline-gaps

## Context

- `gino-agent/src/lib.rs:141` installs the aws-lc-rs provider; `ring` is still compiled because
  reqwest 0.12 `rustls-tls-native-roots` enables `__rustls-ring` (the comment wrongly blames
  tokio-tungstenite).
- `Cargo.toml` pins reqwest 0.12 to avoid `webpki-root-certs` (CDLA-Permissive-2.0) pulled by
  reqwest 0.13's `rustls-platform-verifier`. Latest: reqwest 0.13.5, tokio-tungstenite 0.30.0.
- No `[profile.*]`, no panic hook, no core-dump suppression; CI agent job runs fmt, clippy,
  `cargo test`, cargo-deny and cargo-shear only.

## Proposal

1. TLS: reqwest 0.13 with its default rustls/aws-lc-rs stack (`rustls-platform-verifier`),
   tokio-tungstenite 0.30 on rustls; allow CDLA-Permissive-2.0 in `deny.toml`; drop
   `rustls-native-certs` usage if the verifier replaces it; fix the provider comment.
2. Profiles: `[profile.release]` keeps `panic = "unwind"`; `[profile.dist]` inherits release with
   `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = "symbols"`; the release
   workflow and e2e build with `--profile dist`.
3. Runtime hardening in `main`: `rlimit` sets `RLIMIT_CORE` to 0 and a panic hook logs a
   `tracing::error!` with location and backtrace.
4. CI: add `typos` (with a `_typos.toml` for false positives), `cargo hack check --rust-version`
   and `cargo nextest run` + `cargo test --doc` in place of `cargo test`.

## Risks

- reqwest 0.13 API changes; the platform verifier on Linux reads the system store, so the
  container image must ship CA certificates (the e2e image already installs `ca-certificates`).
- `panic = "abort"` in `dist` skips unwinding; the hook still logs before abort.
- typos may flag identifiers; handled with a narrow `_typos.toml`.

## Scope

`gino-agent/Cargo.toml`, `Cargo.lock`, `deny.toml`, `src/lib.rs`, `src/main.rs`, possibly
`src/transport/*`/`src/register.rs` for API changes, `.github/workflows/{ci,release}.yml`,
`e2e/run.sh`, `_typos.toml`.

## Alternatives

- Keep reqwest 0.12 with `rustls-tls-native-roots-no-provider`: drops ring with a one-line change
  but stays on an old release and off the canonical verifier.

## Annotations

- 2026-10-24: approved ("按推荐处理，其他的基线缺口也一并处理").
- 2026-10-24: implementation notes. tokio-tungstenite exposes no supported platform-verifier
  hook, so the WebSocket keeps `rustls-native-certs` (both clients trust the OS store). The
  docker e2e keeps building the `release` profile for speed; only the release workflow ships
  `dist`. The nextest gate exposed two restart races in the integration tests (killing the
  daemon before the applied global version was persisted, and `wait_connected` returning on
  the pre-restart connection); both are fixed in the tests.
