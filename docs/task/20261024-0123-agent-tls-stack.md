# 20261024-0123-agent-tls-stack Single aws-lc-rs TLS stack on current client crates

- **status**: completed
- **priority**: P2
- **owner**: station/agent-baseline-20261024-0123
- **createdAt**: 2026-10-24 01:23

## Description

The agent compiles both rustls crypto providers: reqwest 0.12's `rustls-tls-native-roots`
enables `__rustls-ring`, so `ring` sits beside `aws-lc-rs`. reqwest is pinned to 0.12 only
because 0.13's `rustls-platform-verifier` pulls `webpki-root-certs` (CDLA-Permissive-2.0).
Align with the pma-rust TLS lock: rustls + aws-lc-rs only, `rustls-platform-verifier`, current
releases.

Acceptance criteria:

- `ring` is absent from `cargo tree`; aws-lc-rs is the only provider.
- reqwest and tokio-tungstenite are on their latest releases; CDLA-Permissive-2.0 is allowed in
  `deny.toml` with a reason.
- fmt, clippy, tests, cargo-deny, cargo-shear, the static musl release build and the docker e2e pass.

## ActiveForm

Upgrading the agent TLS stack

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Plan: `docs/plan/20261024-0123-agent-baseline.md`.

- complete: All agent gates and the docker e2e passed.
