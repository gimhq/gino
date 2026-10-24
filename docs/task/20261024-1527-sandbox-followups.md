# 20261024-1527-sandbox-followups Engine sandbox follow-ups

- **status**: pending
- **priority**: P1
- **owner**: (unassigned)
- **createdAt**: 2026-10-24 15:27

## Description

Leftovers from `20261024-0058-engine-sandbox`:

1. **Unfetchable git dependency (blocks CI and clean builds).** `nsplane-dns` v0.17.0 depends on
   `https://github.com/dotns/simple-dns` (tag `v0.12.0-nsplane.3`), which is not publicly
   reachable. Needs an upstream fix (publish the repo, upstream the patch to crates.io, or vendor it
   in nsplane); then update `gino-agent/Cargo.lock` and `gino-agent/deny.toml`.
2. **Real-sandbox e2e harness.** `e2e/run-real-sandbox.sh` builds the agent without the cargo git
   cache (git dependencies fail to fetch) and silently reuses a stale `e2e/gino-agent.bin` when the
   build fails; it must mount the git cache and fail when the binary cannot be rebuilt.
3. **Leaky test.** nextest reports `sandbox::tests::handover_without_a_tun_device_kills_the_sandbox`
   as leaky (a child keeps handles open past the test).
4. **Unverified with real binaries.** The PreCompact hook (`gino-agent archive`) under the
   sandbox with the real claude CLI, and codex under the sandbox (SIGINT handling); codex
   credentials are needed.

Acceptance criteria: a clean environment builds and passes the agent gates; the harness fails
loudly on a stale binary; no leaky test; the two real-binary checks pass.

## ActiveForm

Closing engine sandbox follow-ups

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)
