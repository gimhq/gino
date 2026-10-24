# 20261024-0245-signed-release-assets Signed lode release assets, agent as tar.gz

- **status**: completed
- **priority**: P2
- **owner**: station/agent-baseline-20261024-0123
- **createdAt**: 2026-10-24 02:45

## Description

The release workflow uploaded the agent as a raw binary (with a broken upload path) and left
signing as a commented-out stub. Pack the agent as `gino-agent-linux-x64.tar.gz` and sign every
asset with lode-cli when the `LODE_SIGNING_KEY` secret is set: `<asset>.sig` sidecars for the
GitHub source, an inline-signed entry plus catalog signature in the host manifest.

Acceptance criteria: a locally simulated release produces assets that `lode-cli verify`
accepts, `validate-release.ts` passes on the signed host manifest, and lode installs the signed
agent archive under `require_signature = "enforce"` while rejecting a tampered asset or an
untrusted key.

## ActiveForm

Signing the lode release assets

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Approved in conversation on 2026-10-24 ("为什么不用压缩包？可以修改一下吧，同时增加签名").

- complete: Simulated release verified with lode-cli verify, validate-release.ts and an enforced lode install; tamper and wrong-key cases rejected.
