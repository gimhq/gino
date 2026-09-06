#!/usr/bin/env bash
# Round-trip the embed-node example against the real gino-agent runner.
# Builds the runner binary (docker + cargo, minutes) when e2e/gino-agent.bin is
# missing, then runs scripts/roundtrip.ts under plain `node`. Not hermetic —
# which is why it is `e2e`, not part of `check`.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
BIN=../../e2e/gino-agent.bin

if [ ! -f "$BIN" ]; then
  echo "── $BIN missing; building it via e2e/run.sh --build-only ──"
  bash ../../e2e/run.sh --build-only
fi

exec node scripts/roundtrip.ts
