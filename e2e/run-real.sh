#!/usr/bin/env bash
# Real-flow e2e: drive one task through the REAL Claude CLI and the host /llm
# proxy to REAL Anthropic. Proves the production data path with no fakes.
#
# The upstream credential is USER-SUPPLIED — this script never reads your personal
# ~/.claude credentials. Provide ONE of:
#
#   export GINO_PROXY_ANTHROPIC_API_KEY='sk-ant-api03-...'        # a real API key (preferred)
#   # or your subscription OAuth access token (your call to repurpose it):
#   export GINO_PROXY_ANTHROPIC_OAUTH_TOKEN='sk-ant-oat01-...'
#
# Then:  ./e2e/run-real.sh            (optional: REAL_MODEL=claude-sonnet ./run-real.sh)
#
# Exit: 0 = real model answered; 3 = reached Anthropic but rate-limited (429);
#       1 = flow failure. Pass --logs to dump host/agent logs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$SCRIPT_DIR/docker-compose.real.yml")
DUMP_LOGS=0; [ "${1:-}" = "--logs" ] && DUMP_LOGS=1

# Credential resolution (in priority order):
#   1. GINO_PROXY_ANTHROPIC_API_KEY / _OAUTH_TOKEN in the env (passed through).
#   2. v1 proxy mode — stage a copy of ~/.claude/.credentials.json INTO the repo
#      (gitignored) so it reaches the host container via the existing /app mount;
#      the host entrypoint sources the OAuth token from it INSIDE the container
#      (as backup/src/api/proxy.ts auto-detected it). A copy is used because a
#      single-file bind mount of the original is blocked by SELinux without a
#      `:Z` relabel of the real creds file.
STAGED_CREDS="$SCRIPT_DIR/.real-creds.json"
CLAUDE_CREDS_DEFAULT="$HOME/.claude/.credentials.json"
STAGED=0

if [ -n "${GINO_PROXY_ANTHROPIC_API_KEY:-}" ] || [ -n "${GINO_PROXY_ANTHROPIC_OAUTH_TOKEN:-}" ]; then
  echo "── credential: from environment ──"
elif [ -s "$CLAUDE_CREDS_DEFAULT" ]; then
  # Owner-only: the container reads it as root over the bind mount; no group/other
  # access is needed. Create atomically under a restrictive umask, then pin 600.
  ( umask 077 && cp "$CLAUDE_CREDS_DEFAULT" "$STAGED_CREDS" )
  chmod 600 "$STAGED_CREDS"
  STAGED=1
  echo "── credential: v1 proxy mode — staged $CLAUDE_CREDS_DEFAULT → e2e/.real-creds.json (read inside the host container, gitignored, removed on exit) ──"
else
  cat >&2 <<'EOF'
✗ No upstream credential available.

Provide ONE of these, then re-run:

  export GINO_PROXY_ANTHROPIC_API_KEY='sk-ant-api03-...'       # a real Anthropic API key (preferred), or
  export GINO_PROXY_ANTHROPIC_OAUTH_TOKEN='sk-ant-oat01-...'   # an OAuth token, or
  # rely on v1 proxy mode: have ~/.claude/.credentials.json present (run `claude login`).
EOF
  exit 2
fi

# The runner binary must be staged (run.sh builds it). Reuse it; build if missing.
if [ ! -f "$SCRIPT_DIR/gino-agent.bin" ]; then
  echo "── runner binary missing; building it (rust:1.96-bookworm) ──"
  docker run --rm -v "$REPO_ROOT/gino-agent:/build" \
    -v "${CARGO_HOME:-$HOME/.cargo}/registry:/usr/local/cargo/registry" \
    -w /build -e CARGO_TERM_COLOR=never rust:1.96-bookworm \
    bash -c 'apt-get update >/dev/null && apt-get install -y --no-install-recommends cmake clang libclang-dev >/dev/null && cargo build --release --locked'
  cp "$REPO_ROOT/gino-agent/target/release/gino-agent" "$SCRIPT_DIR/gino-agent.bin"
fi

if ! docker image inspect gino-runner:latest >/dev/null 2>&1; then
  echo "✗ base image gino-runner:latest not found — it carries the real Claude CLI." >&2
  echo "  Build it from container/build.sh (or load the image) and re-run." >&2
  exit 2
fi

cleanup() {
  echo "── tearing down ──"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  [ "${STAGED:-0}" = 1 ] && rm -f "$STAGED_CREDS"
}
trap cleanup EXIT

echo "── building real-flow agent image ──"
"${COMPOSE[@]}" build

echo "── starting host + real agent (detached) ──"
"${COMPOSE[@]}" up -d host agent

echo "── running real-flow tester ──"
set +e
"${COMPOSE[@]}" run --rm -T tester
CODE=$?
set -e

if [ "$DUMP_LOGS" = 1 ]; then
  for svc in host agent; do
    echo "════════ logs: $svc ════════"
    "${COMPOSE[@]}" logs --no-color "$svc" 2>&1 | tail -80
  done
fi

echo ""
case "$CODE" in
  0) echo "REAL-FLOW PASSED ✓ (real model answered through the full stack)";;
  3) echo "REAL-FLOW REACHED ANTHROPIC but was RATE-LIMITED (429) ⚠ — wiring proven; retry or use a dedicated key";;
  *) echo "REAL-FLOW FAILED ✗ (exit $CODE) — rerun with --logs";;
esac
exit "$CODE"
