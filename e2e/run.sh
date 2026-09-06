#!/usr/bin/env bash
# Build the gino-agent binary, then run the full docker-compose e2e and report.
#
#   ./run.sh             build the runner binary if missing, then run the e2e
#   ./run.sh --rebuild   force a fresh `cargo build` of the runner binary
#   ./run.sh --logs      dump per-service logs after the run
#   ./run.sh --keep      leave the stack up after the run (no `compose down`)
#   ./run.sh --build-only  build/stage the runner binary and exit (no compose run)
#
# The runner is compiled in a rust:1.96-bookworm container with the host's warm
# cargo registry mounted (fast, offline-friendly) and staged as gino-agent.bin
# — a glibc binary that runs as-is on the bookworm-slim runtime image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$SCRIPT_DIR/docker-compose.yml")

REBUILD=0 DUMP_LOGS=0 KEEP=0 BUILD_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --rebuild) REBUILD=1 ;;
    --logs) DUMP_LOGS=1 ;;
    --keep) KEEP=1 ;;
    --build-only) BUILD_ONLY=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

BIN="$SCRIPT_DIR/gino-agent.bin"
CARGO_REGISTRY="${CARGO_HOME:-$HOME/.cargo}/registry"

build_binary() {
  echo "── building gino-agent (rust:1.96-bookworm, warm registry) ──"
  local reg_mount=()
  [ -d "$CARGO_REGISTRY" ] && reg_mount=(-v "$CARGO_REGISTRY:/usr/local/cargo/registry")
  docker run --rm \
    -v "$REPO_ROOT/gino-agent:/build" \
    "${reg_mount[@]}" \
    -w /build \
    -e CARGO_TERM_COLOR=never \
    rust:1.96-bookworm \
    bash -c 'set -e
      apt-get update >/dev/null
      apt-get install -y --no-install-recommends cmake clang libclang-dev >/dev/null
      cargo build --release --locked'
  cp "$REPO_ROOT/gino-agent/target/release/gino-agent" "$BIN"
  echo "staged $BIN ($(du -h "$BIN" | cut -f1))"
}

if [ "$REBUILD" = 1 ] || [ ! -f "$BIN" ]; then
  build_binary
else
  echo "reusing staged runner binary ($BIN) — pass --rebuild to recompile"
fi

if [ "$BUILD_ONLY" = 1 ]; then
  echo "── --build-only: runner binary staged at $BIN; skipping the compose run ──"
  exit 0
fi

cleanup() {
  if [ "$KEEP" = 1 ]; then
    echo "── stack left up (--keep). Tear down with: ${COMPOSE[*]} down -v ──"
  else
    echo "── tearing down ──"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "── building images ──"
"${COMPOSE[@]}" build

# Start the long-lived services detached, then run the tester as a one-off. This
# decouples the tester's lifecycle from the services: the `task lifecycle` group
# deliberately triggers a per-task `close` that (today) shuts the agent down, and
# `--abort-on-container-exit` would otherwise kill the tester mid-summary. With
# `compose run` the tester always runs to completion and owns the exit code.
echo "── starting services (detached) ──"
"${COMPOSE[@]}" up -d mock-llm host agent

echo "── running tester ──"
set +e
"${COMPOSE[@]}" run --rm -T tester
CODE=$?
set -e

if [ "$DUMP_LOGS" = 1 ]; then
  for svc in host agent mock-llm; do
    echo "════════ logs: $svc ════════"
    "${COMPOSE[@]}" logs --no-color "$svc" 2>&1 | tail -100
  done
fi

echo ""
if [ "$CODE" = 0 ]; then
  echo "E2E PASSED ✓"
else
  echo "E2E FAILED ✗ (exit $CODE) — rerun with --logs to inspect host/agent output"
fi
exit "$CODE"
