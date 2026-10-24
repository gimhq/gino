#!/usr/bin/env bash
# Real-engine sandbox e2e: drive the REAL Claude CLI, confined by the agent in a
# bubblewrap sandbox (task meta.isolation), through the host /llm proxy to REAL
# Anthropic, and assert what the sandboxed engine can and cannot see.
#
# Credentials are resolved exactly as run-real.sh does (env, else v1 proxy mode
# staging of ~/.claude/.credentials.json).
#
#   ./e2e/run-real-sandbox.sh            real run (SANDBOX_NETWORK=none|allowlist|full, default none)
#   ./e2e/run-real-sandbox.sh --smoke    no model call: registration, sandboxed task
#                                        dispatch, agent-side meta check, bwrap probe
#   --rebuild   force a fresh `cargo build` of the runner binary
#   --logs      dump host/agent logs after the run
#
# Exit: 0 = passed; 3 = reached Anthropic but rate-limited (429); 1 = failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$SCRIPT_DIR/docker-compose.real-sandbox.yml")
DUMP_LOGS=0; SMOKE=0; REBUILD=0
for arg in "$@"; do
  case "$arg" in
    --logs) DUMP_LOGS=1 ;;
    --smoke) SMOKE=1 ;;
    --rebuild) REBUILD=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done
export SMOKE
export SANDBOX_NETWORK="${SANDBOX_NETWORK:-none}"
LABEL="network: $SANDBOX_NETWORK"
if [ "$SMOKE" = 1 ]; then SANDBOX_NETWORK=none; LABEL="smoke, network: none"; fi
SECCOMP_URL="${SECCOMP_URL:-https://raw.githubusercontent.com/moby/profiles/main/seccomp/default.json}"
CARGO_REGISTRY="${CARGO_REGISTRY-/srv/station/root/.cargo/registry}"

for tool in docker curl jq; do
  command -v "$tool" >/dev/null || { echo "✗ $tool is required" >&2; exit 2; }
done

STAGED_CREDS="$SCRIPT_DIR/.real-creds.json"
CLAUDE_CREDS_DEFAULT="$HOME/.claude/.credentials.json"
STAGED=0
TMP="$REPO_ROOT/.tmp/real-sandbox-$$"

cleanup() {
  echo "── tearing down ──"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  [ "${STAGED:-0}" = 1 ] && rm -f "$STAGED_CREDS"
  rm -rf "$TMP"
}
trap cleanup EXIT

if [ -n "${GINO_PROXY_ANTHROPIC_API_KEY:-}" ] || [ -n "${GINO_PROXY_ANTHROPIC_OAUTH_TOKEN:-}" ]; then
  echo "── credential: from environment ──"
elif [ -s "$CLAUDE_CREDS_DEFAULT" ]; then
  ( umask 077 && cp "$CLAUDE_CREDS_DEFAULT" "$STAGED_CREDS" )
  chmod 600 "$STAGED_CREDS"
  STAGED=1
  echo "── credential: v1 proxy mode — staged $CLAUDE_CREDS_DEFAULT → e2e/.real-creds.json (read inside the host container, gitignored, removed on exit) ──"
elif [ "$SMOKE" = 1 ]; then
  echo "── credential: none (smoke makes no model call) ──"
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

# The runner binary must carry the `sandbox-init` subcommand; build if missing.
if [ "$REBUILD" = 1 ] || [ ! -f "$SCRIPT_DIR/gino-agent.bin" ]; then
  echo "── building runner binary (rust:1.96-bookworm) ──"
  reg_mount=()
  [ -n "$CARGO_REGISTRY" ] && [ -d "$CARGO_REGISTRY" ] && reg_mount=(-v "$CARGO_REGISTRY:/usr/local/cargo/registry")
  docker run --rm --label ai-agent=true \
    -v "$REPO_ROOT/gino-agent:$REPO_ROOT/gino-agent" "${reg_mount[@]}" \
    -w "$REPO_ROOT/gino-agent" -e CARGO_TERM_COLOR=never rust:1.96-bookworm \
    bash -c 'apt-get update >/dev/null && apt-get install -y --no-install-recommends cmake clang libclang-dev >/dev/null && cargo build --release --locked'
  cp "$REPO_ROOT/gino-agent/target/release/gino-agent" "$SCRIPT_DIR/gino-agent.bin"
fi

# Seccomp profile: the moby default with the namespace and mount syscalls
# bubblewrap needs (without CAP_SYS_ADMIN) allowed unconditionally.
mkdir -p "$TMP"
curl -fsSL "$SECCOMP_URL" -o "$TMP/default.json"
jq --argjson names '["unshare","clone","clone3","mount","umount2","pivot_root","setns"]' '
  .syscalls |= map(.names -= $names | select(.names | length > 0))
  | .syscalls += [{ names: $names, action: "SCMP_ACT_ALLOW" }]
' "$TMP/default.json" > "$TMP/seccomp.json"
export SANDBOX_SECCOMP_PROFILE="$TMP/seccomp.json"

echo "── building sandbox agent image ──"
"${COMPOSE[@]}" build

echo "── starting host + sandbox agent (detached) ──"
"${COMPOSE[@]}" up -d host agent

# Files the sandboxed engine must NOT see: one in the workspace root and one in
# another thread's work dir.
"${COMPOSE[@]}" exec -T agent sh -c '
  mkdir -p /workspace/group/threads/decoy-thread
  echo canary > /workspace/group/canary.txt
  echo secret > /workspace/group/threads/decoy-thread/secret.txt'

echo "── running sandbox tester ($LABEL) ──"
set +e
OUT="$("${COMPOSE[@]}" run --rm -T tester 2>&1)"
CODE=$?
set -e
echo "$OUT"

if [ "$SMOKE" = 1 ] && [ "$CODE" = 0 ]; then
  TASK_ID="$(sed -n 's/^SMOKE_TASK_ID=//p' <<<"$OUT" | tail -1)"
  smoke_fail() { echo "✗ SMOKE: $1" >&2; CODE=1; }

  # The agent stored the task's meta.isolation (task_upsert reached it).
  meta=""
  for _ in $(seq 1 30); do
    meta="$("${COMPOSE[@]}" exec -T -e TASK_ID="$TASK_ID" agent node -e '
      const { DatabaseSync } = require("node:sqlite");
      const db = new DatabaseSync(process.env.GINO_DB_PATH, { readOnly: true });
      const row = db.prepare("SELECT meta FROM tasks WHERE id = ?").get(process.env.TASK_ID);
      process.stdout.write(row?.meta ?? "");' 2>/dev/null || true)"
    [ -n "$meta" ] && break
    sleep 1
  done
  if jq -e '.isolation.mode == "sandbox" and .isolation.network.mode == "none"' <<<"$meta" >/dev/null 2>&1; then
    echo "✓ agent task row meta: $meta"
  else
    smoke_fail "agent task row meta not received (got '${meta}')"
  fi

  # The sandboxed run must not fail closed on the sandbox itself.
  sleep 5
  if "${COMPOSE[@]}" logs --no-color agent 2>&1 | grep -E 'sandbox is unavailable|engine sandbox needs|sandbox-init:'; then
    smoke_fail "agent reported a sandbox failure"
  else
    echo "✓ agent log: no sandbox failure"
  fi

  # bubblewrap works for the agent user inside the agent container.
  probe="$("${COMPOSE[@]}" exec -T agent sh -c \
    'echo "user=$(id -u)"; bwrap --unshare-all --die-with-parent --ro-bind / / --proc /proc --dev /dev --uid 1000 --gid 1000 true && echo bwrap=ok' 2>&1 || true)"
  echo "$probe"
  if grep -qx 'bwrap=ok' <<<"$probe" && ! grep -qx 'user=0' <<<"$probe"; then
    echo "✓ bwrap probe as the agent user"
  else
    smoke_fail "bwrap probe failed"
  fi
  echo "$("${COMPOSE[@]}" exec -T agent claude --version 2>&1 | head -1) (claude CLI)"
fi

if [ "$DUMP_LOGS" = 1 ]; then
  for svc in host agent; do
    echo "════════ logs: $svc ════════"
    "${COMPOSE[@]}" logs --no-color "$svc" 2>&1 | tail -80
  done
fi

echo ""
case "$CODE" in
  0) echo "REAL-SANDBOX PASSED ✓ ($LABEL)";;
  3) echo "REAL-SANDBOX REACHED ANTHROPIC but was RATE-LIMITED (429) ⚠ — wiring proven; retry or use a dedicated key";;
  *) echo "REAL-SANDBOX FAILED ✗ (exit $CODE) — rerun with --logs";;
esac
exit "$CODE"
