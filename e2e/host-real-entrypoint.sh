#!/bin/sh
# Host entrypoint for the real-flow e2e — "v1 proxy mode" credential sourcing.
#
# The old proxy (backup/src/api/proxy.ts) auto-detected the Anthropic OAuth token
# by reading ~/.claude/.credentials.json on the host and injecting it; containers
# only ever talked to the proxy. This mirrors that: if no explicit upstream
# credential is configured AND a Claude credentials file is mounted at
# /creds/credentials.json, read the OAuth access token from it (inside the
# container, so it never touches the launching shell or `docker inspect` config)
# and hand it to the proxy as GINO_PROXY_ANTHROPIC_OAUTH_TOKEN.
set -eu

# The creds file is staged by run-real.sh into the repo (gitignored) and reaches
# us via the existing read-only repo mount at /app — avoiding a separate single-
# file bind mount, which SELinux (Fedora) blocks without a `:Z` relabel of the
# original. Override with GINO_CREDS_PATH if needed.
CREDS="${GINO_CREDS_PATH:-/app/e2e/.real-creds.json}"
if [ -z "${GINO_PROXY_ANTHROPIC_API_KEY:-}" ] \
   && [ -z "${GINO_PROXY_ANTHROPIC_OAUTH_TOKEN:-}" ] \
   && [ -s "$CREDS" ]; then
  TOK="$(bun -e "try{const c=JSON.parse(await Bun.file('$CREDS').text());process.stdout.write(c.claudeAiOauth?.accessToken||'')}catch{process.stdout.write('')}" 2>/dev/null || true)"
  if [ -n "$TOK" ]; then
    export GINO_PROXY_ANTHROPIC_OAUTH_TOKEN="$TOK"
    echo "[host] sourced Anthropic OAuth token from mounted Claude credentials (v1 proxy mode)"
  else
    echo "[host] /creds/credentials.json present but no claudeAiOauth.accessToken — falling back"
  fi
fi

exec bun apps/host/src/index.ts
