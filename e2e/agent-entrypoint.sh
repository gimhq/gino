#!/bin/sh
# Entrypoint for the gino gino-agent e2e image.
#
# The runner is a daemon that dials GINO_HOST_URL, registers with
# GINO_REGISTER_TOKEN, opens its WebSocket and runs tasks via the `claude` engine
# on PATH (here the fake at /usr/local/bin/claude). It runs as root in the e2e —
# the real Claude CLI refuses --dangerously-skip-permissions as root, but the
# fake engine ignores all flags, so no user drop is needed. With no arguments the
# runner starts the persistent daemon (the default subcommand).
set -eu

mkdir -p "${GINO_WORKSPACE_DIR:-/workspace}"
exec /app/gino-agent "$@"
