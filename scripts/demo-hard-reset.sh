#!/usr/bin/env bash
# Break-glass reset: destroys and recreates the docker environment from
# scratch (containers + volumes, including the Drasi plugin volume and the
# Postgres data dir), then runs full setup again.
#
# Use this when:
#   - you changed drasi/server.yaml or drasi/seed.sql (both only take effect
#     on container/volume recreation)
#   - drasi-server is wedged or crash-looping
#   - pre-demo paranoia: you want a known-good world
#
# For a routine between-runs clean, scripts/demo-reset.sh is much faster and
# sufficient — it restores identical state without touching the containers.
set -euo pipefail
cd "$(dirname "$0")/.."

if pids=$(lsof -ti :8090 2>/dev/null) && [ -n "$pids" ]; then
  echo "$pids" | xargs kill
  echo "==> stopped MCP server on :8090"
fi

echo "==> destroying containers and volumes"
(cd drasi && docker compose down -v)

rm -f /tmp/drasi-agent-cursor-*.json /tmp/drasi-standalone-agent-cursor.json
echo "==> cleared agent cursor files"

exec "$(dirname "$0")/demo-setup.sh"
