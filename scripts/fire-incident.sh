#!/usr/bin/env bash
# Fire a demo incident occurrence into the MCP Events server's injected
# `incidents.created` stream.
#
# Usage:
#   scripts/fire-incident.sh P1 "checkout-service" "Database connection pool exhausted"
#   scripts/fire-incident.sh P3                      # service/title get defaults
#
# Server endpoint defaults to the demo server; override with MCP_SERVER.
set -euo pipefail

PRIORITY="${1:?usage: fire-incident.sh <P1|P2|P3|P4> [service] [title]}"
SERVICE="${2:-checkout-service}"
TITLE="${3:-Synthetic incident for demo}"
SERVER="${MCP_SERVER:-http://127.0.0.1:8090}"

case "$PRIORITY" in P1|P2|P3|P4) ;; *)
  echo "priority must be P1..P4, got: $PRIORITY" >&2; exit 1;;
esac

payload=$(printf '{"name":"incidents.created","data":{"priority":"%s","service":"%s","title":"%s"}}' \
  "$PRIORITY" "$SERVICE" "$TITLE")

response=$(curl -fsS -X POST "$SERVER/inject" -H 'Content-Type: application/json' -d "$payload")
echo "fired $PRIORITY incident on $SERVICE: $response"
