#!/usr/bin/env bash
# Resets demo state for a clean run:
#   - stops the MCP server (its in-memory event buffer must start empty)
#   - restores the orders table to the four seed rows (ids 1-4, none 'open')
#   - clears the order_flags audit trail
#   - deletes all agent cursor files
#
# The docker containers keep running (start them with scripts/demo-setup.sh).
# After a reset, start the server (Terminal A) and agent (Terminal B) fresh.
set -euo pipefail

docker ps --format '{{.Names}}' | grep -q '^drasi-demo-postgres$' || {
  echo "error: demo containers are not running — run scripts/demo-setup.sh first"
  exit 1
}

# -sTCP:LISTEN scopes the kill to the server itself — a plain `lsof -ti :8090`
# would also match connected agents' client sockets.
if pids=$(lsof -ti tcp:8090 -sTCP:LISTEN 2>/dev/null) && [ -n "$pids" ]; then
  echo "$pids" | xargs kill
  echo "==> stopped MCP server on :8090 (restart it fresh — its buffer must start empty;"
  echo "    running agents will retry/backoff and heal once the server is back)"
fi

echo "==> restoring seed data (row-level deletes, not TRUNCATE, so WAL replication stays happy)"
docker exec -i drasi-demo-postgres psql -U demo -d demo -q <<'SQL'
TRUNCATE order_flags RESTART IDENTITY;
DELETE FROM orders;
ALTER SEQUENCE orders_id_seq RESTART WITH 1;
INSERT INTO orders (customer, total, status) VALUES
    ('alice', 1500.00, 'paid'),
    ('bob',    250.00, 'pending'),
    ('carol', 2200.50, 'shipped'),
    ('dave',   999.99, 'pending');
SQL

rm -f /tmp/drasi-agent-cursor-*.json /tmp/drasi-standalone-agent-cursor.json
echo "==> cleared agent cursor files"

cat <<'EOF'

Clean. Next:
  Terminal A: cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml
  Terminal B: cargo run -p mcp-events-agent  -- --task "..."
EOF
