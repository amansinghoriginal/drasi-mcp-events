#!/usr/bin/env bash
# One-time demo environment setup (safe to re-run — every step is idempotent):
#   1. builds the workspace binaries
#   2. installs the Drasi plugins into the compose volume
#   3. starts the docker containers (postgres + drasi-server) and waits for health
#
# After this, each demo run needs only: scripts/demo-reset.sh, then the
# terminals from DEMO.md (server / agent / psql+fire-incident).
set -euo pipefail
cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "error: docker is required"; exit 1; }
command -v cargo  >/dev/null || { echo "error: rust/cargo is required"; exit 1; }

echo "==> [1/3] building workspace binaries"
cargo build --workspace

echo "==> [2/3] installing Drasi plugins (idempotent)"
(cd drasi && docker compose run --rm plugin-install)

echo "==> [3/3] starting postgres + drasi-server"
(cd drasi && docker compose up -d)

printf "==> waiting for drasi-server health"
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then break; fi
  printf "."; sleep 2
done
echo
curl -fsS http://localhost:8080/health >/dev/null 2>&1 || {
  echo "error: drasi-server did not become healthy;"
  echo "       inspect with: cd drasi && docker compose logs drasi-server"
  exit 1
}
echo "==> continuous queries:"
curl -fsS http://localhost:8080/api/v1/queries |
  python3 -c "import json,sys; [print(f'      {q[\"id\"]}: {q[\"status\"]}') for q in json.load(sys.stdin)['data']]"

if [ ! -f .env ]; then
  echo
  echo "note: no .env found — the agent will run its deterministic policy brain."
  echo "      For LLM-driven choice/reactions: cp .env.example .env and add credentials."
fi

cat <<'EOF'

Setup complete. To run the demo (see DEMO.md for the full script):
  scripts/demo-reset.sh                                   # clean state
  cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml
  cargo run -p mcp-events-agent  -- --task "You are on-call: watch for P1 incidents and escalate them"
  scripts/fire-incident.sh P1 checkout-service "Checkout returning 500s"
EOF
