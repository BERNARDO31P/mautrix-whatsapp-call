#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MATRIX_ROOT="$(cd "$ROOT/.." && pwd)"
COMPOSE=(docker compose -f "$MATRIX_ROOT/docker-compose.yml" -f "$ROOT/compose.yml")
"${COMPOSE[@]}" ps mautrix-whatsapp-call-worker
"${COMPOSE[@]}" exec -T mautrix-whatsapp-call-worker sh -ec 'test -S /run/mautrix-whatsapp-call/worker.sock; test "$(stat -c %a /run/mautrix-whatsapp-call/worker.sock)" = 660'
"${COMPOSE[@]}" exec -T mautrix-whatsapp sh -ec 'test -S /run/mautrix-whatsapp-call/worker.sock; test -r /run/mautrix-whatsapp-call/worker.sock; test -w /run/mautrix-whatsapp-call/worker.sock'
"${COMPOSE[@]}" exec -T mautrix-whatsapp-call-worker mautrix-whatsapp-call-worker healthcheck
echo "mautrix-whatsapp-call worker socket is live and accessible to mautrix-whatsapp."
