#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -lt 1 || $# -gt 3 ]]; then
    echo "Usage: $0 <@whatsapp_...:homeserver> [whatsapp-as-token-file] [https://matrix.example]" >&2
    echo "If session.json already exists, the token argument is optional and only the trust/cross-signing stage is retried." >&2
    exit 2
fi

GHOST="$1"
AS_TOKEN_ARG="${2:-}"
PUBLIC_HOMESERVER="${3:-https://__MATRIXRTC_SERVER_NAME__}"
STATE="$ROOT/worker-state"

[[ "$GHOST" == @*:* ]] || { echo "Ghost must be a full Matrix user ID: $GHOST" >&2; exit 2; }

mkdir -p "$STATE"
chmod 0700 "$STATE"

args=(--homeserver "$PUBLIC_HOMESERVER" --ghost-mxid "$GHOST" --state-dir "$STATE"
      --compose-overlay "$ROOT/compose.yml")
if [[ -n "$AS_TOKEN_ARG" ]]; then args+=(--as-token-file "$AS_TOKEN_ARG"); fi
exec python3 "$ROOT/worker/provision-ghost.py" "${args[@]}"
