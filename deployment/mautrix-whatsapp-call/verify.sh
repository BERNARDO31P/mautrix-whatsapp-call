#!/usr/bin/env bash
set -euo pipefail
# Run after starting the RTC containers and reloading nginx/Synapse.
HOST="https://__MATRIXRTC_SERVER_NAME__"
printf 'JWT service health: '
curl -fsS -o /dev/null -w '%{http_code}\n' "$HOST/livekit/jwt/healthz"
printf 'LiveKit handshake (HTTP response): '
curl -sS -o /dev/null -w '%{http_code}\n' "$HOST/livekit/sfu/"
printf 'Synapse version: '
curl -fsS "$HOST/_matrix/client/versions" | python3 -c 'import sys,json; print(", ".join(json.load(sys.stdin).get("versions",[])[-3:]))'
echo 'MatrixRTC transports require an ACCESS TOKEN. Enter it privately; it is not logged.'
read -r -s -p 'Access token: ' TOKEN
printf '\n'
curl -fsS -H "Authorization: Bearer $TOKEN" "$HOST/_matrix/client/v1/rtc/transports"
printf '\n'
unset TOKEN
