#!/usr/bin/env bash
set -euo pipefail
# Run after starting the RTC containers and reloading nginx/Synapse.
HOST="https://__MATRIXRTC_SERVER_NAME__"
printf 'JWT service health: '
curl --connect-timeout 5 --max-time 20 -fsS -o /dev/null -w '%{http_code}\n' "$HOST/livekit/jwt/healthz"
printf 'LiveKit handshake (HTTP response): '
curl --connect-timeout 5 --max-time 20 -sS -o /dev/null -w '%{http_code}\n' "$HOST/livekit/sfu/"
printf 'Synapse version: '
curl --connect-timeout 5 --max-time 20 -fsS "$HOST/_matrix/client/versions" | python3 -c '
import sys, json
body = json.load(sys.stdin)
print(", ".join(body.get("versions", [])[-3:]))
features = body.get("unstable_features", {})
if not isinstance(features, dict) or features.get("org.matrix.msc4354") is not True or not any(features.get(name) is True for name in ("org.matrix.msc4143", "org.matrix.msc4143.stable")):
    sys.exit("Modern MatrixRTC requires advertised MSC4143 and MSC4354 support")
'
echo 'MatrixRTC transports require an ACCESS TOKEN. Enter it privately; it is not logged.'
read -r -s -p 'Access token: ' TOKEN
printf '\n'
# Matrix tokens are opaque but must not contain control characters; pass the
# header on stdin rather than in the process argument list.
[[ -n "$TOKEN" && "$TOKEN" != *$'\r'* && "$TOKEN" != *$'\n'* ]] || { echo "Invalid token" >&2; exit 2; }
printf 'Authorization: Bearer %s\n' "$TOKEN" | curl --connect-timeout 5 --max-time 20 -fsS -H @- "$HOST/_matrix/client/v1/rtc/transports"
printf '\n'
unset TOKEN
