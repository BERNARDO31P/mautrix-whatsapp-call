#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="$ROOT/generated"
SERVER="${MATRIXRTC_SERVER_NAME:-}"

[[ -n "$SERVER" ]] || {
    echo "MATRIXRTC_SERVER_NAME is required before running prepare.sh" >&2
    exit 2
}
[[ "$SERVER" =~ ^[A-Za-z0-9.-]+$ && "$SERVER" == *.* ]] || {
    echo "Invalid MATRIXRTC_SERVER_NAME: $SERVER" >&2
    exit 2
}

umask 077
mkdir -p "$DEST"

exec 9>"$DEST/.prepare.lock"
flock -x 9
python3 "$ROOT/reconcile-credentials.py" "$ROOT" "$SERVER"
STAGING="$(mktemp -d "$DEST/.render.XXXXXX")"
trap 'rm -rf "$STAGING"' EXIT

WORKER_STATE="$ROOT/worker-state"
mkdir -p "$WORKER_STATE"
chmod 700 "$WORKER_STATE"

SECRET="$(cat "$DEST/livekit_secret")"
cat > "$STAGING/livekit.yaml" <<YAML
port: 7880
rtc:
  tcp_port: 7881
  udp_port: 7882
  use_external_ip: true
keys:
  lk-matrixrtc: "$SECRET"
room:
  auto_create: false
webhook:
  api_key: lk-matrixrtc
  urls:
    - http://lk-jwt-service:8080/sfu_webhook
YAML

cat > "$STAGING/matrixrtc-registration.yaml" <<YAML
id: "matrixrtc-jwt-service"
as_token: "$(cat "$DEST/as_token")"
hs_token: "$(cat "$DEST/hs_token")"
sender_localpart: "_matrixrtc_jwt_service"
url: null
namespaces:
  users:
    - exclusive: false
      regex: ".*"
io.element.msc4502.scopes:
  - "urn:matrix:client:io.element.msc4502:rooms:is_joined"
io.element.msc4512.proxy_prefix: "rtc/livekit"
io.element.msc4512.proxy_url: "http://lk-jwt-service:8080"
YAML

SERVER_REGEX="${SERVER//./\\.}"
cat > "$STAGING/matrixrtc-doublepuppet-registration.yaml" <<YAML
id: "matrixrtc-whatsapp-doublepuppet"
url: null
as_token: "$(cat "$DEST/double_puppet_as_token")"
hs_token: "$(cat "$DEST/double_puppet_hs_token")"
sender_localpart: "_matrixrtc_doublepuppet"
rate_limited: false
namespaces:
  users:
    - regex: "^@[^:]+:${SERVER_REGEX//\\/\\\\}\$"
      exclusive: false
YAML

printf 'as_token:%s\n' "$(cat "$DEST/double_puppet_as_token")" > "$STAGING/double_puppet_secret"
for path in "$STAGING"/*; do chmod 600 "$path"; mv -f "$path" "$DEST/"; done

cat <<TEXT
MatrixRTC secrets and registrations generated locally for $SERVER.
KEEP mautrix-whatsapp-call/generated and mautrix-whatsapp-call/worker-state private.
Generated registrations:
  matrixrtc-registration.yaml
  matrixrtc-doublepuppet-registration.yaml
The double-puppet registration is non-exclusive and limited to local MXIDs on $SERVER.
Next: run configure-bridge.py --apply and merge-synapse.py --apply, validate, then restart Synapse/mautrix-whatsapp before running migrate-rooms.sh.
TEXT
