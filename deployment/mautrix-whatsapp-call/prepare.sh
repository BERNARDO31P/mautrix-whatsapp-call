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
[[ ! -L "$DEST" && ! -L "$ROOT/worker-state" && ! -L "$DEST/.prepare.lock" ]] || { echo "Refusing symbolic link in private deployment state" >&2; exit 2; }
mkdir -p "$DEST"
chmod 700 "$DEST"

exec 9>"$DEST/.prepare.lock"
flock -x 9
python3 "$ROOT/reconcile-credentials.py" "$ROOT" "$SERVER"
STAGING="$(mktemp -d "$DEST/.render.XXXXXX")"
trap 'rm -rf "$STAGING"' EXIT

WORKER_STATE="$ROOT/worker-state"
mkdir -p "$WORKER_STATE"
chmod 700 "$WORKER_STATE"

python3 - "$ROOT" "$DEST" "$STAGING" "$SERVER" <<'PYCONFIG'
import re, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from config_files import dump, atomic_write
src, out = map(Path, sys.argv[2:4])
server = sys.argv[4]
def secret(name):
    return (src / name).read_text().strip()
def write(name, data):
    atomic_write(out / name, dump(data).encode())
write("livekit.yaml", {
    "port": 7880,
    "rtc": {"tcp_port": 7881, "udp_port": 7882, "use_external_ip": True},
    "keys": {"lk-matrixrtc": secret("livekit_secret")},
    "room": {"auto_create": False},
    "webhook": {"api_key": "lk-matrixrtc", "urls": ["http://lk-jwt-service:8080/sfu_webhook"]},
})
write("matrixrtc-registration.yaml", {
    "id": "matrixrtc-jwt-service", "as_token": secret("as_token"), "hs_token": secret("hs_token"),
    "sender_localpart": "_matrixrtc_jwt_service", "url": None,
    "namespaces": {"users": [{"exclusive": False, "regex": ".*"}]},
    "io.element.msc4502.scopes": ["urn:matrix:client:io.element.msc4502:rooms:is_joined"],
    "io.element.msc4512.proxy_prefix": "rtc/livekit",
    "io.element.msc4512.proxy_url": "http://lk-jwt-service:8080",
})
write("matrixrtc-doublepuppet-registration.yaml", {
    "id": "matrixrtc-whatsapp-doublepuppet", "url": None,
    "as_token": secret("double_puppet_as_token"), "hs_token": secret("double_puppet_hs_token"),
    "sender_localpart": "_matrixrtc_doublepuppet", "rate_limited": False,
    "namespaces": {"users": [{"regex": "^@[^:]+:" + re.escape(server) + "$", "exclusive": False}]},
})
atomic_write(out / "double_puppet_secret", ("as_token:" + secret("double_puppet_as_token") + "\n").encode())
PYCONFIG

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
