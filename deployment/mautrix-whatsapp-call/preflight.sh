#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MATRIX_ROOT="$(cd "$ROOT/.." && pwd)"
BRIDGE_CONFIG="${1:-$MATRIX_ROOT/mautrix-whatsapp/config.yaml}"
fail() { echo "ERROR: $*" >&2; exit 1; }
# The installer uses this read-only check before prepare.sh creates generated
# files. Keep the build inputs in sync with both Dockerfiles and the runtime
# helpers called by install-or-upgrade.sh.
for path in \
    bridge/Dockerfile bridge/.dockerignore bridge/mautrix-whatsapp-call-bridge \
    worker/Dockerfile worker/.dockerignore \
    worker/mautrix-whatsapp-call-worker worker/mautrix-whatsapp-call-provision \
    worker/provision-ghost.py worker/auto-provision-whatsapp-ghosts.py \
    worker/portal_kind.py worker/ghost_namespace.py worker/session_files.py \
    compose.yml config_files.py configure-bridge.py merge-synapse.py \
    migrate-rooms.sh prepare.sh reconcile-credentials.py render-hostname.py \
    resolve-appservice.py sync-direct-chats.sh synapse-additions.yaml \
    validate-candidate.py verify-worker.sh verify.sh; do
    [[ -f "$ROOT/$path" && -s "$ROOT/$path" && ! -L "$ROOT/$path" ]] ||
        fail "missing or empty deployment payload: $path"
done
# Validate the uploaded binaries before the installer pauses any service.
# Linux AMD64 is the supported release target. A filename or nonempty file
# alone is not sufficient; this is a header check, not an execution test.
python3 - "$ROOT" <<'PYELF'
from pathlib import Path
import struct, sys
root = Path(sys.argv[1])
for name in ("bridge/mautrix-whatsapp-call-bridge",
             "worker/mautrix-whatsapp-call-worker",
             "worker/mautrix-whatsapp-call-provision"):
    with (root / name).open("rb") as stream:
        header = stream.read(64)
    if (len(header) != 64 or header[:7] != b"\x7fELF\x02\x01\x01"
            or struct.unpack_from("<H", header, 16)[0] not in (2, 3)
            or struct.unpack_from("<H", header, 18)[0] != 62
            or struct.unpack_from("<I", header, 20)[0] != 1
            or struct.unpack_from("<H", header, 52)[0] != 64):
        raise SystemExit(f"Invalid Linux AMD64 executable header: {name}")
PYELF
if [[ "${1:-}" == "--payload-only" ]]; then
    echo "Deployment payload presence: PASS"
    exit 0
fi
[[ -f "$MATRIX_ROOT/docker-compose.yml" ]] || fail "missing $MATRIX_ROOT/docker-compose.yml"
[[ -f "$BRIDGE_CONFIG" ]] || fail "missing mautrix-whatsapp config: $BRIDGE_CONFIG"
[[ -s "$ROOT/generated/matrixrtc_store_passphrase" ]] ||
    fail "missing or empty generated matrixrtc_store_passphrase (run prepare.sh)"
mode="$(stat -c '%a' "$ROOT/generated/matrixrtc_store_passphrase")"
[[ "$mode" == "600" ]] || fail "matrixrtc_store_passphrase must be mode 600, got $mode"
python3 - "$BRIDGE_CONFIG" "$ROOT" <<'PY'
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[2])
from config_files import load
network = load(Path(sys.argv[1]).read_text())["network"]
checks = {
    "experimental_whatsapp_voip": True,
    "rtc_gateway_socket": "/run/mautrix-whatsapp-call/worker.sock",
}
for name, expected in checks.items():
    if network.get(name) != expected:
        raise SystemExit(f"bridge config does not enable expected network.{name}")
print("Bridge RTC gateway config: PASS")
PY
python3 - "$ROOT/compose.yml" <<'PY'
import sys, yaml
from pathlib import Path
yaml.safe_load(Path(sys.argv[1]).read_text())
print("Compose overlay YAML parse: PASS")
PY
# Compose must resolve interpolation from the actual deployment directory,
# including its .env, regardless of the caller's current working directory.
cd "$MATRIX_ROOT"
command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 ||
    fail "Docker Compose v2 unavailable; deployment configuration cannot be validated"
base_services="$(docker compose -f "$MATRIX_ROOT/docker-compose.yml" config --services)" ||
    fail "existing Docker Compose project validation failed"
for service in synapse mautrix-whatsapp; do
    grep -Fxq "$service" <<<"$base_services" || fail "existing Compose service missing: $service"
done
docker compose -f "$MATRIX_ROOT/docker-compose.yml" -f "$ROOT/compose.yml" config --quiet ||
    fail "merged Docker Compose configuration validation failed"
echo "Docker Compose merged config: PASS"
echo "mautrix-whatsapp-call local preflight: PASS"
if [[ "${MATRIXRTC_ENABLE_CALLS:-0}" != "1" ]]; then
    echo "Calls remain fail-closed: set MATRIXRTC_ENABLE_CALLS=1 only after provisioning the required ghost identity."
fi
