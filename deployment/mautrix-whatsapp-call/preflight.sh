#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MATRIX_ROOT="$(cd "$ROOT/.." && pwd)"
BRIDGE_CONFIG="${1:-$MATRIX_ROOT/mautrix-whatsapp/config.yaml}"
fail() { echo "ERROR: $*" >&2; exit 1; }
[[ -f "$MATRIX_ROOT/docker-compose.yml" ]] || fail "missing $MATRIX_ROOT/docker-compose.yml"
[[ -f "$BRIDGE_CONFIG" ]] || fail "missing mautrix-whatsapp config: $BRIDGE_CONFIG"
for path in \
    "$ROOT/worker/Dockerfile" \
    "$ROOT/worker/mautrix-whatsapp-call-worker" \
    "$ROOT/worker/mautrix-whatsapp-call-provision" \
    "$ROOT/worker/provision-ghost.py" \
    "$ROOT/generated/matrixrtc_store_passphrase"; do
    [[ -f "$path" ]] || fail "missing $path (run prepare.sh and install the worker files)"
done
mode="$(stat -c '%a' "$ROOT/generated/matrixrtc_store_passphrase")"
[[ "$mode" == "600" ]] || fail "matrixrtc_store_passphrase must be mode 600, got $mode"
python3 - "$BRIDGE_CONFIG" <<'PY'
from pathlib import Path
import re, sys
text = Path(sys.argv[1]).read_text()
checks = {
    "experimental_whatsapp_voip": r"(?m)^\s{4}experimental_whatsapp_voip\s*:\s*true\s*(?:#.*)?$",
    "rtc_gateway_socket": r'''(?m)^\s{4}rtc_gateway_socket\s*:\s*["']?/run/mautrix-whatsapp-call/worker\.sock["']?\s*(?:#.*)?$''',
}
for name, pattern in checks.items():
    if not re.search(pattern, text):
        raise SystemExit(f"bridge config does not enable expected network.{name}")
print("Bridge RTC gateway config: PASS")
PY
python3 - <<PY
import yaml
from pathlib import Path
yaml.safe_load(Path("$ROOT/compose.yml").read_text())
print("Compose overlay YAML parse: PASS")
PY
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose -f "$MATRIX_ROOT/docker-compose.yml" -f "$ROOT/compose.yml" config >/dev/null
    echo "Docker Compose merged config: PASS"
else
    echo "Docker Compose CLI unavailable here; merged-config validation skipped."
fi
echo "mautrix-whatsapp-call local preflight: PASS"
if [[ "${MATRIXRTC_ENABLE_CALLS:-0}" != "1" ]]; then
    echo "Calls remain fail-closed: set MATRIXRTC_ENABLE_CALLS=1 only after provisioning the required ghost identity."
fi
