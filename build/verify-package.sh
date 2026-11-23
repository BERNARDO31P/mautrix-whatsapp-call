#!/usr/bin/env bash
set -Eeuo pipefail
INPUT="${1:?usage: verify-package.sh <release-directory-or-zip>}"
TMP=""
cleanup() { [[ -z "$TMP" ]] || rm -rf "$TMP"; }
trap cleanup EXIT
if [[ -f "$INPUT" ]]; then
    TMP="$(mktemp -d)"
    unzip -q "$INPUT" -d "$TMP"
    ROOT="$(find "$TMP" -mindepth 1 -maxdepth 1 -type d -print -quit)"
else
    ROOT="$(cd "$INPUT" && pwd)"
fi
[[ -d "$ROOT" ]] || { echo "Release root not found" >&2; exit 40; }
for rel in \
    bin/mautrix-whatsapp-call-bridge \
    bin/mautrix-whatsapp-call-worker \
    bin/mautrix-whatsapp-call-provision \
    deployment/mautrix-whatsapp-call/compose.yml \
    deployment/mautrix-whatsapp-call/install-or-upgrade.sh \
    deployment/mautrix-whatsapp-call/render-hostname.py \
    deployment/mautrix-whatsapp-call/bridge/Dockerfile \
    deployment/mautrix-whatsapp-call/bridge/mautrix-whatsapp-call-bridge \
    deployment/mautrix-whatsapp-call/worker/Dockerfile \
    deployment/mautrix-whatsapp-call/worker/mautrix-whatsapp-call-worker \
    deployment/mautrix-whatsapp-call/worker/mautrix-whatsapp-call-provision \
    SOURCE-VERSIONS.env SHA256SUMS; do
    [[ -f "$ROOT/$rel" ]] || { echo "Missing release file: $rel" >&2; exit 41; }
done
(cd "$ROOT" && sha256sum -c SHA256SUMS >/dev/null)

bad_mode="$(find "$ROOT" -xdev \( -perm -0020 -o -perm -0002 \) -print -quit)"
if [[ -n "$bad_mode" ]]; then
    echo "Group/world-writable path in runtime package: $bad_mode" >&2
    exit 44
fi

for executable in     "$ROOT/bin/mautrix-whatsapp-call-bridge"     "$ROOT/bin/mautrix-whatsapp-call-worker"     "$ROOT/bin/mautrix-whatsapp-call-provision"     "$ROOT/deployment/mautrix-whatsapp-call/install-or-upgrade.sh"     "$ROOT/deployment/mautrix-whatsapp-call/verify-worker.sh"     "$ROOT/deployment/mautrix-whatsapp-call/bridge/mautrix-whatsapp-call-bridge"     "$ROOT/deployment/mautrix-whatsapp-call/worker/mautrix-whatsapp-call-worker"     "$ROOT/deployment/mautrix-whatsapp-call/worker/mautrix-whatsapp-call-provision"; do
    [[ -x "$executable" ]] || { echo "Expected executable bit missing: $executable" >&2; exit 45; }
done

for bin in "$ROOT"/bin/*; do
    file -b "$bin" | grep -q "ELF" || { echo "Not an ELF binary: $bin" >&2; exit 42; }
done
grep -F 'image: "mautrix-whatsapp-call-bridge:local"' "$ROOT/deployment/mautrix-whatsapp-call/compose.yml" >/dev/null
grep -F 'image: "mautrix-whatsapp-call-worker:local"' "$ROOT/deployment/mautrix-whatsapp-call/compose.yml" >/dev/null
grep -F 'RTC_WORKER_ENABLE_CALLS: "${MATRIXRTC_ENABLE_CALLS:-0}"' "$ROOT/deployment/mautrix-whatsapp-call/compose.yml" >/dev/null
grep -F '${MATRIXRTC_SERVER_NAME:?MATRIXRTC_SERVER_NAME is required}' "$ROOT/deployment/mautrix-whatsapp-call/compose.yml" >/dev/null
if grep -R -n "matrix\.bernardo\.fm" "$ROOT/deployment"; then
    echo "Site-specific hostname leaked into release" >&2
    exit 43
fi
echo "Runtime package verification: PASS"
