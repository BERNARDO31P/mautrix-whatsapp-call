#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EVENTS="$ROOT/bridge/pkg/connector/matrixrtc_events.go"
MIGRATE="$ROOT/deployment/mautrix-whatsapp-call/migrate-rooms.sh"
grep -F 'rejectMatrixRTCBridgeBotControlCall' "$EVENTS" >/dev/null
grep -F 'Declined MatrixRTC call to WhatsApp bridge bot' "$EVENTS" >/dev/null
grep -F 'RTC UI restriction would require changing room ownership' "$MIGRATE" >/dev/null
grep -F 'runtime decline remains active' "$MIGRATE" >/dev/null
if grep -E 'transfer_bot_control_power_to_bot|self-demotion|\.users\[.*\] *=[^=]' "$MIGRATE" >/dev/null; then
    echo "Unsafe room ownership mutation is still present" >&2
    exit 1
fi
bash "$MIGRATE" --self-test
echo "Bridge-bot control-room guards: PASS"
