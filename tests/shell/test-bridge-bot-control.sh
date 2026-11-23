#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EVENTS="$ROOT/bridge/pkg/connector/matrixrtc_events.go"
MIGRATE="$ROOT/deployment/mautrix-whatsapp-call/migrate-rooms.sh"
grep -F 'rejectMatrixRTCBridgeBotControlCall' "$EVENTS" >/dev/null
grep -F 'Declined MatrixRTC call to WhatsApp bridge bot' "$EVENTS" >/dev/null
grep -F 'transfer_bot_control_power_to_bot' "$MIGRATE" >/dev/null
grep -F 'via human self-demotion' "$MIGRATE" >/dev/null
grep -F 'runtime decline remains active' "$MIGRATE" >/dev/null
grep -F 'DP_WHOAMI_WITH_CODE' "$MIGRATE" >/dev/null
if grep -F '.users[$bot] = $level' "$MIGRATE" >/dev/null; then
    echo "Unsafe bot-signed bridge-bot power escalation is still present" >&2
    exit 1
fi
bash "$MIGRATE" --self-test
echo "Bridge-bot control-room guards: PASS"
