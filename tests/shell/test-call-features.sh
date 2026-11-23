#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

for needle in StartVoIPGroupCall matrix_video_start matrix_video_stop matrix_video_mute matrix_audio_mute whatsapp_video_state rtcRoomForCall groupVideoSourceSelector; do
    grep -R -F "$needle" "$ROOT/bridge/pkg/connector" >/dev/null
done
for needle in SetMuted GroupJID BuildMuteV2; do
    grep -R -F "$needle" "$ROOT/bridge/third_party/meowcaller" >/dev/null
done
for needle in 'CallKind::Group' matrix_video_start whatsapp_video_state set_local_muted 'unpublish(MediaStreamKind::Camera)' group_jid; do
    grep -R -F "$needle" "$ROOT/rtc/matrixrtc-worker" >/dev/null
done
grep -F 'select_bot_control_room' "$ROOT/deployment/mautrix-whatsapp-call/migrate-rooms.sh" >/dev/null
grep -F 'select_group_candidate' "$ROOT/deployment/mautrix-whatsapp-call/migrate-rooms.sh" >/dev/null
grep -F 'ghosts: set[str] = {bot_mxid}' "$ROOT/deployment/mautrix-whatsapp-call/worker/auto-provision-whatsapp-ghosts.py" >/dev/null
echo "Call feature source guards: PASS"
