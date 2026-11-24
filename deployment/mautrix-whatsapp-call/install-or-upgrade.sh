#!/usr/bin/env bash
set -euo pipefail

MATRIX_ROOT="${MATRIX_ROOT:-/opt/matrix}"
RTC_ROOT="$MATRIX_ROOT/mautrix-whatsapp-call"
SERVER="${MATRIXRTC_SERVER_NAME:-}"

if [[ -z "$SERVER" && -f "$MATRIX_ROOT/.env" ]]; then
    SERVER="$(sed -n 's/^MATRIXRTC_SERVER_NAME=//p' "$MATRIX_ROOT/.env" | tail -1 | tr -d '\r\"')"
fi
[[ -n "$SERVER" ]] || { echo "MATRIXRTC_SERVER_NAME is required" >&2; exit 2; }
[[ "$SERVER" =~ ^[A-Za-z0-9.-]+$ && "$SERVER" == *.* ]] || { echo "Invalid MATRIXRTC_SERVER_NAME: $SERVER" >&2; exit 2; }
[[ -f "$MATRIX_ROOT/docker-compose.yml" ]] || { echo "Missing $MATRIX_ROOT/docker-compose.yml" >&2; exit 2; }
[[ -f "$MATRIX_ROOT/mautrix-whatsapp/config.yaml" ]] || { echo "Missing mautrix-whatsapp config.yaml" >&2; exit 2; }
[[ -f "$RTC_ROOT/compose.yml" ]] || { echo "Missing mautrix-whatsapp-call Compose overlay" >&2; exit 2; }
# Prevent concurrent mutations by two installers. The lock lives outside the
# rsync-managed deployment tree and survives atomic replacements within it.
exec 8>"$MATRIX_ROOT/.mautrix-whatsapp-call-install.lock"
flock -n 8 || { echo "Another call installer is running" >&2; exit 2; }
# Confirm the identity domain before writing any registrations/configuration.
python3 - "$MATRIX_ROOT/synapse/homeserver.yaml" "$SERVER" <<'PYDOMAIN'
import sys,yaml
with open(sys.argv[1]) as stream:
    config=yaml.safe_load(stream)
if config.get("server_name") != sys.argv[2]:
    raise SystemExit("MATRIXRTC_SERVER_NAME must equal the existing Synapse server_name")
PYDOMAIN

normalize_deployment_permissions() {
    # Normalize only shipped public files. Unknown local files may contain
    # credentials or private backups and must never be made world-readable.
    local rel mode
    [[ ! -L "$RTC_ROOT" ]] || return 1
    for rel in bridge worker generated worker-state; do
        if [[ -L "$RTC_ROOT/$rel" ]]; then
            echo "Refusing symbolic link: $rel" >&2; return 1
        fi
    done
    for rel in \
        "INSTALLATION.md" \
        "OPERATIONS.md" \
        "README.md" \
        "bridge/Dockerfile" \
        "bridge/.dockerignore" \
        "compose.yml" \
        "config_files.py" \
        "configure-bridge.py" \
        "install-or-upgrade.sh" \
        "merge-synapse.py" \
        "migrate-rooms.sh" \
        "nginx-443-locations.conf" \
        "preflight.sh" \
        "prepare.sh" \
        "provision-ghost.sh" \
        "reconcile-credentials.py" \
        "render-hostname.py" \
        "resolve-appservice.py" \
        "server-cleanup.sh" \
        "synapse-additions.yaml" \
        "sync-direct-chats.sh" \
        "verify-worker.sh" \
        "verify.sh" \
        "worker/Dockerfile" \
        "worker/.dockerignore" \
        "worker/auto-provision-whatsapp-ghosts.py" \
        "worker/provision-ghost.py" \
        "worker/session_files.py" \
        "bridge/mautrix-whatsapp-call-bridge" \
        "worker/mautrix-whatsapp-call-worker" \
        "worker/mautrix-whatsapp-call-provision"; do
        if [[ -L "$RTC_ROOT/$rel" ]]; then
            echo "Refusing symbolic link: $rel" >&2; return 1
        fi
        [[ -f "$RTC_ROOT/$rel" ]] || continue
        case "$rel" in
            *.py|*.sh|*/mautrix-whatsapp-call-bridge|*/mautrix-whatsapp-call-worker|*/mautrix-whatsapp-call-provision) mode=0755 ;;
            *) mode=0644 ;;
        esac
        chmod "$mode" "$RTC_ROOT/$rel"
    done
    chmod 0755 "$RTC_ROOT" "$RTC_ROOT/bridge" "$RTC_ROOT/worker"

    if [[ -d "$RTC_ROOT/generated" ]]; then
        chmod 0700 "$RTC_ROOT/generated"
        find "$RTC_ROOT/generated" -xdev -type d -exec chmod 0700 {} +
        find "$RTC_ROOT/generated" -xdev -type f -exec chmod 0600 {} +
    fi
    if [[ -d "$RTC_ROOT/worker-state" ]]; then
        chmod 0700 "$RTC_ROOT/worker-state"
        find "$RTC_ROOT/worker-state" -xdev -type d -exec chmod 0700 {} +
        find "$RTC_ROOT/worker-state" -xdev -type f -exec chmod 0600 {} +
    fi
}
normalize_deployment_permissions

env_setting() {
    python3 - "$MATRIX_ROOT/.env" "$1" <<'PY'
from pathlib import Path
import re,shlex,sys
p=Path(sys.argv[1])
value=""
if p.is_file():
    for line in p.read_text().splitlines():
        match=re.fullmatch(r"\s*(?:export\s+)?"+re.escape(sys.argv[2])+r"\s*=\s*(.*)",line)
        if match:
            value=" ".join(shlex.split(match[1],comments=True))
print(value)
PY
}
WA_REG_VALUE="${MATRIXRTC_WHATSAPP_REGISTRATION_PATH:-$(env_setting MATRIXRTC_WHATSAPP_REGISTRATION_PATH)}"
WA_REG_VALUE="${WA_REG_VALUE:-./mautrix-whatsapp/registration.yaml}"
if [[ "$WA_REG_VALUE" = /* ]]; then
    WA_REG_HOST="$WA_REG_VALUE"
else
    WA_REG_HOST="$MATRIX_ROOT/${WA_REG_VALUE#./}"
fi
[[ -f "$WA_REG_HOST" ]] || {
    echo "Missing mautrix-whatsapp appservice registration: $WA_REG_HOST" >&2
    echo "Set MATRIXRTC_WHATSAPP_REGISTRATION_PATH to the host path used by your bridge if it is stored elsewhere." >&2
    exit 2
}

export MATRIXRTC_WHATSAPP_REGISTRATION_PATH="$WA_REG_HOST"
export MATRIXRTC_WHATSAPP_REGISTRATION="$WA_REG_HOST"
export MATRIXRTC_SERVER_NAME="$SERVER"
export MATRIX_ROOT
cd "$MATRIX_ROOT"

compose() {
    docker compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml "$@"
}

verify_runtime_wiring() {
    local expected_calls="$1"
    local socket_gid="${MATRIXRTC_SOCKET_GID:-}"
    if [[ -z "$socket_gid" && -f "$MATRIX_ROOT/.env" ]]; then
        socket_gid="$(sed -n 's/^MATRIXRTC_SOCKET_GID=//p' "$MATRIX_ROOT/.env" | tail -1 | tr -d '\r"')"
    fi
    socket_gid="${socket_gid:-29319}"
    [[ "$socket_gid" =~ ^[0-9]+$ ]] || { echo "Invalid MATRIXRTC_SOCKET_GID: $socket_gid" >&2; return 1; }

    local bridge_gid socket_meta worker_id worker_user worker_calls jwt_id jwt_override
    bridge_gid="$(compose exec -T mautrix-whatsapp sh -c "awk '/^Gid:/ {print \$2; exit}' /proc/1/status")"
    [[ "$bridge_gid" == "$socket_gid" ]] || {
        echo "mautrix-whatsapp PID 1 GID is $bridge_gid, expected $socket_gid" >&2
        return 1
    }

    socket_meta="$(compose exec -T mautrix-whatsapp stat -c '%u:%g %a' /run/mautrix-whatsapp-call/worker.sock)"
    [[ "$socket_meta" == "0:$socket_gid 660" ]] || {
        echo "MatrixRTC worker socket is $socket_meta, expected 0:$socket_gid 660" >&2
        return 1
    }

    worker_id="$(compose ps -q mautrix-whatsapp-call-worker)"
    [[ -n "$worker_id" ]] || { echo "mautrix-whatsapp-call-worker container is missing" >&2; return 1; }
    worker_user="$(docker inspect "$worker_id" --format '{{.Config.User}}')"
    [[ "$worker_user" == "0:$socket_gid" ]] || {
        echo "mautrix-whatsapp-call-worker user is $worker_user, expected 0:$socket_gid" >&2
        return 1
    }
    worker_calls="$(docker inspect "$worker_id" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^RTC_WORKER_ENABLE_CALLS=//p' | tail -1)"
    [[ "$worker_calls" == "$expected_calls" ]] || {
        echo "mautrix-whatsapp-call-worker RTC_WORKER_ENABLE_CALLS is $worker_calls, expected $expected_calls" >&2
        return 1
    }

    jwt_id="$(compose ps -q lk-jwt-service)"
    [[ -n "$jwt_id" ]] || { echo "lk-jwt-service container is missing" >&2; return 1; }
    jwt_override="$(docker inspect "$jwt_id" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^LIVEKIT_CS_API_URL_OVERRIDES=//p' | tail -1)"
    [[ "$jwt_override" == "$SERVER=http://synapse:8008" ]] || {
        echo "lk-jwt-service Client-Server override is not configured for $SERVER" >&2
        return 1
    }

    python3 - "$MATRIX_ROOT/mautrix-whatsapp/config.yaml" <<'PY'
import sys,yaml
network=yaml.safe_load(open(sys.argv[1]))["network"]
if not (network["call_start_notices"] is False and network["experimental_whatsapp_voip"] is True and network["rtc_gateway_socket"]=="/run/mautrix-whatsapp-call/worker.sock"):
    raise SystemExit("bridge RTC wiring mismatch")
PY
    echo "mautrix-whatsapp-call runtime wiring: PASS (bridge_gid=$socket_gid socket=0:$socket_gid/660 calls=$expected_calls)"
}

CALLS_SETTING="$(env_setting MATRIXRTC_ENABLE_CALLS)"
case "${CALLS_SETTING,,}" in
    1|true|yes) RESTORE_CALLS=1 ;;
    0|false|no|"") RESTORE_CALLS=0 ;;
    *) echo "Invalid MATRIXRTC_ENABLE_CALLS in .env" >&2; exit 2 ;;
esac
INSTALL_COMPLETE=0
WORKER_PAUSED=0
restore_worker() {
    MATRIXRTC_ENABLE_CALLS="$RESTORE_CALLS" compose up -d --no-deps --force-recreate mautrix-whatsapp-call-worker || return 1
    local worker_id actual
    worker_id="$(compose ps -q mautrix-whatsapp-call-worker)" || return 1
    [[ -n "$worker_id" ]] || return 1
    actual="$(docker inspect "$worker_id" --format '{{range .Config.Env}}{{println .}}{{end}}')" || return 1
    [[ "$(docker inspect "$worker_id" --format '{{.State.Running}}')" == true ]] && grep -Fxq "RTC_WORKER_ENABLE_CALLS=$RESTORE_CALLS" <<<"$actual"
}
restore_calls_on_error() {
    local rc=$?
    if (( rc && WORKER_PAUSED && !INSTALL_COMPLETE )); then
        echo "Installation incomplete; stopping worker and provisioner. Saved .env is unchanged." >&2
        MATRIXRTC_ENABLE_CALLS=0 compose stop mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner || echo "CRITICAL: stop the call worker and provisioner manually" >&2
    fi
    exit "$rc"
}
trap restore_calls_on_error EXIT

# Stop calls and the background scanner before modifying live files. A failed
# installation must not automatically re-enable calls on a partial stack.
# Stop (rather than create) existing containers: on a fresh install they may not exist yet.
WORKER_PAUSED=1
MATRIXRTC_ENABLE_CALLS=0 compose stop mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner

# Re-rendering is harmless after placeholders are already resolved.
python3 "$RTC_ROOT/render-hostname.py" --server-name "$SERVER" --root "$RTC_ROOT" >/dev/null

# Generate or preserve all secrets. Existing generated files are never rotated.
bash "$RTC_ROOT/prepare.sh"

# Enable the RTC gateway, supported mautrix appservice double puppeting and
# native m.direct synchronization. No human access token is requested/stored.
python3 "$RTC_ROOT/configure-bridge.py" \
    --config "$MATRIX_ROOT/mautrix-whatsapp/config.yaml" \
    --server-name "$SERVER" \
    --double-puppet-secret-file "$RTC_ROOT/generated/double_puppet_secret" \
    --apply

# Install both MatrixRTC-related appservice registrations into Synapse.
python3 "$RTC_ROOT/merge-synapse.py" --apply

# Validate the merged Compose project before restarting anything.
docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    config >/dev/null

# Build the exact bridge/worker/provisioner shipped by this release.
docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    build mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner mautrix-whatsapp

# Keep calls fail-closed while Synapse loads the new appservice identity and
# existing portal metadata/ghost identities are migrated. This environment
# override does not edit .env, so the administrator's configured setting is kept.
MATRIXRTC_ENABLE_CALLS=0 compose up -d --no-deps --force-recreate mautrix-whatsapp-call-worker
MATRIXRTC_ENABLE_CALLS=0 compose up -d --no-deps --force-recreate livekit

MATRIXRTC_ENABLE_CALLS=0 docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    up -d --no-deps --force-recreate synapse

for _ in $(seq 1 60); do
    if curl --connect-timeout 5 --max-time 10 -fsS http://127.0.0.1:8008/_matrix/client/versions >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
curl --connect-timeout 5 --max-time 10 -fsS http://127.0.0.1:8008/_matrix/client/versions >/dev/null

MATRIXRTC_ENABLE_CALLS=0 docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    up -d --no-deps --force-recreate lk-jwt-service

MATRIXRTC_ENABLE_CALLS=0 docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    up -d --no-deps --force-recreate mautrix-whatsapp mautrix-whatsapp-call-worker

for _ in $(seq 1 60); do
    if bash "$RTC_ROOT/verify-worker.sh" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
bash "$RTC_ROOT/verify-worker.sh"
verify_runtime_wiring 0

# Existing portals: add RTC-specific event power-level overrides and m.direct
# mappings. Future portals receive the power levels from the bridge creation
# path, while mautrix sync_direct_chat_list + automatic double puppeting keeps
# m.direct current without an admin/user token.
MATRIXRTC_FAIL_ON_UNENCRYPTED_DMS=0 bash "$RTC_ROOT/migrate-rooms.sh"

# Existing eligible encrypted DM ghosts: provision persistent cross-signed RTC
# devices using the dedicated sidecar. The main call worker never sees the
# mautrix appservice registration or token.
docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    run --rm --no-deps \
    mautrix-whatsapp-call-provisioner \
    python3 /usr/local/lib/mautrix-whatsapp-call/auto-provision-whatsapp-ghosts.py --once

# Future DMs: continuously scan out-of-band. The scanner is idempotent and
# refuses unencrypted rooms, groups, rooms without the functional bridge bot,
# non-local humans and users outside the bridge's registered ghost namespaces.
docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    up -d --no-deps --force-recreate mautrix-whatsapp-call-provisioner

# Restore the administrator's previous call-enable setting. If it was enabled,
# recreate only the worker so it receives RTC_WORKER_ENABLE_CALLS=1 again.
restore_worker

for ((attempt=0; attempt<60; attempt++)); do
    if bash "$RTC_ROOT/verify-worker.sh" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
bash "$RTC_ROOT/verify-worker.sh"
verify_runtime_wiring "$RESTORE_CALLS"

compose ps mautrix-whatsapp mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner livekit lk-jwt-service synapse

INSTALL_COMPLETE=1
trap - EXIT

echo "mautrix-whatsapp-call install/upgrade migration: PASS"
echo "Existing WhatsApp DMs, RTC permissions, m.direct mappings and ghost identities were migrated automatically."
echo "Future WhatsApp DMs receive RTC permissions at creation; mautrix keeps m.direct synchronized via appservice double puppeting; the dedicated provisioner creates persistent cross-signed RTC identities out-of-band."
echo "The main call worker does not receive the WhatsApp appservice registration."
echo "Existing MATRIXRTC_ENABLE_CALLS setting restored: $RESTORE_CALLS"
echo "Force-stop/reopen Element clients once after an upgrade so cached call eligibility is refreshed."
