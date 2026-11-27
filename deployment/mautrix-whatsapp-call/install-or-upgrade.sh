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
# Never obscure a previous interrupted upgrade with a second transaction.
require_no_pending_journal() {
    local journal
    for journal in "$MATRIX_ROOT"/.matrixrtc-config-rollback.*; do
        if [[ -e "$journal" || -L "$journal" ]]; then
            echo "Unresolved private configuration journal: $journal; review recovery before retrying" >&2
            return 1
        fi
    done
}
require_no_pending_journal || exit 2
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
        "validate-candidate.py" \
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
        "worker/ghost_namespace.py" \
        "worker/portal_kind.py" \
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

# The overlay defines a bridge service itself. Ensure it extends the existing
# service instead of accidentally creating a new bridge with no session mounts.
verify_existing_base_services() {
    local services service
    services="$(docker compose -f docker-compose.yml config --services)" || return 1
    for service in synapse mautrix-whatsapp; do
        if ! grep -Fxq "$service" <<<"$services"; then
            echo "Existing Compose service missing: $service" >&2
            return 1
        fi
    done
}
verify_existing_base_services

# Room alias semantics changed in lk-jwt-service 0.10.0. Never silently
# replace an existing service using a different contract. Check stopped
# containers too, and the rendered Compose image before modifying files.
JWT_IMAGE="ghcr.io/element-hq/lk-jwt-service:0.9.0"
JWT_IMAGE_ID=""
verify_jwt_image_contract() {
    local ids id image image_id local_id existing_id=""
    compose config --format json | python3 -c '
import json,sys
service=json.load(sys.stdin)["services"]["lk-jwt-service"]
if (service.get("image") != sys.argv[1] or service.get("build") is not None
    or service.get("pull_policy", "missing") not in ("missing", "if_not_present", "never")):
    raise SystemExit("JWT image contract mismatch in merged Compose configuration")
' "$JWT_IMAGE" || return 1
    ids="$(compose ps --all --quiet lk-jwt-service)" || return 1
    while IFS= read -r id; do
        [[ -n "$id" ]] || continue
        image="$(docker inspect --format '{{.Config.Image}}' "$id")" || return 1
        image_id="$(docker inspect --format '{{.Image}}' "$id")" || return 1
        if [[ "$image" != "$JWT_IMAGE" || ! "$image_id" =~ ^sha256:[0-9a-f]{64}$ || ( -n "$existing_id" && "$existing_id" != "$image_id" ) ]]; then
            echo "Existing JWT service uses a different image; refusing automatic replacement (room alias compatibility requires review)." >&2
            return 1
        fi
        existing_id="$image_id"
    done <<<"$ids"
    if ! local_id="$(docker image inspect --format '{{.Id}}' "$JWT_IMAGE" 2>/dev/null)"; then
        # Never pull a replacement for an already deployed image or after
        # preflight. Fresh installs may fetch the pinned tag before mutation.
        [[ -z "$existing_id" && -z "$JWT_IMAGE_ID" ]] || return 1
        docker pull "$JWT_IMAGE" || return 1
        local_id="$(docker image inspect --format '{{.Id}}' "$JWT_IMAGE")" || return 1
    fi
    if [[ ! "$local_id" =~ ^sha256:[0-9a-f]{64}$ || ( -n "$existing_id" && "$existing_id" != "$local_id" ) || ( -n "$JWT_IMAGE_ID" && "$JWT_IMAGE_ID" != "$local_id" ) ]]; then
        echo "JWT image identity changed; refusing automatic replacement (room alias compatibility requires review)." >&2
        return 1
    fi
    JWT_IMAGE_ID="$local_id"
}
verify_running_jwt_image() {
    local ids id actual running count=0
    ids="$(compose ps --quiet lk-jwt-service)" || return 1
    while IFS= read -r id; do
        [[ -n "$id" ]] || continue
        actual="$(docker inspect --format '{{.Image}}' "$id")" || return 1
        running="$(docker inspect --format '{{.State.Running}}' "$id")" || return 1
        [[ -n "$JWT_IMAGE_ID" && "$actual" == "$JWT_IMAGE_ID" && "$running" == true ]] || { echo "JWT service running image differs from verified image" >&2; return 1; }
        count=$((count + 1))
    done <<<"$ids"
    [[ "$count" == 1 ]] || { echo "Expected one running JWT service" >&2; return 1; }
}
verify_jwt_image_contract
# The overlay must preserve existing bridge data/config mounts, and both
# call endpoints must use the same writable named volume. Validate resolved
# Compose models, not the unexpanded YAML or the container's runtime state.
verify_call_mount_contract() {
    python3 - <<'PYMOUNTS'
import json
import os
from pathlib import Path
import subprocess


def model(overlay=False):
    args = ["docker", "compose", "-f", "docker-compose.yml"]
    if overlay:
        args += ["-f", "mautrix-whatsapp-call/compose.yml"]
    result = subprocess.run(args + ["config", "--format", "json"],
                            capture_output=True, check=True, timeout=30)
    return json.loads(result.stdout)


def mounts(config, service):
    result = {}
    for mount in config["services"][service].get("volumes", []):
        target = mount["target"]
        if target in result:
            raise ValueError("Duplicate mount target")
        result[target] = mount
    return result


def verify_config_directory(config, service, relative, critical_files):
    # The installer edits these host directories directly. A different /data
    # source or an overriding file mount would leave the live config unchanged.
    root = Path(os.environ["MATRIX_ROOT"]).resolve()
    entries = mounts(config, service)
    data = entries.get("/data", {})
    if (data.get("type") != "bind" or not data.get("source")
            or Path(data["source"]).resolve() != (root / relative).resolve()
            or data.get("read_only", False) is not False):
        raise ValueError("Unexpected configuration directory mount")
    for target in entries:
        if target == "/data":
            continue
        normalized = os.path.normpath(target)
        for name in critical_files:
            path = "/data/" + name
            if normalized == path or path.startswith(normalized.rstrip("/") + "/"):
                raise ValueError("Configuration file shadowed by another mount")


def verify_call_bindings(config):
    root = Path(os.environ["MATRIX_ROOT"]).resolve()
    rtc = root / "mautrix-whatsapp-call"
    state_target = "/var/lib/mautrix-whatsapp-call"
    passphrase_target = "/run/secrets/matrixrtc_store_passphrase"
    registration_target = "/run/secrets/mautrix-whatsapp-registration.yaml"
    config_target = "/run/secrets/mautrix-whatsapp-config.yaml"

    def require_bind(entries, target, source, read_only):
        mount = entries.get(target, {})
        if (mount.get("type") != "bind" or not mount.get("source")
                or Path(mount["source"]).resolve() != source.resolve()
                or mount.get("read_only", False) is not read_only):
            raise ValueError("Unexpected call state or credential bind")
        for other in entries:
            if other == target:
                continue
            normalized = os.path.normpath(other)
            if (normalized == target or target.startswith(normalized.rstrip("/") + "/")
                    or normalized.startswith(target.rstrip("/") + "/")):
                raise ValueError("Overlapping call state or credential bind")

    for service in ("mautrix-whatsapp-call-worker", "mautrix-whatsapp-call-provisioner"):
        entries = mounts(config, service)
        require_bind(entries, state_target, rtc / "worker-state", False)
        require_bind(entries, passphrase_target, rtc / "generated/matrixrtc_store_passphrase", True)
        if service == "mautrix-whatsapp-call-provisioner":
            require_bind(entries, registration_target,
                         Path(os.environ["MATRIXRTC_WHATSAPP_REGISTRATION_PATH"]), True)
            require_bind(entries, config_target, root / "mautrix-whatsapp/config.yaml", True)
    require_bind(mounts(config, "livekit"), "/etc/livekit.yaml",
                 rtc / "generated/livekit.yaml", True)
    jwt = mounts(config, "lk-jwt-service")
    require_bind(jwt, "/run/secrets/livekit_secret", rtc / "generated/livekit_secret", True)
    require_bind(jwt, "/run/secrets/matrixrtc-registration.yaml",
                 rtc / "generated/matrixrtc-registration.yaml", True)


def verify_call_builds(config):
    # Verify the build consumes the local payload checked by preflight, not
    # a retained base-stack Dockerfile, an inline recipe or a remote context.
    rtc = Path(os.environ["MATRIX_ROOT"]).resolve() / "mautrix-whatsapp-call"
    for service, directory, image in (
            ("mautrix-whatsapp", "bridge", "mautrix-whatsapp-call-bridge:local"),
            ("mautrix-whatsapp-call-worker", "worker", "mautrix-whatsapp-call-worker:local"),
            ("mautrix-whatsapp-call-provisioner", "worker", "mautrix-whatsapp-call-worker:local")):
        entry = config["services"][service]
        build = entry["build"]
        context = Path(build["context"])
        if (not context.is_absolute() or context.resolve() != (rtc / directory).resolve()
                or entry.get("image") != image
                or entry.get("platform") not in (None, "linux/amd64")
                or build.get("platforms", []) not in ([], ["linux/amd64"])
                or build.get("dockerfile_inline") is not None
                or build.get("target")
                or build.get("additional_contexts")):
            raise ValueError("Unexpected call image build model")
        recipe = context / build.get("dockerfile", "Dockerfile")
        if recipe.resolve() != (rtc / directory / "Dockerfile").resolve():
            raise ValueError("Unexpected call image Dockerfile")


def verify_call_processes(config):
    # A retained base-stack entrypoint or executable bind can bypass the
    # validated image. Check call-owned processes before pausing services.
    services = config["services"]
    server = os.environ["MATRIXRTC_SERVER_NAME"]
    shared = {"MATRIX_HOMESERVER": "http://synapse:8008",
              "MATRIXRTC_SERVER_NAME": server,
              "RTC_WORKER_STATE_DIR": "/var/lib/mautrix-whatsapp-call",
              "RTC_WORKER_STORE_PASSPHRASE_FILE": "/run/secrets/matrixrtc_store_passphrase"}
    for service, command, extra in (
        ("mautrix-whatsapp-call-worker", ["/usr/local/bin/mautrix-whatsapp-call-worker"],
         {"RTC_WORKER_SOCKET": "/run/mautrix-whatsapp-call/worker.sock",
          "RTC_WORKER_SOCKET_MODE": "0660", "RTC_WORKER_FFMPEG": "/usr/bin/ffmpeg"}),
        ("mautrix-whatsapp-call-provisioner",
         ["python3", "/usr/local/lib/mautrix-whatsapp-call/auto-provision-whatsapp-ghosts.py"],
         {"RTC_AUTOPROVISION_REGISTRATION": "/run/secrets/mautrix-whatsapp-registration.yaml",
          "RTC_AUTOPROVISION_CONFIG": "/run/secrets/mautrix-whatsapp-config.yaml"})):
        entry = services[service]
        env = entry.get("environment", {})
        if (entry.get("entrypoint") not in (None, []) or entry.get("command") != command
                or any(env.get(key) != value for key, value in (shared | extra).items())):
            raise ValueError("Unexpected call process or environment")
        # /usr, /bin and their ancestors must come from the validated image.
        for target in mounts(config, service):
            normalized = os.path.normpath(target)
            if any(normalized == path or normalized.startswith(path + "/")
                   or path.startswith(normalized.rstrip("/") + "/")
                   for path in ("/usr", "/bin", "/lib", "/lib64")):
                raise ValueError("Call process executable path shadowed")
    health = services["mautrix-whatsapp-call-worker"].get("healthcheck", {})
    if (health.get("disable", False) is not False or health.get("test") !=
            ["CMD", "mautrix-whatsapp-call-worker", "healthcheck"]):
        raise ValueError("Unexpected worker healthcheck")


def verify_existing_service_settings(original, merged):
    # The overlay may replace the call stack and bridge executable, but it
    # must not change unrelated service settings or the existing message
    # bridge's command, entrypoint, ports, dependencies or environment.
    call_services = {"livekit", "lk-jwt-service", "mautrix-whatsapp-call-worker",
                     "mautrix-whatsapp-call-provisioner"}
    bridge = "mautrix-whatsapp"
    before_services = original["services"]
    after_services = merged["services"]
    for name, service in before_services.items():
        if name not in call_services and name != bridge:
            if after_services.get(name) != service:
                raise ValueError("Unrelated service configuration changed")

    before, after = before_services[bridge], after_services[bridge]
    permitted = {"image", "build", "volumes", "environment", "group_add"}
    if ({key: value for key, value in before.items() if key not in permitted}
            != {key: value for key, value in after.items() if key not in permitted}):
        raise ValueError("Existing bridge process configuration changed")
    before_env = before.get("environment") or {}
    after_env = after.get("environment") or {}
    allowed_env = {"GID", "MATRIXRTC_INCOMING_MEMBERSHIP_FORMAT"}
    if ({key: value for key, value in before_env.items() if key not in allowed_env}
            != {key: value for key, value in after_env.items() if key not in allowed_env}):
        raise ValueError("Existing bridge environment changed")
    old_groups = set(map(str, before.get("group_add") or []))
    new_groups = set(map(str, after.get("group_add") or []))
    if not old_groups <= new_groups or new_groups - old_groups - {str(after_env.get("GID"))}:
        raise ValueError("Existing bridge supplemental groups changed")
    old_mounts = mounts(original, bridge)
    new_mounts = mounts(merged, bridge)
    if (set(new_mounts) - set(old_mounts) - {"/run/mautrix-whatsapp-call"}):
        raise ValueError("Unexpected additional bridge mount")
    # Resource definitions are shared between services. Existing named
    # volumes, networks, configs and secrets must retain their definitions.
    for kind in ("volumes", "networks", "configs", "secrets"):
        for name, definition in (original.get(kind) or {}).items():
            if (merged.get(kind) or {}).get(name) != definition:
                raise ValueError("Existing shared resource definition changed")


def verify_call_networks(original, merged):
    # These URLs use Compose service DNS, not host-published ports. A valid
    # Compose model can still isolate its services on different networks.
    def attachments(config, service):
        entry = config["services"][service]
        if entry.get("network_mode"):
            return entry["network_mode"], {}
        networks = entry.get("networks") or {"default": None}
        if isinstance(networks, list):
            networks = dict.fromkeys(networks)
        if not isinstance(networks, dict):
            raise ValueError("Invalid service network attachments")
        return None, networks

    for service in ("synapse", "mautrix-whatsapp"):
        before = attachments(original, service)
        after = attachments(merged, service)
        if before != after:
            raise ValueError("Existing service network attachment changed")
        for network in before[1]:
            if original.get("networks", {}).get(network) != merged.get("networks", {}).get(network):
                raise ValueError("Existing service network definition changed")

    for left, right in (
            ("mautrix-whatsapp-call-worker", "synapse"),
            ("mautrix-whatsapp-call-provisioner", "synapse"),
            ("lk-jwt-service", "synapse"),
            ("livekit", "lk-jwt-service")):
        left_mode, left_networks = attachments(merged, left)
        right_mode, right_networks = attachments(merged, right)
        if left_mode or right_mode or not (left_networks.keys() & right_networks.keys()):
            raise ValueError("Required call service DNS network is missing")


try:
    original = model()
    base = mounts(original, "mautrix-whatsapp")
    merged = model(True)
    for config in (original, merged):
        verify_config_directory(config, "mautrix-whatsapp", "mautrix-whatsapp", ("config.yaml",))
        verify_config_directory(config, "synapse", "synapse", (
            "homeserver.yaml", "matrixrtc-registration.yaml", "matrixrtc-doublepuppet-registration.yaml"))
    bridge = mounts(merged, "mautrix-whatsapp")
    worker = mounts(merged, "mautrix-whatsapp-call-worker")
    if any(bridge.get(target) != mount for target, mount in base.items()):
        raise ValueError("Existing bridge mount changed")
    for mount in base.values():
        if mount.get("type") == "volume" and mount.get("source"):
            name = mount["source"]
            if original["volumes"][name] != merged["volumes"][name]:
                raise ValueError("Existing bridge volume definition changed")
    target = "/run/mautrix-whatsapp-call"
    volume = "mautrix-whatsapp-call-worker-socket"
    for mount in (bridge.get(target, {}), worker.get(target, {})):
        if (mount.get("type") != "volume" or mount.get("source") != volume
                or mount.get("read_only", False) is not False
                or mount.get("volume", {}).get("subpath")):
            raise ValueError("Invalid call socket mount")
    if volume not in merged["volumes"]:
        raise ValueError("Missing call socket volume")
    verify_call_bindings(merged)
    verify_call_builds(merged)
    verify_call_processes(merged)
    verify_call_networks(original, merged)
    verify_existing_service_settings(original, merged)
except (KeyError, TypeError, AttributeError, ValueError, OSError, subprocess.SubprocessError):
    # Resolved Compose output can contain secrets. Never print it or exceptions.
    raise SystemExit("Existing configuration directories, bridge mounts, call socket or call state/credential binds, image builds, process, network or existing service settings failed prevalidation; refusing installation.") from None
print("Configuration directories, bridge mount preservation, call socket and call state/credential binds, image builds, process, network and existing service settings: PASS")
PYMOUNTS
}
verify_call_mount_contract
# Check all shipped build inputs before stopping any existing call service.
for directory in "$RTC_ROOT" "$RTC_ROOT/bridge" "$RTC_ROOT/worker"; do
    [[ -d "$directory" && ! -L "$directory" ]] || { echo "Invalid call build directory" >&2; exit 2; }
done
bash "$RTC_ROOT/preflight.sh" --payload-only
# Validate the planned YAML and surviving credentials before pausing calls.
CANDIDATE_FINGERPRINT="$(python3 -B "$RTC_ROOT/validate-candidate.py" --server-name "$SERVER" --fingerprint)"
[[ "$CANDIDATE_FINGERPRINT" =~ ^[a-f0-9]{64}$ ]] || exit 1
assert_candidate_unchanged() {
    local current
    current="$(python3 -B "$RTC_ROOT/validate-candidate.py" --server-name "$SERVER" --fingerprint)" || return 1
    [[ "$current" == "$CANDIDATE_FINGERPRINT" ]] || {
        echo "Deployment inputs changed after validation; refusing to apply stale configuration." >&2
        return 1
    }
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
    [[ "$(docker inspect --format '{{.Config.Image}}' "$jwt_id")" == "$JWT_IMAGE" ]] || {
        echo "JWT service runtime image differs from the supported pin" >&2
        return 1
    }
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
# Bound CLI waiting as well as Docker's configured per-service stop grace.
# A successful stop request is not proof that a scanner/worker has stopped.
command -v timeout >/dev/null || { echo "GNU timeout is required" >&2; exit 2; }
INSTALL_COMPLETE=0
WORKER_PAUSED=0
LIVE_JOURNAL=""
LIVE_WRITES_STARTED=0
# Build before any service shutdown or configuration/credential mutation.
# Local image tags may change, but a running container is not recreated by build.
# Both Dockerfiles consume only the validated, shipped build inputs.
compose build mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner mautrix-whatsapp || {
    echo "Call image build failed; no call services have been paused" >&2
    exit 1
}
# Recheck resolved Compose and local candidate inputs after the potentially long build.
verify_existing_base_services
verify_jwt_image_contract
verify_call_mount_contract
assert_candidate_unchanged
normalize_deployment_permissions
stop_call_services() {
    local ids statuses status id
    local -a compose_args=(-f docker-compose.yml -f mautrix-whatsapp-call/compose.yml)
    local -a services=(mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner)
    local -a containers=() observed=()
    MATRIXRTC_ENABLE_CALLS=0 timeout --kill-after=5s 75s docker compose "${compose_args[@]}" stop "${services[@]}" || return 1
    ids="$(MATRIXRTC_ENABLE_CALLS=0 timeout --kill-after=5s 10s docker compose "${compose_args[@]}" ps --all --quiet "${services[@]}")" || return 1
    # An empty successful listing is normal on a fresh installation.
    [[ -n "$ids" ]] || return 0
    mapfile -t containers <<<"$ids"
    for id in "${containers[@]}"; do
        [[ "$id" =~ ^[[:xdigit:]]{12,64}$ ]] || return 1
    done
    statuses="$(timeout --kill-after=5s 10s docker inspect --format '{{.State.Status}}' "${containers[@]}")" || return 1
    mapfile -t observed <<<"$statuses"
    [[ "${#containers[@]}" -eq "${#observed[@]}" ]] || return 1
    for status in "${observed[@]}"; do
        case "$status" in
            exited|created) ;;
            *) echo "Call service did not reach stopped state: $status" >&2; return 1 ;;
        esac
    done
}
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
        if (( LIVE_WRITES_STARTED )) && [[ -n "$LIVE_JOURNAL" ]]; then
            if python3 -B "$RTC_ROOT/validate-candidate.py" --server-name "$SERVER" --restore-live "$LIVE_JOURNAL"; then
                rm -rf -- "$LIVE_JOURNAL" || echo "Private rollback journal retained: $LIVE_JOURNAL" >&2
            else
                echo "CRITICAL: live configuration rollback incomplete; private journal: $LIVE_JOURNAL" >&2
            fi
        elif [[ -n "$LIVE_JOURNAL" ]]; then
            echo "Private configuration journal retained: $LIVE_JOURNAL" >&2
        fi
        echo "Installation incomplete; stopping worker and provisioner. Saved .env is unchanged." >&2
        stop_call_services || echo "CRITICAL: verify and stop the call worker and provisioner manually" >&2
    fi
    exit "$rc"
}
trap restore_calls_on_error EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Stop calls and the background scanner before modifying live files. A failed
# installation must not automatically re-enable calls on a partial stack.
# Stop (rather than create) existing containers: on a fresh install they may not exist yet.
WORKER_PAUSED=1
stop_call_services || { echo "Cannot confirm both call services stopped; refusing configuration changes" >&2; exit 1; }
assert_candidate_unchanged

# Re-rendering is harmless after placeholders are already resolved.
python3 "$RTC_ROOT/render-hostname.py" --server-name "$SERVER" --root "$RTC_ROOT" >/dev/null

# Generate or preserve all secrets. Existing generated files are never rotated.
bash "$RTC_ROOT/prepare.sh"

# Snapshot original live files after credentials are prepared and before
# changing either existing service's configuration. Keep the journal private.
LIVE_JOURNAL="$(mktemp -d "$MATRIX_ROOT/.matrixrtc-config-rollback.XXXXXX")"
python3 -B "$RTC_ROOT/validate-candidate.py" --server-name "$SERVER" --snapshot-live "$LIVE_JOURNAL"
LIVE_WRITES_STARTED=1

# Enable the RTC gateway, supported mautrix appservice double puppeting and
# native m.direct synchronization. No human access token is requested/stored.
python3 "$RTC_ROOT/configure-bridge.py" \
    --config "$MATRIX_ROOT/mautrix-whatsapp/config.yaml" \
    --server-name "$SERVER" \
    --double-puppet-secret-file "$RTC_ROOT/generated/double_puppet_secret" \
    --apply

# Install both MatrixRTC-related appservice registrations into Synapse.
python3 "$RTC_ROOT/merge-synapse.py" --apply

# Confirm the persisted configuration, not just the pre-write plan. A
# partial apply must not trigger a restart with missing credentials or state.
python3 -B "$RTC_ROOT/validate-candidate.py" --server-name "$SERVER" --applied
verify_existing_base_services
verify_jwt_image_contract
verify_call_mount_contract

# Validate the merged Compose project before restarting anything.
docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    config >/dev/null

# Beyond this point containers may consume the new configuration. Never
# automatically restore old files underneath a possibly restarted service.
python3 -B "$RTC_ROOT/validate-candidate.py" --server-name "$SERVER" --mark-restart "$LIVE_JOURNAL"
LIVE_WRITES_STARTED=0

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

verify_jwt_image_contract
MATRIXRTC_ENABLE_CALLS=0 docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    up -d --no-deps --pull never --force-recreate lk-jwt-service
verify_running_jwt_image

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
verify_jwt_image_contract
verify_running_jwt_image
restore_worker

for ((attempt=0; attempt<60; attempt++)); do
    if bash "$RTC_ROOT/verify-worker.sh" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
bash "$RTC_ROOT/verify-worker.sh"
verify_runtime_wiring "$RESTORE_CALLS"
verify_running_jwt_image

compose ps mautrix-whatsapp mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner livekit lk-jwt-service synapse

INSTALL_COMPLETE=1
trap - EXIT INT TERM
# Only a fully verified upgrade may discard its configuration journal.
rm -rf -- "$LIVE_JOURNAL" || { echo "Upgrade verified, but private journal cleanup failed: $LIVE_JOURNAL" >&2; exit 1; }
LIVE_JOURNAL=""

echo "mautrix-whatsapp-call install/upgrade migration: PASS"
echo "Existing WhatsApp DMs, RTC permissions, m.direct mappings and ghost identities were migrated automatically."
echo "Future WhatsApp DMs receive RTC permissions at creation; mautrix keeps m.direct synchronized via appservice double puppeting; the dedicated provisioner creates persistent cross-signed RTC identities out-of-band."
echo "The main call worker does not receive the WhatsApp appservice registration."
echo "Existing MATRIXRTC_ENABLE_CALLS setting restored: $RESTORE_CALLS"
echo "Force-stop/reopen Element clients once after an upgrade so cached call eligibility is refreshed."
