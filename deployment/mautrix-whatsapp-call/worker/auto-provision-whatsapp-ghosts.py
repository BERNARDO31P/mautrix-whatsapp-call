#!/usr/bin/env python3
"""Automatically provision persistent cross-signed MatrixRTC devices for WhatsApp DM ghosts.

The WhatsApp appservice registration is mounted read-only. Its token is used only
for Matrix application-service authentication and is never logged or passed in a
process command line. Existing sessions are never replaced.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import fcntl
from session_files import identity_lock, ensure_session, is_ready, mark_ready
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib import error, parse, request

import yaml

REGISTRATION = Path(os.environ.get("RTC_AUTOPROVISION_REGISTRATION", "/run/secrets/mautrix-whatsapp-registration.yaml"))
CONFIG = Path(os.environ.get("RTC_AUTOPROVISION_CONFIG", "/run/secrets/mautrix-whatsapp-config.yaml"))
HOMESERVER = os.environ.get("MATRIX_HOMESERVER", "http://synapse:8008").rstrip("/")
SERVER = os.environ.get("MATRIXRTC_SERVER_NAME", "").strip().lower()
STATE_ROOT = Path(os.environ.get("RTC_WORKER_STATE_DIR", "/var/lib/mautrix-whatsapp-call"))
INTERVAL = max(5, int(os.environ.get("RTC_AUTOPROVISION_INTERVAL_SECONDS", "15")))
CACHE_TTL = max(300, int(os.environ.get("RTC_AUTOPROVISION_CACHE_TTL_SECONDS", "21600")))
NEGATIVE_CACHE_TTL = max(5, int(os.environ.get("RTC_AUTOPROVISION_NEGATIVE_CACHE_TTL_SECONDS", "30")))
JOBS = max(1, min(16, int(os.environ.get("RTC_AUTOPROVISION_JOBS", "4"))))
PROVISION_BIN = os.environ.get("RTC_GHOST_PROVISION_BIN", "/usr/local/bin/mautrix-whatsapp-call-provision")
CACHE_FILE = STATE_ROOT / ".autoprovision-room-cache.json"


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def load_yaml(path: Path, label: str) -> dict:
    if not path.is_file():
        raise RuntimeError(f"{label} not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise RuntimeError(f"failed to parse {label} {path}") from None
    if not isinstance(data, dict):
        raise RuntimeError(f"{label} root must be a mapping: {path}")
    return data


def registration_value(key: str) -> str:
    data = load_yaml(REGISTRATION, "WhatsApp appservice registration")
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"Missing {key} in {REGISTRATION}")
    return value


def config_appservice() -> dict:
    data = load_yaml(CONFIG, "mautrix-whatsapp config")
    appservice = data.get("appservice")
    if not isinstance(appservice, dict):
        raise RuntimeError(f"Missing appservice mapping in {CONFIG}")
    return appservice


def config_bot_localpart() -> str:
    appservice = config_appservice()
    bot = appservice.get("bot")
    if isinstance(bot, dict):
        username = bot.get("username")
        if isinstance(username, str) and username:
            return username
    sender = load_yaml(REGISTRATION, "WhatsApp appservice registration").get("sender_localpart")
    if isinstance(sender, str) and sender:
        return sender
    raise RuntimeError(f"Could not determine mautrix-whatsapp bot username from {CONFIG} or {REGISTRATION}")


def username_template_pattern(template: str, server_name: str) -> re.Pattern[str]:
    marker = "{{.}}"
    if template.count(marker) != 1:
        raise RuntimeError(f"unsupported mautrix-whatsapp username_template: {template!r}")
    prefix, suffix = template.split(marker, 1)
    if not (prefix or suffix) or any(c in prefix + suffix for c in "@:\r\n"):
        raise RuntimeError("username_template must define a distinct ghost namespace")
    expression = "^@" + re.escape(prefix) + "[^:]+" + re.escape(suffix) + ":" + re.escape(server_name) + "$"
    return re.compile(expression)


def registration_user_regexes() -> list[re.Pattern[str]]:
    registration = load_yaml(REGISTRATION, "WhatsApp appservice registration")
    namespaces = registration.get("namespaces")
    patterns: list[re.Pattern[str]] = []
    if isinstance(namespaces, dict):
        users = namespaces.get("users")
        if isinstance(users, list):
            for entry in users:
                if not isinstance(entry, dict):
                    continue
                value = entry.get("regex")
                if not isinstance(value, str) or "@" not in value:
                    continue
                try:
                    patterns.append(re.compile(value))
                except re.error as exc:
                    raise RuntimeError(f"invalid user namespace regex in {REGISTRATION}: {value}: {exc}") from exc
    template = config_appservice().get("username_template")
    if not isinstance(template, str) or not template:
        raise RuntimeError("appservice.username_template is required")
    scope = username_template_pattern(template, SERVER).pattern
    return [re.compile(rf"^(?=(?:{p.pattern})){scope[1:]}") for p in patterns] or [re.compile(scope)]

def is_bridge_user(user_id: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.fullmatch(user_id)  for pattern in patterns)


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def api(path: str, token: str, as_user: str | None = None, body: dict | None = None) -> tuple[int, dict]:
    url = HOMESERVER + path
    if as_user:
        separator = "&" if "?" in url else "?"
        url += separator + "user_id=" + parse.quote(as_user, safe="")
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    req = request.Request(
        url,
        data=data,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        method="POST" if body is not None else "GET",
    )
    try:
        with request.build_opener(NoRedirect()).open(req, timeout=20) as response:
            payload = response.read()
            return response.status, json.loads(payload or b"{}")
    except error.HTTPError as exc:
        payload = exc.read()
        try:
            parsed = json.loads(payload or b"{}")
        except json.JSONDecodeError:
            parsed = {}
        return exc.code, parsed


def eligible_ghost_from_state(
    members: dict,
    hints: dict,
    encryption: dict,
    bot_mxid: str,
    ghost_patterns: list[re.Pattern[str]],
    server_name: str,
) -> str | None:
    if encryption.get("algorithm") != "m.megolm.v1.aes-sha2":
        return None
    service = set(hints.get("service_members") or [])
    if bot_mxid not in service or bot_mxid not in (members.get("joined") or {}):
        return None
    humans = [user for user in (members.get("joined") or {}) if user not in service]
    room_ghosts = [user for user in humans if is_bridge_user(user, ghost_patterns)]
    real_humans = [user for user in humans if not is_bridge_user(user, ghost_patterns)]
    if len(humans) != 2 or len(room_ghosts) != 1 or len(real_humans) != 1:
        return None
    suffix = ":" + server_name
    if not room_ghosts[0].endswith(suffix) or not real_humans[0].endswith(suffix):
        return None
    return room_ghosts[0]



def is_whatsapp_dm_portal(state, bot, patterns):
    if not isinstance(state, list):
        return False
    kinds = set()
    for event in state:
        if not isinstance(event, dict) or event.get("type") not in ("m.bridge", "uk.half-shot.bridge"):
            continue
        content = event.get("content") or {}
        sender = event.get("sender", "")
        if content.get("bridgebot") != bot or (content.get("protocol") or {}).get("id") != "whatsapp":
            continue
        if sender != bot and not is_bridge_user(sender, patterns):
            continue
        jid = (content.get("channel") or {}).get("id", "")
        kind = content.get("com.beeper.room_type.v2", content.get("com.beeper.room_type"))
        if isinstance(jid, str) and kind == "dm" and re.search(r"@(s\.whatsapp\.net|lid|hosted|hosted\.lid|bot)$", jid):
            kinds.add("dm")
        else:
            kinds.add("other")
    return kinds == {"dm"}

def run_self_test() -> None:
    bot = "@whatsappbot:example.org"
    ghost = "@whatsapp_123:example.org"
    human = "@alice:example.org"
    hints = {"service_members": [bot]}
    encryption = {"algorithm": "m.megolm.v1.aes-sha2"}
    patterns = [re.compile(r"^@whatsapp_.+:example\.org$")]
    members = {"joined": {bot: {}, ghost: {}, human: {}}}
    assert eligible_ghost_from_state(members, hints, encryption, bot, patterns, "example.org") == ghost
    expanded = {"joined": {bot: {}, ghost: {}, human: {}, "@bob:example.org": {}}}
    assert eligible_ghost_from_state(expanded, hints, encryption, bot, patterns, "example.org") is None
    assert eligible_ghost_from_state(members, {"service_members": []}, encryption, bot, patterns, "example.org") is None
    assert eligible_ghost_from_state(members, hints, {}, bot, patterns, "example.org") is None
    custom = "@wa-custom-123:example.org"
    custom_patterns = [username_template_pattern("wa-custom-{{.}}", "example.org")]
    custom_members = {"joined": {bot: {}, custom: {}, human: {}}}
    assert eligible_ghost_from_state(custom_members, hints, encryption, bot, custom_patterns, "example.org") == custom
    remote_human = {"joined": {bot: {}, ghost: {}, "@alice:remote.example": {}}}
    assert eligible_ghost_from_state(remote_human, hints, encryption, bot, patterns, "example.org") is None
    portal = {"type":"m.bridge", "sender":bot, "content":{"bridgebot":bot,"protocol":{"id":"whatsapp"},"channel":{"id":"123@s.whatsapp.net"},"com.beeper.room_type":"dm"}}
    assert is_whatsapp_dm_portal([portal], bot, patterns)
    assert not is_whatsapp_dm_portal([], bot, patterns)
    portal["content"]["channel"]["id"] = "123@g.us"
    assert not is_whatsapp_dm_portal([portal], bot, patterns)
    assert CACHE_TTL >= 300
    assert NEGATIVE_CACHE_TTL >= 5
    assert username_template_pattern("whatsapp_{{.}}", "example.org").fullmatch("@whatsapp_lid-123:example.org")
    assert not username_template_pattern("whatsapp_{{.}}", "example.org").fullmatch("@telegram_123:example.org")
    assert bot == "@whatsappbot:example.org"
    print("auto-provision-whatsapp-ghosts self-test: PASS")


def load_room_cache() -> dict[str, dict]:
    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    clean: dict[str, dict] = {}
    for room_id, entry in raw.items():
        if not isinstance(room_id, str) or not isinstance(entry, dict):
            continue
        checked_at = entry.get("checked_at")
        ghost = entry.get("ghost")
        if isinstance(checked_at, (int, float)) and isinstance(ghost, str):
            clean[room_id] = {"checked_at": int(checked_at), "ghost": ghost}
    return clean


def save_room_cache(cache: dict[str, dict]) -> None:
    STATE_ROOT.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".tmp")
    old_umask = os.umask(0o077)
    try:
        tmp.write_text(json.dumps(cache, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, CACHE_FILE)
    finally:
        os.umask(old_umask)


def eligible_ghosts(token: str, bot_mxid: str, ghost_patterns: list[re.Pattern[str]]) -> list[str]:
    status, joined = api("/_matrix/client/v3/joined_rooms", token, bot_mxid)
    if status != 200:
        raise RuntimeError(f"failed to list bridge-bot rooms: HTTP {status}")

    now = int(time.time())
    joined_rooms = [room for room in joined.get("joined_rooms", []) if isinstance(room, str)]
    joined_set = set(joined_rooms)
    cache = {room: entry for room, entry in load_room_cache().items() if room in joined_set}
    # The bridge bot is also the aggregate MatrixRTC identity for WhatsApp
    # group calls. Appservice sender_localpart login is authoritative and does
    # not require the ghost namespace regex.
    ghosts: set[str] = {bot_mxid}

    for room_id in joined_rooms:
        cached = cache.get(room_id)
        if cached:
            ghost = cached.get("ghost", "")
            ttl = CACHE_TTL if ghost else NEGATIVE_CACHE_TTL
            if now - int(cached.get("checked_at", 0)) < ttl:
                if not ghost:
                    continue
                if is_bridge_user(ghost, ghost_patterns) and ghost_is_ready(ghost):
                    ghosts.add(ghost)
                    continue

        room = parse.quote(room_id, safe="")
        status, members = api(f"/_matrix/client/v3/rooms/{room}/joined_members", token, bot_mxid)
        if status != 200:
            continue
        status, hints = api(
            f"/_matrix/client/v3/rooms/{room}/state/io.element.functional_members/",
            token,
            bot_mxid,
        )
        if status != 200:
            continue
        status, encryption = api(
            f"/_matrix/client/v3/rooms/{room}/state/m.room.encryption/",
            token,
            bot_mxid,
        )
        if status != 200 or encryption.get("algorithm") != "m.megolm.v1.aes-sha2":
            cache[room_id] = {"checked_at": now, "ghost": ""}
            continue
        ghost = eligible_ghost_from_state(members, hints, encryption, bot_mxid, ghost_patterns, SERVER)
        if ghost:
            status, state = api(f"/_matrix/client/v3/rooms/{room}/state", token, bot_mxid)
            if status != 200:
                continue
            if not is_whatsapp_dm_portal(state, bot_mxid, ghost_patterns):
                ghost = None
        cache[room_id] = {"checked_at": now, "ghost": ghost or ""}
        if ghost:
            ghosts.add(ghost)

    save_room_cache(cache)
    return sorted(ghosts)


def write_session(ghost: str, token: str) -> Path:
    folder=ghost_state_folder(ghost)
    def request_login():
        status, login = api(
            "/_matrix/client/v3/login",
            token,
            body={
                "type": "m.login.application_service",
                "identifier": {"type": "m.id.user", "user": ghost},
                "initial_device_display_name": "WhatsApp MatrixRTC worker (automatic)",
            },
        )
        if status != 200:
            raise RuntimeError(f"application-service login failed for {ghost}: HTTP {status}")
        return login
    ensure_session(folder,ghost,request_login)
    return folder


def ghost_state_folder(ghost: str) -> Path:
    return STATE_ROOT / "identities" / hashlib.sha256(ghost.encode()).hexdigest()


def ghost_is_ready(ghost: str) -> bool:
    folder = ghost_state_folder(ghost)
    return is_ready(folder,ghost)


def ensure_ghost(ghost: str, token: str) -> tuple[str, bool, str]:
    folder = ghost_state_folder(ghost)
    with identity_lock(folder):
        if ghost_is_ready(ghost):
            return ghost, False, "ready"

        write_session(ghost, token)
        env = os.environ.copy()
        env["RTC_GHOST_MXID"] = ghost
        env["RTC_WORKER_STATE_DIR"] = str(STATE_ROOT)
        env["MATRIX_HOMESERVER"] = HOMESERVER
        if HOMESERVER.startswith("http://"):
            env["RTC_ALLOW_INSECURE_HOMESERVER"] = "1"
        result = subprocess.run(
            [PROVISION_BIN],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip().splitlines()
            last = detail[-1] if detail else f"exit {result.returncode}"
            raise RuntimeError(f"cross-signing preparation failed for {ghost}: {last}")
        mark_ready(folder, ghost)
        return ghost, True, "provisioned"


def scan_once() -> int:
    if not SERVER or "." not in SERVER:
        raise RuntimeError("MATRIXRTC_SERVER_NAME is required")
    token = registration_value("as_token")
    bot_localpart = config_bot_localpart()
    ghost_patterns = registration_user_regexes()
    bot_mxid = f"@{bot_localpart}:{SERVER}"
    status, whoami = api("/_matrix/client/v3/account/whoami", token, bot_mxid)
    if status != 200 or whoami.get("user_id") != bot_mxid:
        raise RuntimeError(f"WhatsApp appservice token cannot authenticate as {bot_mxid}")

    ghosts = eligible_ghosts(token, bot_mxid, ghost_patterns)
    pending = [ghost for ghost in ghosts if not ghost_is_ready(ghost)]
    if not pending:
        return 0

    failures = 0
    changed = 0
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = {pool.submit(ensure_ghost, ghost, token): ghost for ghost in pending}
        for future in as_completed(futures):
            ghost = futures[future]
            try:
                _, was_changed, _ = future.result()
                if was_changed:
                    changed += 1
                    log(f"MatrixRTC auto-provisioned ghost: {ghost}")
            except Exception as exc:
                failures += 1
                log(f"MatrixRTC auto-provision failed for {ghost}: {exc}")
    if changed or failures:
        log(f"MatrixRTC auto-provision pass: eligible={len(ghosts)} pending={len(pending)} provisioned={changed} failed={failures}")
    return failures


def locked_scan() -> int:
    STATE_ROOT.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_ROOT / ".autoprovision.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        with os.fdopen(fd, "r+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            return scan_once()
    except Exception:
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        return 0
    if args.once:
        try:
            return 1 if locked_scan() else 0
        except Exception as exc:
            log(f"MatrixRTC auto-provision pass failed: {exc}")
            return 1

    while True:
        try:
            locked_scan()
        except Exception as exc:
            log(f"MatrixRTC auto-provision scan failed: {exc}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    raise SystemExit(main())
