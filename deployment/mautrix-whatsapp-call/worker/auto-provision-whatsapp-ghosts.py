#!/usr/bin/env python3
"""Automatically provision persistent cross-signed MatrixRTC devices for WhatsApp DM ghosts.

The WhatsApp appservice registration is mounted read-only. Its token is used only
for Matrix application-service authentication and is never logged or passed in a
process command line. Existing sessions are never replaced.
"""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import fcntl
from session_files import identity_lock, ensure_session, is_ready, prepare_ready
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib import error, parse, request

from ghost_namespace import resolve_metadata, username_template_pattern
from portal_kind import portal_kind

REGISTRATION = Path(os.environ.get("RTC_AUTOPROVISION_REGISTRATION", "/run/secrets/mautrix-whatsapp-registration.yaml"))
CONFIG = Path(os.environ.get("RTC_AUTOPROVISION_CONFIG", "/run/secrets/mautrix-whatsapp-config.yaml"))
HOMESERVER = os.environ.get("MATRIX_HOMESERVER", "http://synapse:8008").rstrip("/")
SERVER = os.environ.get("MATRIXRTC_SERVER_NAME", "").strip()
STATE_ROOT = Path(os.environ.get("RTC_WORKER_STATE_DIR", "/var/lib/mautrix-whatsapp-call"))
INTERVAL = max(5, int(os.environ.get("RTC_AUTOPROVISION_INTERVAL_SECONDS", "15")))
CACHE_TTL = max(300, int(os.environ.get("RTC_AUTOPROVISION_CACHE_TTL_SECONDS", "21600")))
NEGATIVE_CACHE_TTL = max(5, int(os.environ.get("RTC_AUTOPROVISION_NEGATIVE_CACHE_TTL_SECONDS", "30")))
JOBS = max(1, min(16, int(os.environ.get("RTC_AUTOPROVISION_JOBS", "4"))))
PROVISION_BIN = os.environ.get("RTC_GHOST_PROVISION_BIN", "/usr/local/bin/mautrix-whatsapp-call-provision")
CACHE_FILE = STATE_ROOT / ".autoprovision-room-cache.json"
GROUP_MEDIA = os.environ.get("MATRIXRTC_EXPERIMENTAL_INDEPENDENT_GROUP_MEDIA", "0") == "1"
GROUP_CACHE_TTL = min(CACHE_TTL, 60)


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


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


class InvalidRoomResponse(ValueError):
    """A successful HTTP response did not contain the expected room state."""


def validate_room_inputs(members, hints, encryption) -> None:
    if not all(isinstance(value, dict) for value in (members, hints, encryption)):
        raise InvalidRoomResponse("room state must be an object")
    joined = members.get("joined")
    service = hints.get("service_members")
    if not isinstance(joined, dict) or not all(isinstance(user, str) and isinstance(info, dict)
                                               for user, info in joined.items()):
        raise InvalidRoomResponse("invalid joined members")
    if not isinstance(service, list) or not all(isinstance(user, str) for user in service):
        raise InvalidRoomResponse("invalid service members")
    if not isinstance(encryption.get("algorithm"), str):
        raise InvalidRoomResponse("invalid encryption algorithm")


def validate_portal_state(state) -> None:
    if not isinstance(state, list) or not all(isinstance(event, dict) for event in state):
        raise InvalidRoomResponse("invalid room state list")
    for event in state:
        if event.get("type") not in ("m.bridge", "uk.half-shot.bridge"):
            continue
        content = event.get("content")
        if not isinstance(content, dict) or not isinstance(event.get("sender"), str):
            raise InvalidRoomResponse("invalid bridge state event")
        for key in ("protocol", "channel"):
            if key in content and not isinstance(content[key], dict):
                raise InvalidRoomResponse("invalid bridge state content")


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
    return portal_kind(state, bot, patterns) == "dm"


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
    except (OSError, ValueError, RecursionError):
        return {}
    if not isinstance(raw, dict):
        return {}
    clean: dict[str, dict] = {}
    for room_id, entry in raw.items():
        if not isinstance(room_id, str) or not isinstance(entry, dict):
            continue
        checked_at = entry.get("checked_at")
        ghost = entry.get("ghost")
        if type(checked_at) is int and checked_at >= 0 and isinstance(ghost, str):
            clean[room_id] = {"checked_at": checked_at, "ghost": ghost}
            group_ghosts = entry.get("group_ghosts")
            if isinstance(group_ghosts, list) and all(isinstance(g, str) for g in group_ghosts):
                clean[room_id]["group_ghosts"] = group_ghosts
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
    if not isinstance(joined, dict) or not isinstance(joined.get("joined_rooms"), list):
        raise InvalidRoomResponse("invalid joined rooms response")
    if not all(isinstance(room, str) for room in joined["joined_rooms"]):
        raise InvalidRoomResponse("invalid joined room ID")
    joined_rooms = list(dict.fromkeys(joined["joined_rooms"]))
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
            if GROUP_MEDIA and "group_ghosts" in cached:
                group_ghosts = cached["group_ghosts"]
                if 0 <= now - cached["checked_at"] < GROUP_CACHE_TTL and all(
                    is_bridge_user(g, ghost_patterns) and g.endswith(":" + SERVER) for g in group_ghosts
                ):
                    ghosts.update(group_ghosts)
                    continue
            elif 0 <= now - cached["checked_at"] < ttl:
                if not ghost and GROUP_MEDIA:
                    pass  # Legacy negative entries may represent groups.
                elif not ghost:
                    continue
                if ghost and is_bridge_user(ghost, ghost_patterns) and ghost_is_ready(ghost):
                    ghosts.add(ghost)
                    continue

        try:
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
            if status != 200:
                continue
            validate_room_inputs(members, hints, encryption)
            if encryption.get("algorithm") != "m.megolm.v1.aes-sha2":
                cache[room_id] = {"checked_at": now, "ghost": ""}
                continue
            ghost = eligible_ghost_from_state(members, hints, encryption, bot_mxid, ghost_patterns, SERVER)
            if ghost or GROUP_MEDIA:
                status, state = api(f"/_matrix/client/v3/rooms/{room}/state", token, bot_mxid)
                if status != 200:
                    continue
                validate_portal_state(state)
                kind = portal_kind(state, bot_mxid, ghost_patterns)
                if kind != "dm":
                    ghost = None
                if GROUP_MEDIA and kind == "group":
                    joined = set(members["joined"])
                    services = set(hints["service_members"])
                    local = ":" + SERVER
                    group_ghosts = []
                    if bot_mxid in joined and bot_mxid in services and any(
                        u.endswith(local) and u not in services and not is_bridge_user(u, ghost_patterns)
                        for u in joined
                    ):
                        group_ghosts = sorted(u for u in joined - services - {bot_mxid}
                            if u.endswith(local) and is_bridge_user(u, ghost_patterns))
                    cache[room_id] = {"checked_at": now, "ghost": "", "group_ghosts": group_ghosts}
                    ghosts.update(group_ghosts)
                    continue
            cache[room_id] = {"checked_at": now, "ghost": ghost or ""}
            if ghost:
                ghosts.add(ghost)
        except (OSError, json.JSONDecodeError, UnicodeError, InvalidRoomResponse) as exc:
            # A transient read failure is not evidence that a room is ineligible.
            log(f"MatrixRTC auto-provision room read failed for {room_id}: {type(exc).__name__}")
            continue

    try:
        save_room_cache(cache)
    except OSError as exc:
        # The cache is an optimisation, not a prerequisite for provisioning.
        log(f"MatrixRTC auto-provision cache write failed: {type(exc).__name__}")
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
    try:
        return is_ready(folder, ghost)
    except OSError:
        # Readiness discovery is advisory. A local failure must not abort the
        # entire room scan. The per-identity provisioning job will recheck
        # under its lock and report any persistent error independently.
        return False


def ensure_ghost(ghost: str, token: str) -> tuple[str, bool, str]:
    folder = ghost_state_folder(ghost)
    with identity_lock(folder):
        if ghost_is_ready(ghost):
            return ghost, False, "ready"

        write_session(ghost, token)
        with prepare_ready(folder, ghost):
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
        return ghost, True, "provisioned"


def scan_once() -> int:
    if not SERVER or "." not in SERVER:
        raise RuntimeError("MATRIXRTC_SERVER_NAME is required")
    metadata = resolve_metadata(REGISTRATION, CONFIG, SERVER)
    token = metadata["as_token"]
    bot_localpart = metadata["bot_localpart"]
    ghost_patterns = [re.compile(pattern) for pattern in metadata["ghost_regexes"]]
    bot_mxid = f"@{bot_localpart}:{SERVER}"
    status, whoami = api("/_matrix/client/v3/account/whoami", token, bot_mxid)
    if status != 200 or whoami.get("user_id") != bot_mxid:
        raise RuntimeError(f"WhatsApp appservice token cannot authenticate as {bot_mxid}")

    failures = 0
    changed = 0
    # Group-call readiness must not depend on enumerating the DM rooms.
    # Prepare the authenticated bot once before discovery, even if discovery fails.
    if not ghost_is_ready(bot_mxid):
        try:
            _, was_changed, _ = ensure_ghost(bot_mxid, token)
            if was_changed:
                changed += 1
                log(f"MatrixRTC auto-provisioned ghost: {bot_mxid}")
        except Exception as exc:
            failures += 1
            log(f"MatrixRTC auto-provision failed for {bot_mxid}: {exc}")

    try:
        ghosts = eligible_ghosts(token, bot_mxid, ghost_patterns)
    except (RuntimeError, OSError, ValueError) as exc:
        log(f"MatrixRTC auto-provision discovery failed: {type(exc).__name__}")
        return failures + 1
    pending = [ghost for ghost in ghosts if ghost != bot_mxid and not ghost_is_ready(ghost)]
    if not pending:
        return failures

    # Keep executor backlog bounded even on installations with many contacts.
    remaining = iter(pending)
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = {pool.submit(ensure_ghost, ghost, token): ghost
                   for ghost in (next(remaining, None) for _ in range(JOBS))
                   if ghost is not None}
        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                ghost = futures.pop(future)
                try:
                    _, was_changed, _ = future.result()
                    if was_changed:
                        changed += 1
                        log(f"MatrixRTC auto-provisioned ghost: {ghost}")
                except Exception as exc:
                    failures += 1
                    log(f"MatrixRTC auto-provision failed for {ghost}: {exc}")
                next_ghost = next(remaining, None)
                if next_ghost is not None:
                    futures[pool.submit(ensure_ghost, next_ghost, token)] = next_ghost
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
