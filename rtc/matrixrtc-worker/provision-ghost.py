#!/usr/bin/env python3
"""One-time Matrix AS ghost-device provisioning. Never runs during a call.

Uses an appservice token only to request a scoped device token. Requires the
existing mautrix-whatsapp application-service namespace and a registered ghost.
Tokens never appear in command-line arguments or console output.
"""
from session_files import identity_lock, ensure_session, prepare_ready
import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
import urllib.request
import urllib.error

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

p = argparse.ArgumentParser()
p.add_argument("--homeserver", required=True)
p.add_argument("--ghost-mxid", required=True)
p.add_argument("--as-token-file", type=Path)
p.add_argument("--compose-overlay", type=Path)
p.add_argument("--state-dir", required=True, type=Path)
p.add_argument("--device-name", default="WhatsApp MatrixRTC worker (staging)")
args = p.parse_args()
if not args.ghost_mxid.startswith("@") or ":" not in args.ghost_mxid:
    p.error("ghost-mxid must be a full Matrix user ID")
if not args.homeserver.startswith("https://"):
    p.error("homeserver must be HTTPS")
state = args.state_dir / "identities" / hashlib.sha256(args.ghost_mxid.encode()).hexdigest()
def login():
    if not args.as_token_file:
        p.error("as-token-file is required to create a new device")
    token = args.as_token_file.read_text().strip()
    if not token:
        p.error("as-token-file is empty")
    request = urllib.request.Request(
        args.homeserver.rstrip("/") + "/_matrix/client/v3/login",
        data=json.dumps({
            "type": "m.login.application_service",
            "identifier": {"type": "m.id.user", "user": args.ghost_mxid},
            "initial_device_display_name": args.device_name,
        }).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=15) as response:
            session = json.load(response)
    except urllib.error.HTTPError as exc:
        p.error("Matrix appservice login failed with HTTP " + str(exc.code))
    return session

os.umask(0o077)
with identity_lock(state):
    session = ensure_session(state, args.ghost_mxid, login)
    if args.compose_overlay:
        with prepare_ready(state, args.ghost_mxid):
            overlay = args.compose_overlay.resolve()
            subprocess.run([
                "docker", "compose", "-f", str(overlay.parents[1] / "docker-compose.yml"),
                "-f", str(overlay), "run", "--rm", "--no-deps",
                "-e", "RTC_GHOST_MXID=" + args.ghost_mxid, "-e", "RTC_ALLOW_INSECURE_HOMESERVER=1",
                "matrixrtc-worker", "/usr/local/bin/matrixrtc-ghost-provision"
            ], check=True)
print("Stored MatrixRTC device", session["device_id"], "under", state)
