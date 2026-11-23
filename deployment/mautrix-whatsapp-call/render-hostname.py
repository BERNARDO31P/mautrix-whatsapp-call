#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

parser = argparse.ArgumentParser(description="Render MatrixRTC deployment templates for one homeserver")
parser.add_argument("--server-name", required=True)
parser.add_argument("--root", default=str(Path(__file__).resolve().parent))
args = parser.parse_args()
server = args.server_name.strip().lower()
if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", server):
    raise SystemExit("invalid Matrix server name")
if ".." in server or "." not in server:
    raise SystemExit("server name must be a fully-qualified DNS name")
root = Path(args.root).resolve()
changed = []
for path in root.rglob("*"):
    if not path.is_file() or path.name == "render-hostname.py":
        continue
    try:
        data = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        continue
    if "__MATRIXRTC_SERVER_NAME__" not in data:
        continue
    path.write_text(data.replace("__MATRIXRTC_SERVER_NAME__", server), encoding="utf-8", newline="\n")
    changed.append(str(path.relative_to(root)))
required = {"synapse-additions.yaml", "verify.sh", "provision-ghost.sh"}
missing = sorted(required.difference(changed))
if missing:
    raise SystemExit("required hostname templates were not rendered: " + ", ".join(missing))
print("Rendered MatrixRTC deployment for:", server)
for rel in sorted(changed):
    print("  ", rel)
