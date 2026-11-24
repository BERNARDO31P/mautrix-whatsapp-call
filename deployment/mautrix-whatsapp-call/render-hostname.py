#!/usr/bin/env python3
"""Render only shipped hostname templates, never runtime state or credentials."""
import argparse
from pathlib import Path
import re

from config_files import atomic_write

PLACEHOLDER = "__MATRIXRTC_SERVER_NAME__"
TEMPLATES = ("synapse-additions.yaml", "verify.sh", "provision-ghost.sh", "nginx-443-locations.conf")


def render(root, server):
    server = server.strip().lower()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", server) or ".." in server or "." not in server:
        raise ValueError("invalid fully-qualified Matrix server name")
    updates = []
    # Validate every file before changing any file. A repeated invocation is
    # idempotent; a previously rendered different hostname is an explicit error.
    for rel in TEMPLATES:
        path = root / rel
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"missing regular hostname template: {rel}")
        text = path.read_text(encoding="utf-8")
        if PLACEHOLDER in text:
            updated = text.replace(PLACEHOLDER, server)
            updates.append((path, updated))
        elif rel == "nginx-443-locations.conf":
            if not re.search(r"\bserver_name\s+" + re.escape(server) + r"(?:\s|;|`|$)", text):
                raise ValueError(f"hostname mismatch in {rel}; restore the shipped template")
        elif not re.search(r"https://" + re.escape(server) + r"(?:/|[\s\"'}]|$)", text):
            raise ValueError(f"hostname mismatch in {rel}; restore the shipped template")
    for path, updated in updates:
        st = path.stat()
        atomic_write(path, updated.encode(), st, st.st_mode & 0o777)
    return [path.name for path, _ in updates]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-name", required=True)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    changes = render(args.root.resolve(), args.server_name)
    print("Rendered MatrixRTC templates for:", args.server_name, "(" + ", ".join(changes) + ")")
