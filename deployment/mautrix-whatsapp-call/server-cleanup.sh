#!/usr/bin/env bash
set -Eeuo pipefail
# Only remove old, complete project release pairs. No global Docker pruning.
python3 - "${1:-/root}" <<'PY'
from pathlib import Path
import re, sys
root = Path(sys.argv[1])
pairs = []
for path in root.iterdir():
    if not re.fullmatch(r"mautrix-whatsapp-call-[0-9]{8}-[0-9]{6}\.zip", path.name):
        continue
    sidecar = path.with_name(path.name + ".sha256")
    if path.is_symlink() or sidecar.is_symlink() or not path.is_file() or not sidecar.is_file():
        continue
    pairs.append((path, sidecar))
pairs.sort(key=lambda pair: pair[0].name, reverse=True)
for path, sidecar in pairs[2:]:
    path.unlink()
    sidecar.unlink()
    print("Removed old release pair:", path.name)
PY
echo "mautrix-whatsapp-call cleanup complete; persistent runtime state and Docker images were not modified."
