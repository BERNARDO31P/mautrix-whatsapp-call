#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/build/versions.env"

tree_hash() {
    python3 - "$1" <<'PY'
from pathlib import Path
import hashlib,sys
root=Path(sys.argv[1])
h=hashlib.sha256()
for path in sorted(p for p in root.rglob('*') if p.is_file() and '.git' not in p.parts):
    rel=path.relative_to(root).as_posix().encode()
    h.update(rel+b'\0'+hashlib.sha256(path.read_bytes()).digest()+b'\n')
print(h.hexdigest())
PY
}

[[ "$(tree_hash "$ROOT/bridge")" == "$BRIDGE_SOURCE_TREE_SHA256" ]] || { echo "Bridge source tree hash mismatch" >&2; exit 20; }
[[ "$(tree_hash "$ROOT/bridge/third_party/meowcaller")" == "$MEOWCALLER_SOURCE_TREE_SHA256" ]] || { echo "meowcaller source tree hash mismatch" >&2; exit 21; }
[[ "$(tree_hash "$ROOT/rtc")" == "$RTC_SOURCE_TREE_SHA256" ]] || { echo "RTC source tree hash mismatch" >&2; exit 22; }

grep -F 'replace github.com/purpshell/meowcaller => ./third_party/meowcaller' "$ROOT/bridge/go.mod" >/dev/null
grep -F 'StartVoIPGroupCall' "$ROOT/bridge/pkg/connector/voip.go" >/dev/null
grep -F 'rejectMatrixRTCBridgeBotControlCall' "$ROOT/bridge/pkg/connector/matrixrtc_events.go" >/dev/null
grep -F 'CallKind::Group' "$ROOT/rtc/matrixrtc-worker/src/rtc.rs" >/dev/null
grep -F 'select_bot_control_room' "$ROOT/deployment/mautrix-whatsapp-call/migrate-rooms.sh" >/dev/null

# The canonical project name is mautrix-whatsapp-call. Reject the old typo in
# project-owned paths or text without embedding that typo literally here.
MISSPELLED_NAME="maut""ix-whatsapp-call"
MISSPELLED_ENV="MAUT""IX_WHATSAPP_CALL"
if find "$ROOT" -path "$ROOT/.git" -prune -o -name "*${MISSPELLED_NAME%%-whatsapp-call}"'*' -print -quit | grep -q .; then
    echo "Misspelled project path remains" >&2
    exit 23
fi
if grep -RIl --binary-files=without-match --exclude-dir=.git --exclude-dir=dist --exclude-dir=target --exclude-dir=__pycache__ -e "$MISSPELLED_NAME" -e "$MISSPELLED_ENV" -- "$ROOT" | grep -q .; then
    echo "Misspelled project name remains in repository content" >&2
    exit 24
fi

# Historical patch/reconstruction artifacts must not be part of this repository.
if [[ -d "$ROOT/patches" || -f "$ROOT/FULL-SOURCE.md" ]]; then
    echo "Historical reconstruction artifacts remain" >&2
    exit 25
fi

# On a Git checkout, reject files that belong in caches, releases, runtime state
# or local developer state even if someone bypassed .gitignore.
if git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    python3 - "$ROOT" <<'PYHYGIENE'
from pathlib import Path
import subprocess, sys
root = Path(sys.argv[1])
tracked = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"]).split(b"\0")
for raw in tracked:
    if not raw:
        continue
    rel = raw.decode("utf-8", "surrogateescape")
    parts = Path(rel).parts
    name = Path(rel).name
    forbidden_parts = {"dist", "target", ".cache", "generated", "worker-state", "__pycache__", "node_modules"}
    forbidden_suffixes = (".zip", ".tar", ".tar.gz", ".tgz", ".xz", ".7z", ".o", ".obj", ".a", ".so", ".dylib", ".dll", ".exe", ".pem", ".key", ".p12", ".pfx", ".db", ".sqlite", ".sqlite3")
    if any(part in forbidden_parts for part in parts) or name == ".env" or name.startswith(".env.") and name != ".env.example" or rel.endswith(forbidden_suffixes) or (root / rel).is_symlink():
        raise SystemExit(f"Forbidden generated/runtime artifact is tracked: {rel}")
PYHYGIENE
fi

while IFS= read -r -d '' script; do bash -n "$script"; done < <(find "$ROOT/build" "$ROOT/bootstrap" "$ROOT/deployment" "$ROOT/tests" -type f -name '*.sh' -print0)
python3 - "$ROOT/deployment/mautrix-whatsapp-call" "$ROOT/tests" "$ROOT/build" <<'PYCODE'
from pathlib import Path
import sys
for base in map(Path, sys.argv[1:]):
    for path in base.rglob("*.py"):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
PYCODE
python3 - "$ROOT/deployment/mautrix-whatsapp-call/compose.yml" "$ROOT/deployment/mautrix-whatsapp-call/synapse-additions.yaml" <<'PY'
import sys,yaml
for path in sys.argv[1:]:
    with open(path,'r',encoding='utf-8') as f: yaml.safe_load(f)
PY

echo "mautrix-whatsapp-call source verification: PASS"
