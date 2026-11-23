#!/usr/bin/env bash
set -Eeuo pipefail

# Conservative cleanup for this project's generated release archives and unused
# Docker images. Persistent generated/ and worker-state/ directories are never touched.
ROOT="${1:-/root}"

keep_newest_two() {
    local pattern="$1"
    mapfile -t files < <(compgen -G "$pattern" | xargs -r ls -1t 2>/dev/null || true)
    if (( ${#files[@]} > 2 )); then
        printf '%s\0' "${files[@]:2}" | xargs -0 -r rm -f --
    fi
}

keep_newest_two "$ROOT/mautrix-whatsapp-call-*.zip"
keep_newest_two "$ROOT/mautrix-whatsapp-call-*.zip.sha256"

docker image prune -f >/dev/null 2>&1 || true

echo "mautrix-whatsapp-call cleanup complete; persistent runtime state was not modified."
