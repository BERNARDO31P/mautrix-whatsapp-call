#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/dist/mautrix-whatsapp-call-source.zip}"
mkdir -p "$(dirname "$OUT")"

# Publish only the verified committed tree. A denylist copy of a local
# directory can inadvertently include credentials or runtime state.
git -C "$ROOT" rev-parse --verify HEAD >/dev/null
[[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=normal)" ]] || {
    echo "Commit or remove local changes before packaging source" >&2; exit 2;
}
"$ROOT/build/verify-source.sh"
rm -f "$OUT" "$OUT.sha256"
git -C "$ROOT" archive --format=zip --prefix="mautrix-whatsapp-call/" --output="$OUT" HEAD

sha256sum "$OUT" > "$OUT.sha256"
echo "Source package: $OUT"
