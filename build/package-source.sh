#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/dist/mautrix-whatsapp-call-source.zip}"
mkdir -p "$(dirname "$OUT")"

# In a real Git checkout, package exactly the committed tree. This guarantees
# ignored/untracked caches, credentials and build output can never leak into a
# published source archive.
if git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
   && git -C "$ROOT" rev-parse --verify HEAD >/dev/null 2>&1; then
    rm -f "$OUT" "$OUT.sha256"
    git -C "$ROOT" archive \
        --format=zip \
        --prefix="mautrix-whatsapp-call/" \
        --output="$OUT" \
        HEAD
else
    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT
    DEST="$TMP/mautrix-whatsapp-call"
    mkdir -p "$DEST"
    rsync -a \
        --exclude='.git/' \
        --exclude='dist/' \
        --exclude='.cache/' \
        --exclude='target/' \
        --exclude='generated/' \
        --exclude='worker-state/' \
        --exclude='__pycache__/' \
        --exclude='.idea/' \
        --exclude='.vscode/' \
        --exclude='.vs/' \
        "$ROOT/" "$DEST/"

    find "$DEST" -type d -exec chmod 0755 {} +
    find "$DEST" -type f -exec chmod 0644 {} +
    find "$DEST" -type f -name "*.sh" -exec chmod 0755 {} +

    rm -f "$OUT" "$OUT.sha256"
    (
        cd "$TMP"
        zip -qr "$OUT" mautrix-whatsapp-call
    )
fi

sha256sum "$OUT" > "$OUT.sha256"
echo "Source package: $OUT"
