#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/build/versions.env"
CACHE="${1:?cache}"
WORK="${2:?isolated work directory}"
JOBS="${3:?jobs}"
mkdir -p "$CACHE/downloads" "$WORK"
ARCHIVE="$CACHE/downloads/opus-${OPUS_VERSION}.tar.gz"
valid_archive() { [[ -f "$1" ]] && [[ "$(sha256sum "$1" | awk '{print $1}')" == "$OPUS_SHA256" ]]; }
if ! valid_archive "$ARCHIVE"; then
    TEMP="$(mktemp "$CACHE/downloads/.opus.XXXXXXXX")"
    trap 'rm -f -- "$TEMP"' EXIT
    curl --fail --location --retry 8 --retry-all-errors --retry-delay 3 --output "$TEMP" "https://downloads.xiph.org/releases/opus/opus-${OPUS_VERSION}.tar.gz"
    valid_archive "$TEMP" || { echo "Opus source checksum mismatch" >&2; exit 26; }
    mv -f "$TEMP" "$ARCHIVE"
    trap - EXIT
fi
valid_archive "$ARCHIVE" || exit 26
for target in native musl; do
    TREE="$WORK/$target"
    mkdir "$TREE"
    tar -xzf "$ARCHIVE" --strip-components=1 -C "$TREE"
    (
        cd "$TREE"
        case "$target" in
            native) CC=cc ./configure --disable-shared --enable-static --disable-extra-programs --disable-doc ;;
            musl) CC=musl-gcc ./configure --host=x86_64-linux-musl --disable-shared --enable-static --disable-extra-programs --disable-doc ;;
        esac
        make -j "$JOBS" libopus.la
    )
    [[ -s "$TREE/.libs/libopus.a" ]] || { echo "Missing $target static Opus archive" >&2; exit 26; }
    [[ ! -e "$TREE/.libs/libopus.so" ]] || { echo "Unexpected shared Opus library" >&2; exit 26; }
done
