#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$REPO_ROOT/build/versions.env"

CACHE="${1:-${MAUTRIX_WHATSAPP_CALL_CACHE:-$HOME/.cache/mautrix-whatsapp-call}}"
DOWNLOADS="$CACHE/downloads"
TOOLS="$CACHE/tools"
mkdir -p "$DOWNLOADS" "$TOOLS"

check_sha() {
    local file="$1" expected="$2" actual
    actual="$(sha256sum "$file" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
        echo "SHA-256 mismatch for $file" >&2
        echo "Expected: $expected" >&2
        echo "Actual:   $actual" >&2
        return 1
    fi
}

download() {
    local url="$1" output="$2" expected="${3:-}"
    mkdir -p "$(dirname "$output")"
    if [[ -f "$output" ]]; then
        if [[ -z "$expected" ]] || check_sha "$output" "$expected" >/dev/null 2>&1; then
            echo "Already present: $(basename "$output")"
            return 0
        fi
        rm -f "$output"
    fi
    echo "Downloading: $url"
    curl --fail --location --retry 8 --retry-all-errors --retry-delay 3 --continue-at - --output "$output" "$url"
    if [[ -n "$expected" ]]; then
        check_sha "$output" "$expected"
    fi
}

install_dependencies() {
    if [[ "${MAUTRIX_WHATSAPP_CALL_SKIP_APT:-0}" == "1" ]]; then
        return 0
    fi

    local packages=(
        build-essential binutils ca-certificates cmake curl ffmpeg file git jq
        libglib2.0-dev libsqlite3-dev libssl-dev libwebp-dev musl-tools ninja-build
        patch patchelf pkg-config python3 python3-yaml tar unzip xz-utils zip zstd
    )
    local missing=() package
    for package in "${packages[@]}"; do
        if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -qx 'install ok installed'; then
            missing+=("$package")
        fi
    done
    if [[ "${#missing[@]}" -eq 0 ]]; then
        echo "Build dependencies already installed; skipping apt."
        return 0
    fi

    local sudo_cmd=()
    if [[ "$(id -u)" -ne 0 ]]; then
        command -v sudo >/dev/null || { echo "sudo is required to install build dependencies" >&2; exit 10; }
        sudo_cmd=(sudo)
    fi
    echo "Installing missing build dependencies: ${missing[*]}"
    "${sudo_cmd[@]}" apt-get update --allow-releaseinfo-change
    DEBIAN_FRONTEND=noninteractive "${sudo_cmd[@]}" apt-get install -y "${missing[@]}"
}

input_matches() {
    local kind="$1" root="$2" expected="$3"
    [[ -d "$root" ]] && [[ "$(python3 "$REPO_ROOT/build/input-digest.py" "$kind" "$root")" == "$expected" ]]
}

install_dependencies

GO_ROOT="$TOOLS/go"
if ! { [[ -x "$GO_ROOT/bin/go" ]] && input_matches go "$GO_ROOT" "$GO_INPUT_TREE_SHA256"; }; then
    download "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" "$DOWNLOADS/go${GO_VERSION}.linux-amd64.tar.gz" "$GO_SHA256"
    rm -rf "$TOOLS/go"
    tar -xzf "$DOWNLOADS/go${GO_VERSION}.linux-amd64.tar.gz" -C "$TOOLS"
fi

RUST_ROOT="$TOOLS/rust"
RUST_TOOLCHAIN="$RUST_ROOT/rustup/toolchains/$RUST_VERSION-x86_64-unknown-linux-gnu"
if ! { [[ -x "$RUST_TOOLCHAIN/bin/rustc" ]] && input_matches rust "$RUST_TOOLCHAIN" "$RUST_INPUT_TREE_SHA256"; }; then
    stage="$(mktemp -d "$TOOLS/.rust.XXXXXX")"
    trap 'rm -rf -- "$stage"' EXIT
    for component in rustc cargo rust-std; do
        case "$component" in
            rustc) url="$RUSTC_COMPONENT_URL"; sha="$RUSTC_COMPONENT_SHA256" ;;
            cargo) url="$CARGO_COMPONENT_URL"; sha="$CARGO_COMPONENT_SHA256" ;;
            rust-std) url="$RUST_STD_COMPONENT_URL"; sha="$RUST_STD_COMPONENT_SHA256" ;;
        esac
        archive="$DOWNLOADS/${url##*/}"
        download "$url" "$archive" "$sha"
        tar -xJf "$archive" -C "$stage"
        "$stage/$(basename "$archive" .tar.xz)/install.sh" --prefix="$stage/prefix" --disable-ldconfig
    done
    input_matches rust "$stage/prefix" "$RUST_INPUT_TREE_SHA256" || { echo "Rust component integrity mismatch" >&2; exit 13; }
    mkdir -p "$(dirname "$RUST_TOOLCHAIN")"
    rm -rf "$RUST_TOOLCHAIN"
    mv "$stage/prefix" "$RUST_TOOLCHAIN"
    rm -rf "$stage"
    trap - EXIT
fi

find_webrtc_lib() {
    local lib
    lib="$(find "$1" -type f -path '*/lib/libwebrtc.a' -print -quit 2>/dev/null || true)"
    if [[ -n "$lib" ]] && input_matches webrtc "$(dirname "$(dirname "$lib")")" "$WEBRTC_INPUT_TREE_SHA256"; then
        printf '%s\n' "$lib"
    fi
}

WEBRTC_ROOT="$TOOLS/webrtc-${WEBRTC_RELEASE_TAG}"
WEBRTC_LIB="$(find_webrtc_lib "$WEBRTC_ROOT")"
if [[ -z "$WEBRTC_LIB" ]]; then
    download "https://github.com/livekit/rust-sdks/releases/download/${WEBRTC_RELEASE_TAG}/webrtc-linux-x64-release.zip" "$DOWNLOADS/webrtc-linux-x64-release.zip" "$WEBRTC_SHA256"
    rm -rf "$WEBRTC_ROOT"
    mkdir -p "$WEBRTC_ROOT"
    unzip -q "$DOWNLOADS/webrtc-linux-x64-release.zip" -d "$WEBRTC_ROOT"
    WEBRTC_LIB="$(find_webrtc_lib "$WEBRTC_ROOT")"
fi
[[ -n "$WEBRTC_LIB" ]] || { echo "libwebrtc.a not found" >&2; exit 12; }

LLVM_ROOT="$TOOLS/llvm-${LLVM_VERSION}"
valid_llvm_root() {
    local root="$1" clang
    clang="$(find "$root" -path '*/bin/clang' -print -quit 2>/dev/null || true)"
    [[ -n "$clang" ]] && input_matches llvm "$(dirname "$(dirname "$clang")")" "$LLVM_INPUT_TREE_SHA256"
}
if ! valid_llvm_root "$LLVM_ROOT"; then
    LLVM_ARCHIVE="$DOWNLOADS/LLVM-${LLVM_VERSION}-Linux-X64.tar.xz"
    download "https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/LLVM-${LLVM_VERSION}-Linux-X64.tar.xz" "$LLVM_ARCHIVE" "$LLVM_SHA256"
    [[ "$(stat -c '%s' "$LLVM_ARCHIVE")" == "$LLVM_SIZE" ]] || { echo "LLVM archive size mismatch" >&2; exit 11; }
    rm -rf "$LLVM_ROOT"
    mkdir -p "$LLVM_ROOT"
    tar -xJf "$LLVM_ARCHIVE" -C "$LLVM_ROOT"
fi
LLVM_CLANG="$(find "$LLVM_ROOT" -path '*/bin/clang' -print -quit 2>/dev/null || true)"
[[ -n "$LLVM_CLANG" ]] || { echo "Clang not found after LLVM preparation" >&2; exit 13; }

[[ -x "$GO_ROOT/bin/go" ]] && input_matches go "$GO_ROOT" "$GO_INPUT_TREE_SHA256" || { echo "Prepared Go integrity mismatch" >&2; exit 13; }
[[ -x "$RUST_TOOLCHAIN/bin/rustc" ]] && input_matches rust "$RUST_TOOLCHAIN" "$RUST_INPUT_TREE_SHA256" || { echo "Prepared Rust integrity mismatch" >&2; exit 13; }
valid_llvm_root "$LLVM_ROOT" || { echo "Prepared LLVM integrity mismatch" >&2; exit 13; }

INPUT_PATHS="$CACHE/inputs.env"
(umask 077; printf 'export GOROOT=%q\nexport RUST_ROOT=%q\nexport LLVM_HOME=%q\nexport LK_CUSTOM_WEBRTC=%q\n' \
    "$GO_ROOT" "$RUST_ROOT" "$(dirname "$(dirname "$LLVM_CLANG")")" "$(dirname "$(dirname "$WEBRTC_LIB")")" \
    > "$INPUT_PATHS.tmp.$$"; mv "$INPUT_PATHS.tmp.$$" "$INPUT_PATHS")

echo "Prepared mautrix-whatsapp-call build inputs in: $CACHE"
echo "Go:     $($GO_ROOT/bin/go version)"
echo "Rust:   $($RUST_TOOLCHAIN/bin/rustc --version)"
echo "Clang:  $($LLVM_CLANG --version | head -1)"
echo "WebRTC: $WEBRTC_ROOT"
