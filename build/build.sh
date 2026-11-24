#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/versions.env"

CACHE="${MAUTRIX_WHATSAPP_CALL_CACHE:-$HOME/.cache/mautrix-whatsapp-call}"
WORK="${MAUTRIX_WHATSAPP_CALL_WORK:-$CACHE/work}"
DIST="${MAUTRIX_WHATSAPP_CALL_DIST:-$REPO_ROOT/dist}"
JOBS="${MAUTRIX_WHATSAPP_CALL_BUILD_JOBS:-$(nproc 2>/dev/null || echo 2)}"
SKIP_TESTS="${MAUTRIX_WHATSAPP_CALL_SKIP_TESTS:-0}"
BRIDGE_SRC="$REPO_ROOT/bridge"
RTC_SRC="$REPO_ROOT/rtc"
MEOW_SRC="$BRIDGE_SRC/third_party/meowcaller"
BIN="$WORK/bin"
LOG="$WORK/logs"

mkdir -p "$CACHE" "$DIST"
rm -rf "$BIN" "$LOG"
mkdir -p "$BIN" "$LOG"

bash "$REPO_ROOT/bootstrap/prepare-build-inputs.sh" "$CACHE"
source "$CACHE/inputs.env"
bash "$SCRIPT_DIR/verify-source.sh"

# Keep Go dependency/test/build operations outside the checkout, preserving
# the public repository's immutable-source build fix.
GO_SOURCE="$WORK/go-source"
rm -rf "$GO_SOURCE"
mkdir -p "$GO_SOURCE"
cp -a "$REPO_ROOT/bridge" "$GO_SOURCE/bridge"
BRIDGE_SRC="$GO_SOURCE/bridge"
MEOW_SRC="$BRIDGE_SRC/third_party/meowcaller"

export PATH="$GOROOT/bin:$PATH"
export GOMODCACHE="$CACHE/go-mod"
export GOCACHE="$CACHE/go-build"
export GOPROXY="https://proxy.golang.org,direct"
export GOSUMDB="sum.golang.org"
export GOTOOLCHAIN="local"
mkdir -p "$GOMODCACHE" "$GOCACHE"
(
    cd "$BRIDGE_SRC"
    go mod download all
)
(
    cd "$MEOW_SRC"
    go mod download all
)

export CARGO_HOME="$CACHE/cargo"
export RUSTUP_HOME="$RUST_ROOT/rustup"
export PATH="$RUST_ROOT/rustup/toolchains/$RUST_VERSION-x86_64-unknown-linux-gnu/bin:$PATH"
export CARGO_TARGET_DIR="$CACHE/rust-target"
mkdir -p "$CARGO_HOME" "$CARGO_TARGET_DIR"
(
    cd "$RTC_SRC"
    cargo fetch --locked
)

export CC="$LLVM_HOME/bin/clang"
export CXX="$LLVM_HOME/bin/clang++"
export AR="$LLVM_HOME/bin/llvm-ar"
export RANLIB="$LLVM_HOME/bin/llvm-ranlib"
export LLVM_CONFIG="$LLVM_HOME/bin/llvm-config"
export LIBCLANG_PATH="$LLVM_HOME/lib"
export PATH="$LLVM_HOME/bin:$PATH"

GLIB_INCLUDE="$(find "$LK_CUSTOM_WEBRTC" -type d -path '*/usr/include/glib-2.0' -print -quit)"
GLIB_CONFIG_INCLUDE="$(find "$LK_CUSTOM_WEBRTC" -type d -path '*/usr/lib/x86_64-linux-gnu/glib-2.0/include' -print -quit)"
[[ -n "$GLIB_INCLUDE" && -n "$GLIB_CONFIG_INCLUDE" ]] || { echo "Bundled WebRTC GLib headers not found" >&2; exit 24; }
PKG_CONFIG_LOCAL="$CACHE/pkgconfig-webrtc"
mkdir -p "$PKG_CONFIG_LOCAL"
cat > "$PKG_CONFIG_LOCAL/glib-2.0.pc" <<PC
prefix=/
Name: GLib
Description: WebRTC bundled GLib headers
Version: 2.80.0
Cflags: -I$GLIB_INCLUDE -I$GLIB_CONFIG_INCLUDE
Libs:
PC
cat > "$PKG_CONFIG_LOCAL/gobject-2.0.pc" <<PC
prefix=/
Name: GObject
Description: WebRTC bundled GObject headers
Version: 2.80.0
Requires: glib-2.0
Cflags: -I$GLIB_INCLUDE -I$GLIB_CONFIG_INCLUDE
Libs:
PC
cat > "$PKG_CONFIG_LOCAL/gio-2.0.pc" <<PC
prefix=/
Name: GIO
Description: WebRTC bundled GIO headers
Version: 2.80.0
Requires: gobject-2.0 glib-2.0
Cflags: -I$GLIB_INCLUDE -I$GLIB_CONFIG_INCLUDE
Libs:
PC
export PKG_CONFIG_PATH="$PKG_CONFIG_LOCAL:/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/share/pkgconfig"

if [[ "$SKIP_TESTS" != "1" ]]; then
    python3 -B "$REPO_ROOT/tests/test_project.py"
    DEPLOY="$REPO_ROOT/deployment/mautrix-whatsapp-call"
    python3 -B "$DEPLOY/resolve-appservice.py" --self-test
    python3 -B "$DEPLOY/worker/auto-provision-whatsapp-ghosts.py" --self-test
    bash "$DEPLOY/sync-direct-chats.sh" --self-test
    for test in "$REPO_ROOT"/tests/shell/*.sh; do bash "$test"; done
    (
        cd "$BRIDGE_SRC"
        go test -p "$JOBS" -mod=readonly -tags "goolm" -count=1 \
            -run 'Test(RTC(EventProcessorDispatchClasses|RingLifetimeAfterAcceptAndCancellation|UnansweredRingExpires)|MatrixRTCDMPowerLevels|MatrixRTCCall(DeviceClassification|StaticJIDAvailability)|WhatsApp(BusinessCallOptionClassification|BusinessCallProfileResponseClassification|VoIPFeatureResponseClassification|EndReasonFormatting)|CombineWhatsAppMatrixRTCCallAvailabilityPrecedence|VoIPManagerRefinesEndReason)$' \
            -v ./pkg/connector 2>&1 | tee "$LOG/go-focused.log"
        go test -p "$JOBS" -mod=readonly -tags "goolm" -count=1 ./pkg/connector/voipbridge/... ./pkg/connector 2>&1 | tee "$LOG/go-connector.log"
    )
    (
        cd "$MEOW_SRC"
        go test -p "$JOBS" -mod=readonly -count=1 ./rtp ./mlow ./signaling . 2>&1 | tee "$LOG/meowcaller.log"
    )
    (
        cd "$RTC_SRC"
        cargo test --locked -p matrix-rtc-core --features testing --lib -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-core.log"
        cargo test --locked -p matrix-rtc-call -p matrix-rtc-transport --lib -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-call-transport.log"
        cargo test --locked -p matrix-rtc-call-sdk --features matrix-sdk --lib -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-call-sdk.log"
        cargo test --locked -p matrix-rtc-matrix-sdk -p matrix-rtc-livekit --lib -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-native-backends.log"
        cargo test --locked -p matrixrtc-whatsapp-worker -j "$JOBS" 2>&1 | tee "$LOG/rust-core.log"
        cargo test --locked -p matrixrtc-whatsapp-worker --features rtc -j "$JOBS" 2>&1 | tee "$LOG/rust-rtc.log"
    )
fi

(
    cd "$RTC_SRC"
    cargo build --locked --release -p matrixrtc-whatsapp-worker --features rtc --bins -j "$JOBS" 2>&1 | tee "$LOG/rust-build.log"
)
(
    cd "$BRIDGE_SRC"
    env CC="musl-gcc" CXX="musl-gcc" CGO_ENABLED="1" GOOS="linux" GOARCH="amd64" \
        go build -p "$JOBS" -mod=readonly -buildvcs=false -tags "goolm,netgo,osusergo" -trimpath \
        -ldflags='-linkmode external -extldflags "-static"' \
        -o "$BIN/mautrix-whatsapp-call-bridge" ./cmd/mautrix-whatsapp 2>&1 | tee "$LOG/go-build.log"
)

cp "$CARGO_TARGET_DIR/release/matrixrtc-whatsapp-worker" "$BIN/mautrix-whatsapp-call-worker"
cp "$CARGO_TARGET_DIR/release/matrixrtc-ghost-provision" "$BIN/mautrix-whatsapp-call-provision"
strip "$BIN/mautrix-whatsapp-call-worker" "$BIN/mautrix-whatsapp-call-provision" || true

"$BIN/mautrix-whatsapp-call-worker" tls-self-test 2>&1 | tee "$LOG/rust-tls-release.log"

STAMP="$(date -u +%Y%m%d-%H%M%S)"
PACKAGE_NAME="mautrix-whatsapp-call-${STAMP}"
PACKAGE_WORK="$WORK/package"
RELEASE="$PACKAGE_WORK/$PACKAGE_NAME"
ZIP_WORK="$PACKAGE_WORK/$PACKAGE_NAME.zip"
SHA_WORK="$PACKAGE_WORK/$PACKAGE_NAME.zip.sha256"
rm -rf "$PACKAGE_WORK"
mkdir -p "$RELEASE/bin" "$RELEASE/deployment" "$DIST"
python3 -B "$SCRIPT_DIR/runtime-layout.py" stage "$REPO_ROOT" "$RELEASE"
cp "$BIN"/* "$RELEASE/bin/"
cp "$BIN/mautrix-whatsapp-call-worker" "$RELEASE/deployment/mautrix-whatsapp-call/worker/"
cp "$BIN/mautrix-whatsapp-call-provision" "$RELEASE/deployment/mautrix-whatsapp-call/worker/"
cp "$BIN/mautrix-whatsapp-call-bridge" "$RELEASE/deployment/mautrix-whatsapp-call/bridge/"
cp "$SCRIPT_DIR/versions.env" "$RELEASE/SOURCE-VERSIONS.env"

# Windows/DrvFS source trees can report permissive 0777 modes. Runtime archives
# must have deterministic Linux permissions regardless of the checkout filesystem.
find "$RELEASE" -type d -exec chmod 0755 {} +
find "$RELEASE" -type f -exec chmod 0644 {} +
find "$RELEASE/deployment" -type f \( -name "*.sh" -o -name "*.py" \) -exec chmod 0755 {} +
chmod 0755     "$RELEASE/bin/mautrix-whatsapp-call-bridge"     "$RELEASE/bin/mautrix-whatsapp-call-worker"     "$RELEASE/bin/mautrix-whatsapp-call-provision"     "$RELEASE/deployment/mautrix-whatsapp-call/bridge/mautrix-whatsapp-call-bridge"     "$RELEASE/deployment/mautrix-whatsapp-call/worker/mautrix-whatsapp-call-worker"     "$RELEASE/deployment/mautrix-whatsapp-call/worker/mautrix-whatsapp-call-provision"

(
    cd "$RELEASE"
    find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)
bash "$SCRIPT_DIR/verify-package.sh" "$RELEASE"
(
    cd "$PACKAGE_WORK"
    zip -qr "$ZIP_WORK" "$PACKAGE_NAME"
)
bash "$SCRIPT_DIR/verify-package.sh" "$ZIP_WORK"
sha256sum "$ZIP_WORK" | sed "s#  .*#  $PACKAGE_NAME.zip#" > "$SHA_WORK"
cp -f "$ZIP_WORK" "$DIST/$PACKAGE_NAME.zip"
cp -f "$SHA_WORK" "$DIST/$PACKAGE_NAME.zip.sha256"
echo "Built release: $DIST/$PACKAGE_NAME.zip"
