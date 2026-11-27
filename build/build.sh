#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/versions.env"

CACHE="${MAUTRIX_WHATSAPP_CALL_CACHE:-$HOME/.cache/mautrix-whatsapp-call}"
WORK_ROOT="${MAUTRIX_WHATSAPP_CALL_WORK:-$CACHE/work}"
DIST="${MAUTRIX_WHATSAPP_CALL_DIST:-$ORIGINAL_ROOT/dist}"
JOBS="${MAUTRIX_WHATSAPP_CALL_BUILD_JOBS:-$(nproc 2>/dev/null || echo 2)}"
SKIP_TESTS="${MAUTRIX_WHATSAPP_CALL_SKIP_TESTS:-0}"
[[ "$SKIP_TESTS" == "0" ]] || { echo "Release builds require all tests" >&2; exit 2; }
[[ "$JOBS" =~ ^[1-9][0-9]{0,2}$ ]] && (( JOBS <= 256 )) || { echo "Invalid build job count" >&2; exit 2; }
mkdir -p "$CACHE" "$WORK_ROOT/runs" "$DIST"
CACHE="$(cd "$CACHE" && pwd)"
DIST="$(cd "$DIST" && pwd)"
# Do not race a second builder sharing toolchain caches or cargo outputs.
exec 9>"$CACHE/build.lock"
flock -n 9 || { echo "Another release build is using this cache" >&2; exit 2; }
RUN="$(mktemp -d "$WORK_ROOT/runs/build.XXXXXXXX")"
RUN="$(cd "$RUN" && pwd)"
chmod 0700 "$RUN"
LOG="$RUN/logs"
mkdir -p "$LOG"
echo "Build evidence: $RUN"

bash "$SCRIPT_DIR/verify-source.sh"
# Stable path keeps compiler fingerprints and CGO library paths consistent.
# The cache lock above protects this private scratch directory, even when
# the evidence location (WORK_ROOT) is overridden.
WORK="$CACHE/build-scratch-v1"
MARKER="$WORK/.mautrix-whatsapp-call-scratch"
if [[ -e "$WORK" || -L "$WORK" ]]; then
    [[ -d "$WORK" && ! -L "$WORK" && -f "$MARKER" && ! -L "$MARKER" ]] &&
        [[ "$(cat "$MARKER")" == "mautrix-whatsapp-call scratch v1" ]] || {
        echo "Refusing to remove unrecognised scratch directory: $WORK" >&2; exit 2;
    }
    rm -rf --one-file-system -- "$WORK"
fi
mkdir -m 0700 "$WORK"
printf '%s\n' "mautrix-whatsapp-call scratch v1" > "$MARKER"
cleanup_scratch() {
    local status=$?
    trap - EXIT
    if ! rm -rf --one-file-system -- "$WORK"; then
        echo "Scratch cleanup failed: $WORK" >&2
        if (( status == 0 )); then status=2; fi
    fi
    exit "$status"
}
trap cleanup_scratch EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
BIN="$WORK/bin"
mkdir -p "$BIN"
REPO_ROOT="$WORK/source"
SOURCE_DIGEST="$(python3 -B "$SCRIPT_DIR/build-record.py" snapshot "$ORIGINAL_ROOT" "$REPO_ROOT")"
SCRIPT_DIR="$REPO_ROOT/build"
source "$SCRIPT_DIR/versions.env"
bash "$SCRIPT_DIR/verify-source.sh"
# Both compilers now consume the same isolated, byte-checked snapshot.
BRIDGE_SRC="$REPO_ROOT/bridge"
RTC_SRC="$REPO_ROOT/rtc"
MEOW_SRC="$BRIDGE_SRC/third_party/meowcaller"
bash "$REPO_ROOT/bootstrap/prepare-build-inputs.sh" "$CACHE"
source "$CACHE/inputs.env"

# Fresh static codec builds derive from the same verified source archive.
bash "$SCRIPT_DIR/prepare-opus.sh" "$CACHE" "$WORK/opus" "$JOBS" 2>&1 | tee "$LOG/opus-build.log"
OPUS_NATIVE_LIB="$WORK/opus/native/.libs"
OPUS_MUSL_LIB="$WORK/opus/musl/.libs"
export CGO_LDFLAGS="-L \"$OPUS_NATIVE_LIB\""
export CGO_ENABLED=1

export PATH="$GOROOT/bin:$PATH"
export GOMODCACHE="$CACHE/go-mod"
export GOCACHE="$CACHE/go-build"
export GOPROXY="https://proxy.golang.org,direct"
export GOSUMDB="sum.golang.org"
export GOTOOLCHAIN="local"
mkdir -p "$GOMODCACHE" "$GOCACHE"
# Downloads may write go.sum even with -mod=readonly. Never accept those
# writes as part of an immutable release build; pin them in source instead.
export GOWORK=off GOENV=off GOFLAGS="-mod=readonly"
export GOPRIVATE="" GONOSUMDB="" GONOPROXY=""
verify_dependency_snapshot() {
    local actual
    actual="$(python3 -B "$SCRIPT_DIR/build-record.py" digest "$REPO_ROOT")"
    [[ "$actual" == "$SOURCE_DIGEST" ]] || {
        echo "Build source changed during dependency preparation; update and review pinned checksums before rebuilding" >&2
        return 20
    }
}
prepare_go_module() (
    cd "$1"
    go mod download all
    verify_dependency_snapshot
    go mod verify
    verify_dependency_snapshot
)
prepare_go_module "$BRIDGE_SRC"
prepare_go_module "$MEOW_SRC"

export CARGO_HOME="$CACHE/cargo"
export RUSTUP_HOME="$RUST_ROOT/rustup"
export PATH="$RUST_ROOT/rustup/toolchains/$RUST_VERSION-x86_64-unknown-linux-gnu/bin:$PATH"
export CARGO_TARGET_DIR="$CACHE/rust-target"
# Release verification does not require DWARF or incremental compiler state.
# Keep dependency artefacts reusable, without multiplying multi-GB debug data.
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
mkdir -p "$CARGO_HOME" "$CARGO_TARGET_DIR"
(
    cd "$RTC_SRC"
    cargo fetch --locked
)
verify_dependency_snapshot

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

if [[ "$SKIP_TESTS" == "0" ]]; then
    python3 -B "$REPO_ROOT/tests/test_project.py" 2>&1 | tee "$LOG/project-fixtures.log"
    DEPLOY="$REPO_ROOT/deployment/mautrix-whatsapp-call"
    (
        python3 -B "$DEPLOY/resolve-appservice.py" --self-test
        python3 -B "$DEPLOY/worker/auto-provision-whatsapp-ghosts.py" --self-test
        bash "$DEPLOY/sync-direct-chats.sh" --self-test
        for test in "$REPO_ROOT"/tests/shell/*.sh; do bash "$test"; done
    ) 2>&1 | tee "$LOG/deployment-fixtures.log"
    bash "$REPO_ROOT/build/test-wire-interop.sh" "$BRIDGE_SRC" 2>&1 | tee "$LOG/wire-interop.log"
    (
        cd "$BRIDGE_SRC"
        go test -p "$JOBS" -mod=readonly -tags "goolm,liveopus" -count=1 -v ./pkg/connector/voipbridge/... ./pkg/connector 2>&1 | tee "$LOG/go-connector.log"
        # The full connector run already executes these tests. Retain explicit
        # evidence for each required regression without running it twice.
        focused=(TestRTCEventProcessorDispatchClasses TestRTCRingLifetimeAfterAcceptAndCancellation
            TestRTCUnansweredRingExpires TestMatrixRTCDMPowerLevels TestMatrixRTCCallDeviceClassification
            TestMatrixRTCCallStaticJIDAvailability TestWhatsAppBusinessCallOptionClassification
            TestWhatsAppBusinessCallProfileResponseClassification TestWhatsAppVoIPFeatureResponseClassification
            TestWhatsAppEndReasonFormatting TestCombineWhatsAppMatrixRTCCallAvailabilityPrecedence)
        for name in "${focused[@]}"; do
            grep -E "^--- PASS: ${name} \(" "$LOG/go-connector.log" || {
                echo "Required connector regression did not pass: $name" >&2; exit 26;
            }
        done | tee "$LOG/go-focused.log"
    )
    (
        cd "$MEOW_SRC"
        MAUTRIX_WHATSAPP_CALL_EXPECT_OPUS_VERSION="$OPUS_VERSION" go test -p "$JOBS" -mod=readonly -tags liveopus -count=1 -v ./liveopus 2>&1 | tee "$LOG/meowcaller.log"
        grep -q "^--- PASS: TestRoundTripNegotiatedFrameLengths " "$LOG/meowcaller.log" || { echo "Native Opus regression did not execute" >&2; exit 26; }
        go test -p "$JOBS" -mod=readonly -tags liveopus -count=1 ./rtp ./mlow ./signaling ./relay ./stun . 2>&1 | tee -a "$LOG/meowcaller.log"
    )
    (
        cd "$RTC_SRC"
        cargo test --locked -p matrix-rtc-core --features testing --lib --test key_rotation -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-core.log"
        cargo test --locked -p matrix-rtc-call -p matrix-rtc-transport --lib --features matrix-rtc-transport/testing -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-call-transport.log"
        cargo test --locked -p matrix-rtc-call-sdk --features matrix-sdk --lib -j "$JOBS" 2>&1 | tee "$LOG/matrix-rtc-call-sdk.log"
        cargo test --locked -p matrix-rtc-call-sdk --features matrix-sdk,testing --test resilience -j "$JOBS" 2>&1 | tee -a "$LOG/matrix-rtc-call-sdk.log"
        cargo test --locked -p matrix-rtc-call-sdk --features matrix-sdk,testing --test e2e_call --no-run -j "$JOBS" 2>&1 | tee -a "$LOG/matrix-rtc-call-sdk.log"
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
    env CC="musl-gcc" CXX="musl-gcc" CGO_ENABLED="1" GOOS="linux" GOARCH="amd64" CGO_LDFLAGS="-L \"$OPUS_MUSL_LIB\"" \
        go build -p "$JOBS" -mod=readonly -buildvcs=false -tags "goolm,netgo,osusergo,liveopus" -trimpath \
        -ldflags='-linkmode external -extldflags "-static"' \
        -o "$BIN/mautrix-whatsapp-call-bridge" ./cmd/mautrix-whatsapp 2>&1 | tee "$LOG/go-build.log"
    [[ -s "$BIN/mautrix-whatsapp-call-bridge" ]] || { echo "Go compiler produced no bridge binary" >&2; exit 23; }
    echo "Go bridge release compilation: PASS" | tee -a "$LOG/go-build.log"
)

cp "$CARGO_TARGET_DIR/release/matrixrtc-whatsapp-worker" "$BIN/mautrix-whatsapp-call-worker"
cp "$CARGO_TARGET_DIR/release/matrixrtc-ghost-provision" "$BIN/mautrix-whatsapp-call-provision"
strip "$BIN/mautrix-whatsapp-call-worker" "$BIN/mautrix-whatsapp-call-provision" || true

"$BIN/mautrix-whatsapp-call-worker" tls-self-test 2>&1 | tee "$LOG/rust-tls-release.log"
(
    cd "$BRIDGE_SRC"
    RTC_TEST_RELEASE_WORKER="$BIN/mautrix-whatsapp-call-worker" go test -mod=readonly -count=1 -v \
        -run '^Test(RTCWorkerRelease|WorkerHarnessToneDiscrimination)$' ./pkg/connector/voipbridge/rtcwire
) 2>&1 | tee "$LOG/worker-process.log"
grep -q '^--- PASS: TestRTCWorkerRelease ' "$LOG/worker-process.log" || { echo "Actual worker process regression did not execute" >&2; exit 23; }

# Only commands that returned success can reach this point (set -e / pipefail).
# Bind the record to actual artifacts and nonempty logs from this fresh run.
[[ "$(python3 -B "$SCRIPT_DIR/build-record.py" digest "$ORIGINAL_ROOT")" == "$SOURCE_DIGEST" ]] || { echo "Checkout changed during build" >&2; exit 20; }
RECORD="$RUN/BUILD-RECORD.json"
python3 -B "$SCRIPT_DIR/build-record.py" create "$RECORD" "$REPO_ROOT" "$BIN" "$LOG" "$SOURCE_DIGEST"

# Serialize publication even if builders use different caches and the same dist.
exec 8>"$DIST/.release.lock"
flock -x 8
STAMP="$(date -u +%Y%m%d-%H%M%S)"
PACKAGE_NAME="mautrix-whatsapp-call-${STAMP}"
PACKAGE_WORK="$WORK/package"
RELEASE="$PACKAGE_WORK/$PACKAGE_NAME"
ZIP_WORK="$PACKAGE_WORK/$PACKAGE_NAME.zip"
SHA_WORK="$PACKAGE_WORK/$PACKAGE_NAME.zip.sha256"
if [[ -e "$DIST/$PACKAGE_NAME.zip" || -L "$DIST/$PACKAGE_NAME.zip" || -e "$DIST/$PACKAGE_NAME.zip.sha256" || -L "$DIST/$PACKAGE_NAME.zip.sha256" ]]; then
    echo "Release name already exists; retaining previous outputs" >&2
    exit 21
fi
mkdir -p "$RELEASE/bin" "$RELEASE/deployment" "$DIST"
python3 -B "$SCRIPT_DIR/runtime-layout.py" stage "$REPO_ROOT" "$RELEASE"
cp "$BIN"/* "$RELEASE/bin/"
cp "$BIN/mautrix-whatsapp-call-worker" "$RELEASE/deployment/mautrix-whatsapp-call/worker/"
cp "$BIN/mautrix-whatsapp-call-provision" "$RELEASE/deployment/mautrix-whatsapp-call/worker/"
cp "$BIN/mautrix-whatsapp-call-bridge" "$RELEASE/deployment/mautrix-whatsapp-call/bridge/"
cp "$SCRIPT_DIR/versions.env" "$RELEASE/SOURCE-VERSIONS.env"
cp "$RECORD" "$RELEASE/BUILD-RECORD.json"

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
python3 -B "$SCRIPT_DIR/build-record.py" verify-local "$RECORD" "$REPO_ROOT" "$BIN" "$LOG" "$SOURCE_DIGEST"
[[ "$(python3 -B "$SCRIPT_DIR/build-record.py" digest "$ORIGINAL_ROOT")" == "$SOURCE_DIGEST" ]] || { echo "Checkout changed before publication" >&2; exit 20; }
# Publish the archive last. A checksum alone is not a deployable release.
# The caller holds the destination lock. Never overwrite pre-existing outputs.
publish_release() (
    local dist="$1" name="$2" archive="$3" checksum="$4" stage sidecar_owned=0
    local final_zip="$dist/$name.zip" final_sha="$dist/$name.zip.sha256"
    if [[ -e "$final_zip" || -L "$final_zip" || -e "$final_sha" || -L "$final_sha" ]]; then
        echo "Release destination already exists; retaining previous outputs" >&2
        return 21
    fi
    stage="$(mktemp -d "$dist/.publish.XXXXXXXX")" || return 21
    cleanup_publication() {
        # Only remove our own sidecar, and only if no archive was published.
        if (( sidecar_owned )) && [[ ! -e "$final_zip" && ! -L "$final_zip" ]] && [[ -f "$final_sha" && ! -L "$final_sha" ]] && cmp -s "$checksum" "$final_sha"; then
            rm -f -- "$final_sha" || true
        fi
        rm -rf -- "$stage"
    }
    trap cleanup_publication EXIT
    trap 'exit 21' HUP INT TERM
    cp -- "$archive" "$stage/$name.zip" || return 21
    cp -- "$checksum" "$stage/$name.zip.sha256" || return 21
    (cd "$stage" && sha256sum -c "$name.zip.sha256") || return 21
    mv -n -- "$stage/$name.zip.sha256" "$final_sha" || return 21
    [[ ! -e "$stage/$name.zip.sha256" ]] || return 21
    sidecar_owned=1
    mv -n -- "$stage/$name.zip" "$final_zip" || return 21
    [[ ! -e "$stage/$name.zip" ]] || return 21
    (cd "$dist" && sha256sum -c "$name.zip.sha256") || return 21
)
if ! publish_release "$DIST" "$PACKAGE_NAME" "$ZIP_WORK" "$SHA_WORK"; then
    echo "Incomplete release export; build logs and record retained at $RUN" >&2
    exit 21
fi
echo "Built release: $DIST/$PACKAGE_NAME.zip"
