#!/usr/bin/env bash
# Offline Go <-> Rust codec exchange. Uses the pinned toolchains from build.sh.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRIDGE_SRC="${1:-$ROOT/bridge}"
WORK="$(mktemp -d)"
trap 'rm -rf -- "$WORK"' EXIT
export RTC_WIRE_INTEROP_DIR="$WORK"
(
    cd "$BRIDGE_SRC"
    go test -mod=readonly -count=1 -run '^TestRTCWireInteropExport$' ./pkg/connector/voipbridge/rtcwire
)
[[ -s "$WORK/go.frames" ]]
(
    cd "$ROOT/rtc"
    cargo test --locked -p matrixrtc-whatsapp-worker --bin matrixrtc-whatsapp-worker protocol::interop_tests::cross_language_exchange -- --exact
)
[[ -s "$WORK/rust.frames" ]]
(
    cd "$BRIDGE_SRC"
    go test -mod=readonly -count=1 -run '^TestRTCWireInteropImport$' ./pkg/connector/voipbridge/rtcwire
)
[[ "$(cat "$WORK/go-import.ok")" == PASS ]]
echo "Go/Rust wire interoperability: PASS"
