#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EVENTS="$ROOT/bridge/pkg/connector/matrixrtc_events.go"
TESTS="$ROOT/bridge/pkg/connector/matrixrtc_call_test.go"
grep -F '"business_calling_enabled"' "$EVENTS" >/dev/null
grep -F 'Tag:   "business_calling"' "$TESTS" >/dev/null
grep -F 'business negative beats consumer fallback' "$TESTS" >/dev/null
grep -F 'business negative beats feature positive' "$TESTS" >/dev/null
grep -F 'feature negative beats business positive' "$TESTS" >/dev/null
grep -F 'feature timeout leaves consumer callable' "$TESTS" >/dev/null
grep -F 'hosted unknown fails closed' "$TESTS" >/dev/null
echo "Business callability source guards: PASS"
