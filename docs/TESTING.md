# Testing

`build/build.sh` runs the repository-level fixtures plus the focused Go and Rust suites before producing a deployable package.

The source-only checks can be run without the full toolchain:

```bash
./build/verify-source.sh
python3 -B tests/test_project.py
python3 -B deployment/mautrix-whatsapp-call/resolve-appservice.py --self-test
python3 -B deployment/mautrix-whatsapp-call/worker/auto-provision-whatsapp-ghosts.py --self-test
bash deployment/mautrix-whatsapp-call/sync-direct-chats.sh --self-test
for test in tests/shell/*.sh; do bash "$test"; done
```

The full build additionally runs:

- focused mautrix connector/callability tests;
- the connector and `voipbridge` Go suites;
- meowcaller RTP, codec, signalling and lifecycle tests;
- `matrix-rtc-core` tests;
- the worker tests without RTC media;
- the worker tests with the `rtc` feature;
- final bridge and worker release builds;
- runtime package checksum/layout verification.

Live WhatsApp/Matrix acceptance remains a deployment concern and is not simulated by the offline source tests.

For the full real-world acceptance matrix (both call directions, modes, termination, business eligibility, reconnect, media, migration and ordinary messages), see [Live acceptance checklist](../deployment/mautrix-whatsapp-call/OPERATIONS.md#5-live-acceptance-checklist-for-every-deployment-or-dependency-update). Record actual observations; do not infer live-media success from offline test counts or worker health.

## Dependency-free Go race checks

These checks exercise the local wire protocol, media bus, lifecycle manager and alias cache without fetching upstream modules. They supplement rather than replace the normal pinned build.

```bash
(cd bridge/pkg/connector/voipbridge && GO111MODULE=off GOTOOLCHAIN=local go test -race -count=1 ./...)
(cd bridge/pkg/connector && GO111MODULE=off GOTOOLCHAIN=local go test -race -count=1 matrixrtc_availability_cache.go matrixrtc_availability_cache_test.go)
```

Release-layout tests use synthetic ELF headers, not real native binaries. The complete release gate must compile and test all three executables, then use real calls and migration/recovery tests in a non-production deployment. Source guards, hashes and successful fixture tests do not prove that external protocols interoperate.

## Call compatibility regression gates

A successful socket health probe alone is not a successful call test. The
release build now runs the core, call, transport and native call SDK library
suites, then invokes `mautrix-whatsapp-call-worker tls-self-test` on the actual
compiled release executable. This constructs the Rustls and reqwest TLS clients
without needing a homeserver, and fails packaging if the combined feature set
cannot initialize a crypto provider. It does not prove a remote TLS handshake.

For the legacy room-state dialect, the regression fixtures check encrypted-room
membership, both legacy state event names, the historical uppercase `ROOM`
slot and the user/device identity fallback. The native feeder delivers the
room's state, encryption, joined members and sticky snapshot in one input; a
failed state request is retried instead of treated as an empty roster.

Before declaring the bridge release functional, verify the actual Element X
client's call membership and token flow. On an isolated Matrix/LiveKit test
stack, the bundled upstream `rtc/crates/matrix-rtc-call-sdk/tests/E2E_CALL.md`
explains how to run the ignored end-to-end tests, including
`e2e_call_single_focus_room_state` and `e2e_call_rejoin_room_state`.
Those tests exercise Rust clients with encrypted media; they do not substitute
for WhatsApp-to-Element X and Element X-to-WhatsApp acceptance tests.
Do not run the throwaway-user or container-restart scenarios on production.
