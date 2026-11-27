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

For the full real-world acceptance matrix (both call directions, modes, termination, business eligibility, reconnect, media, migration and ordinary messages), see [Live acceptance checklist](../deployment/mautrix-whatsapp-call/OPERATIONS.md#5-live-acceptance-checklist-for-every-deployment-or-dependency-update). 

## Dependency-free Go race checks

These checks exercise the local wire protocol, media bus, lifecycle manager and alias cache without fetching upstream modules. They supplement rather than replace the normal pinned build.

```bash
(cd bridge/pkg/connector/voipbridge && GO111MODULE=off GOTOOLCHAIN=local go test -race -count=1 ./...)
(cd bridge/pkg/connector && GO111MODULE=off GOTOOLCHAIN=local go test -race -count=1 matrixrtc_availability_cache.go matrixrtc_availability_cache_test.go)
```

Release-layout tests use synthetic ELF headers, not real native binaries. Run the full build and live call tests to verify interoperability.

## Call compatibility tests

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

To test client interoperability, verify the actual Element X
client's call membership and token flow. On an isolated Matrix/LiveKit test
stack, the bundled upstream `rtc/crates/matrix-rtc-call-sdk/tests/E2E_CALL.md`
explains how to run the ignored end-to-end tests, including
`e2e_call_single_focus_room_state` and `e2e_call_rejoin_room_state`.
Those tests exercise Rust clients with encrypted media; they do not substitute
for WhatsApp-to-Element X and Element X-to-WhatsApp acceptance tests.
Do not run the throwaway-user or container-restart scenarios on production.

## Go/Rust wire interoperability

`bash build/test-wire-interop.sh` (with the pinned Go and Rust toolchains on
PATH) exchanges 26 semantic fixtures between the actual Go and Rust v1 codecs
in both directions. The test uses a fresh temporary directory and verifies
exact end-of-stream, identity and membership fields, PCM bytes, H.264 payload
framing, media controls and call lifecycle messages. The full build runs these tests automatically unless tests are explicitly skipped. No network or account
is used. The corpus tests in each language also run in their ordinary unit
suites; the Go-only check can run with `GO111MODULE=off GOTOOLCHAIN=local` from
`bridge/pkg/connector/voipbridge/rtcwire`.

This is a wire compatibility regression, not a call-state-machine, decoder or
live media test. In particular, H.264 fixture bytes are framing samples, not
an assertion that a real video decoder can play them.

## Worker integration tests

The release build runs `TestRTCWorkerRelease` from the Go
`rtcwire` package against the compiled RTC-enabled worker. The
worker gets a private temporary Unix socket and empty state, no credentials,
and a minimal environment. It exercises disabled/enabled-but-unconfigured
admission in both directions and dialects, rejected ID reuse, malformed frame
isolation, real socket shutdown with a pending connection, and restart using
the same empty state. Packaging requires the named test's PASS record. This
is separate from the cross-language codec fixture exchange; it does not
prove successful encrypted call setup.

The ignored SDK `worker_bridge::e2e_worker_bridge_staging` test drives the
same Go codec over the real worker, real Matrix signalling, trusted focus,
media-key exchange and real encrypted SFU audio. The Go driver substitutes
only the WhatsApp provider boundary. It supplies a paced 440 Hz tone and
checks a 660 Hz tone from the Matrix peer; the peer independently checks the
440 Hz tone after decrypting it. It checks call-ID isolation, dial/answer
ordering, cancellation before answer, terminal closure and eventual empty
membership. It has distinct `incoming`/`outgoing`, `msc`/`room_state`, and
`gateway`/`peer`/`cancel` cases. The test checks admission, media and termination.

Use **only** a separate TLS-enabled disposable Synapse/LiveKit deployment
with the production-shaped `/livekit/jwt` and `/livekit/sfu` paths; never
point the harness at an existing production room or worker state. Prepare
one encrypted DM containing exactly two test users and suitable power levels
for both users to publish membership and open/close a slot. The users must
have independently prepared, cross-signed test devices. Use fresh fixture
rooms if an earlier failure left memberships or unresolved delayed events.
Set up a private dedicated directory named `rtc-worker-e2e-*` with the normal
worker `identities/<sha256(mxid)>/{session.json,crypto}` layout for the ghost;
put the prepared peer's own `session.json` and `crypto` directory under
`test-peer/` in that root. Both stores must use the designated private
passphrase file. No production credentials or state should be copied there.
The harness never registers a user, reauthenticates a test device, changes
ordinary messaging, starts Docker or invokes production deployment scripts.

In the pinned Linux/WSL build environment, compile the Go test driver and
select one disposable test case:

```bash
(cd bridge && go test -mod=readonly -c -o /tmp/rtc-worker-e2e-driver ./pkg/connector/voipbridge/rtcwire)
export RTC_WORKER_E2E_DISPOSABLE=1
export RTC_WORKER_E2E_SERVER="staging.example.test"
export RTC_WORKER_E2E_STATE="/absolute/path/rtc-worker-e2e-fixture"
export RTC_WORKER_E2E_PASSPHRASE_FILE="/absolute/path/staging-passphrase"
export RTC_WORKER_E2E_GHOST="@ghost:staging.example.test"
export RTC_WORKER_E2E_ROOM="!fixture:staging.example.test"
export RTC_WORKER_E2E_SLOT="m.call#room"
export RTC_TEST_RELEASE_WORKER="/absolute/path/mautrix-whatsapp-call-worker"
export RTC_WORKER_E2E_GO_HARNESS="/tmp/rtc-worker-e2e-driver"
printf 'disposable\n' > "$RTC_WORKER_E2E_STATE/.matrixrtc-e2e-disposable"
export RTC_WORKER_E2E_FORMAT="room_state"
export RTC_WORKER_E2E_DIRECTION="incoming"
export RTC_WORKER_E2E_END="gateway"
(cd rtc && cargo test --locked -p matrix-rtc-call-sdk --features matrix-sdk,testing \
  --test e2e_call worker_bridge::e2e_worker_bridge_staging -- --exact --ignored --nocapture --test-threads=1)
```

Run all twelve combinations serially with suitable disposable fixtures. For
legacy deployments use the actual slot identifier of the fixture. A live
result requires both independent tone assertions (except the deliberately
unanswered cases) and the terminal/cleanup assertions. The provider's real
relay, WhatsApp signaling, camera/screen media, group topology, and stock
Element X UI still require separate end-to-end tests.
