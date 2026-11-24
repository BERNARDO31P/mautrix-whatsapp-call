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
