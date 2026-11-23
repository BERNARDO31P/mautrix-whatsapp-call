# Testing

`build/build.sh` runs the repository-level fixtures plus the focused Go and Rust suites before producing a deployable package.

The source-only checks can be run without the full toolchain:

```bash
./build/verify-source.sh
python3 -B tests/test_project.py
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
