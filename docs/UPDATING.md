# Updating mautrix-whatsapp-call and its dependencies

`mautrix-whatsapp-call` intentionally pins its source, toolchain and runtime dependencies. A normal Docker image pull is **not** the mechanism used to update the modified bridge or the call worker.

## 1. Normal project release update

For normal users, the recommended dependency-update path is to install a newer tested prebuilt release rather than manually changing individual pins or rebuilding dependencies.

Download releases from:

```text
https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/latest
```

Download both `mautrix-whatsapp-call-<timestamp>.zip` and the matching `.zip.sha256`, verify them, then run the normal server upgrade flow.

Maintainers or users who intentionally build from source can instead update the checkout:

On the build machine:

```bash
git pull --ff-only
bash build/verify-source.sh
```

Build a new runtime package using the documented Windows/WSL or native-Linux build flow. Upload the resulting `mautrix-whatsapp-call-<timestamp>.zip` and `.sha256` to the server.

On the server, replace only the deployment files. Preserve these directories across every upgrade:

```text
/opt/matrix/mautrix-whatsapp-call/generated/
/opt/matrix/mautrix-whatsapp-call/worker-state/
```

Then run:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/install-or-upgrade.sh
bash mautrix-whatsapp-call/verify-worker.sh
```

The installer temporarily disables calls while changing runtime wiring and restores the previous `MATRIXRTC_ENABLE_CALLS` value only after verification succeeds.

## 2. Updating the rest of the Docker Compose stack

The call-enabled `mautrix-whatsapp` service is defined by merging the existing Matrix stack with this overlay:

```text
docker-compose.yml
mautrix-whatsapp-call/compose.yml
```

The overlay changes `mautrix-whatsapp` to use the locally built image:

```text
mautrix-whatsapp-call-bridge:local
```

It also adds the worker socket mount and the call worker/provisioner services.

### Safe container pull

To update registry-backed services such as Synapse, LiveKit or the JWT service without trying to pull locally built project images:

```bash
cd /opt/matrix

docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    pull --ignore-buildable

docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    up -d
```

`--ignore-buildable` is important: the bridge, worker and provisioner are local build services, not images that should be fetched from a public registry.

### Do not recreate the stack using only the base Compose file

If you run commands such as:

```bash
docker compose -f docker-compose.yml pull
docker compose -f docker-compose.yml up -d
```

without the `mautrix-whatsapp-call/compose.yml` overlay, Docker Compose sees only the original `mautrix-whatsapp` service definition. If that base service points at the normal upstream mautrix-whatsapp image, Compose may pull and recreate that ordinary image. Message bridging should still work, but the calling extension, private worker socket and call-worker integration are no longer attached to that bridge container.

This does not destroy the persistent call state. Reapplying the overlay and running the installer restores the call-enabled bridge, but avoiding the accidental downgrade is preferable.

### Optional: make the overlay automatic

On a Linux server using the supported `/opt/matrix` layout, you may put this in `/opt/matrix/.env`:

```dotenv
COMPOSE_FILE=docker-compose.yml:mautrix-whatsapp-call/compose.yml
```

After that, commands run from `/opt/matrix` can use simply:

```bash
docker compose config
docker compose pull --ignore-buildable
docker compose up -d
```

Do not overwrite an existing custom `COMPOSE_FILE`; merge the project overlay into it instead. Explicit `-f` arguments remain the least ambiguous option.

## 3. Updating pinned upstream dependencies as a maintainer

Dependency bumps are source changes and must go through the full build/test/release process. Do not update only a version string.

The central pin file is:

```text
build/versions.env
```

### mautrix-whatsapp

The modified bridge source is vendored under `bridge/`.

When moving to a newer mautrix-whatsapp release:

1. merge/rebase the new upstream source into `bridge/` while preserving the call integration;
2. update `MAUTRIX_VERSION` and `MAUTRIX_BASE_COMMIT` in `build/versions.env`;
3. update the default base-image tag in `deployment/mautrix-whatsapp-call/bridge/Dockerfile` and `deployment/mautrix-whatsapp-call/compose.yml`;
4. recompute `BRIDGE_SOURCE_TREE_SHA256` using the same digest logic as `build/verify-source.sh`;
5. run the full Go, worker and integration test suite and build a runtime package.

Changing only `MATRIXRTC_BRIDGE_BASE_IMAGE` updates the container base but does **not** update the checked-in modified bridge source. Those versions must stay compatible.

### meowcaller

The effective meowcaller source is vendored at:

```text
bridge/third_party/meowcaller/
```

For an update:

1. merge the new upstream meowcaller source into that directory;
2. update `MEOWCALLER_BASE_COMMIT`;
3. update Go module sums/dependencies if upstream changed them;
4. recompute `MEOWCALLER_SOURCE_TREE_SHA256` and the enclosing `BRIDGE_SOURCE_TREE_SHA256`;
5. run all meowcaller and bridge tests.

### matrix-rust-rtc / Matrix Rust SDK

MatrixRTC source is vendored under `rtc/`. The Matrix Rust SDK revision and Rust dependencies are resolved through Cargo.

For an update:

1. merge the desired matrix-rust-rtc upstream revision into `rtc/`;
2. update `MATRIX_RUST_RTC_BASE_COMMIT`;
3. update `MATRIX_RUST_SDK_REV` and the Cargo manifests when the SDK pin changes;
4. regenerate/update `rtc/Cargo.lock` deliberately and keep `--locked` builds passing;
5. update `RTC_SOURCE_TREE_SHA256`;
6. run MatrixRTC core tests, default worker tests and the RTC-enabled worker tests.

### Go, Rust, LLVM/Clang and LiveKit WebRTC build inputs

These large build inputs are downloaded into `~/.cache/mautrix-whatsapp-call/`, not committed to Git.

Their versions, URLs and integrity values live in `build/versions.env` and are consumed by `bootstrap/prepare-build-inputs.sh`.

When bumping one of them, update all corresponding version/URL/SHA-256 values and recompute the relevant prepared-input digest (`*_INPUT_TREE_SHA256`). The bootstrap will discard/recreate an incompatible cached input automatically.

The important pins include:

- `GO_VERSION` / `GO_SHA256`;
- `RUST_VERSION` plus the Rust component URLs and SHA-256 values;
- `LLVM_VERSION` / `LLVM_SHA256` / `LLVM_SIZE`;
- `WEBRTC_RELEASE_TAG` / `WEBRTC_SHA256`;
- `WEBRTC_SYS_VERSION` and `LIBWEBRTC_CRATE_VERSION` when the Rust WebRTC crates change.

### LiveKit server and Element JWT service

Their runtime image versions are pinned directly in:

```text
deployment/mautrix-whatsapp-call/compose.yml
```

Update the image tags there, then run the full runtime verification. A plain `docker compose pull` does not advance a pinned tag to a new release; it only fetches the image currently named by that tag.

## 4. Required validation after any dependency bump

At minimum run the source/project checks:

```bash
bash build/verify-source.sh
python3 -B tests/test_project.py
for test in tests/shell/*.sh; do bash "$test"; done
```

Then run a normal full build. The build additionally exercises focused Go tests, meowcaller tests, MatrixRTC core tests, default worker tests and RTC-enabled worker tests before producing the runtime ZIP.

After deploying, run:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/verify-worker.sh
```

Finally perform live outgoing/incoming audio/video acceptance tests before publishing the dependency bump as a release.
