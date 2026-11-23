# mautrix-whatsapp-call

`mautrix-whatsapp-call` adds WhatsApp audio and video calling to **mautrix-whatsapp** rooms through MatrixRTC and LiveKit while preserving normal WhatsApp message bridging.

The repository contains the source required to build the modified mautrix-whatsapp bridge and the MatrixRTC call worker. Large build inputs such as Go, Rust, LLVM/Clang and the LiveKit WebRTC bundle are **not committed**; the bootstrap downloads pinned versions and verifies their SHA-256 digests.

## Installation and documentation

**Installing on a Matrix server? Start with the [complete installation guide](deployment/mautrix-whatsapp-call/INSTALLATION.md).** It covers the prebuilt runtime, an existing Synapse/WhatsApp stack, automatic LiveKit and JWT service installation, credential generation, actual configuration examples, DNS/TLS, reverse proxy, firewall, room migration and verification. No separate LiveKit installation or build toolchain is required on the runtime host.

For upgrades, backup/restore, secret recovery, troubleshooting and the live test checklist, see the [operations guide](deployment/mautrix-whatsapp-call/OPERATIONS.md). Both guides are included in future runtime packages. Maintainers publishing builds should also see [Releasing](docs/RELEASING.md).

## What it provides

- outgoing and incoming WhatsApp audio/video calls from MatrixRTC-capable Matrix clients;
- call accept, decline, cancel, no-answer and hangup handling;
- audio/video mode changes and microphone/camera state changes;
- WhatsApp business/callability checks;
- direct and group-call support;
- encrypted persistent MatrixRTC ghost identities;
- LiveKit SFU + Element JWT service integration;
- idempotent room/permission/m.direct migration for existing mautrix-whatsapp portals;
- an upgrade-safe installer that preserves generated credentials and worker state.

## Supported deployment model

This project extends an **existing** Docker Compose Matrix installation. The supported server layout is:

```text
/opt/matrix/
├── docker-compose.yml
├── .env
├── synapse/
├── mautrix-whatsapp/
└── mautrix-whatsapp-call/
```

The existing Compose project must expose services named `synapse` and `mautrix-whatsapp`. The default mautrix-whatsapp files are expected at:

```text
/opt/matrix/mautrix-whatsapp/config.yaml
/opt/matrix/mautrix-whatsapp/registration.yaml
```

If the registration is elsewhere, set `MATRIXRTC_WHATSAPP_REGISTRATION_PATH` in `/opt/matrix/.env`.

The build target is Linux AMD64. Windows builds use WSL2.

## Prebuilt release download

Most users do **not** need Go, Rust, LLVM/Clang or WebRTC build tooling. Download the prebuilt Linux AMD64 runtime ZIP and its checksum from GitHub Releases:

```text
https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/latest
```

Release assets use the build output name:

```text
mautrix-whatsapp-call-<timestamp>.zip
mautrix-whatsapp-call-<timestamp>.zip.sha256
```

For the initial `v0.1.0` release, the direct asset path is:

```text
https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/download/v0.1.0/mautrix-whatsapp-call-20261123-233131.zip
https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/download/v0.1.0/mautrix-whatsapp-call-20261123-233131.zip.sha256
```

See [docs/DEPLOY.md](docs/DEPLOY.md) for verification and installation commands.

## Repository layout

```text
bridge/                         modified mautrix-whatsapp + integrated meowcaller
rtc/                            MatrixRTC crates + WhatsApp RTC worker
bootstrap/                      pinned toolchain/WebRTC preparation
build/                          build, source verification and packaging
deployment/mautrix-whatsapp-call/ Docker Compose overlay + installer
tests/                          project/deployment regression tests
docs/                           architecture, build, deploy, migration and provenance
licenses/                       license texts/notices
```

Unrelated upstream mobile/web/demo artifacts, downloaded toolchains, dependency caches, compiled binaries and runtime secrets are intentionally not part of the repository.

## Quick start: Windows + WSL2

### 1. Clone

```powershell
git clone https://github.com/YOUR_GITHUB_USER/mautrix-whatsapp-call.git
cd mautrix-whatsapp-call
```

### 2. Clean-machine bootstrap

Run this when WSL2/the dedicated build environment has not been prepared yet:

```powershell
powershell.exe `
    -NoProfile `
    -ExecutionPolicy Bypass `
    -File ".\Bootstrap.ps1"
```

The bootstrap can enable WSL2, create the default Ubuntu 22.04 build distro, install Linux build packages, and download the pinned Go/Rust/LLVM/WebRTC inputs.

A reboot may be required the first time Windows enables WSL2.

### 3. Build

If you already have a WSL distribution you want to use:

```powershell
powershell.exe `
    -NoProfile `
    -ExecutionPolicy Bypass `
    -File ".\Build.ps1" `
    -DistroName "Ubuntu-22.04"
```

Otherwise the distro created by `Bootstrap.ps1` can be used by running `Bootstrap.ps1` again without `-PrepareOnly`.

The build performs source verification and regression tests before producing:

```text
dist/mautrix-whatsapp-call-<timestamp>.zip
dist/mautrix-whatsapp-call-<timestamp>.zip.sha256
```

## Quick start: native Linux build

On a supported Debian/Ubuntu-style Linux AMD64 build host:

```bash
git clone https://github.com/YOUR_GITHUB_USER/mautrix-whatsapp-call.git
cd mautrix-whatsapp-call
bash build/build.sh
```

`build/build.sh` invokes the build-input preparation automatically. Missing system packages are installed with `apt` (using `sudo` when needed), and pinned external toolchains are stored under `~/.cache/mautrix-whatsapp-call/`.

To prepare inputs without compiling:

```bash
bash bootstrap/prepare-build-inputs.sh
```

## Server installation

### 1. Configure the existing Matrix Compose project

At minimum, `/opt/matrix/.env` must contain your Matrix server name:

```dotenv
MATRIXRTC_SERVER_NAME=matrix.example.com
MATRIXRTC_ENABLE_CALLS=0
MATRIXRTC_SOCKET_GID=29319
```

Keep calls disabled (`0`) until the initial installation and verification complete. Set it to `1` afterward and rerun the installer to enable calls.

If mautrix-whatsapp's appservice registration is not at the default path, also set for example:

```dotenv
MATRIXRTC_WHATSAPP_REGISTRATION_PATH=./mautrix-whatsapp/registration.yaml
```

### 2. Copy the built runtime package to the server

From Windows PowerShell:

```powershell
$Release = Get-ChildItem ".\dist\mautrix-whatsapp-call-*.zip" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

scp `
    $Release.FullName `
    "$($Release.FullName).sha256" `
    root@YOUR_SERVER:/root/
```

### 3. Verify and extract it on the server

```bash
cd /root
ZIP="$(ls -1t mautrix-whatsapp-call-*.zip | head -n 1)"
SHA="${ZIP}.sha256"
EXPECTED="$(awk '{print $1}' "$SHA")"
ACTUAL="$(sha256sum "$ZIP" | awk '{print $1}')"
test "$EXPECTED" = "$ACTUAL"

RELEASE="/root/${ZIP%.zip}"
rm -rf "$RELEASE"
unzip -q "$ZIP" -d /root
cd "$RELEASE"
sha256sum -c SHA256SUMS
```

### 4. Install the deployment overlay

```bash
cd /opt/matrix
mkdir -p mautrix-whatsapp-call

rsync -a --delete \
    --exclude="generated/" \
    --exclude="worker-state/" \
    "$RELEASE/deployment/mautrix-whatsapp-call/" \
    /opt/matrix/mautrix-whatsapp-call/
```

For an upgrade, **never delete** these two directories:

```text
/opt/matrix/mautrix-whatsapp-call/generated/
/opt/matrix/mautrix-whatsapp-call/worker-state/
```

They contain generated credentials and persistent encrypted MatrixRTC identity state. They are ignored by Git and normalized to private permissions by the installer.

Validate Compose and install:

```bash
cd /opt/matrix

docker compose \
    -f docker-compose.yml \
    -f mautrix-whatsapp-call/compose.yml \
    config >/dev/null

bash mautrix-whatsapp-call/install-or-upgrade.sh
bash mautrix-whatsapp-call/verify-worker.sh
```

The installer temporarily disables calls while it updates Synapse/bridge configuration, migrates existing rooms and provisions RTC identities. It restores the previous `MATRIXRTC_ENABLE_CALLS` setting only after verification.

### 5. Reverse proxy and firewall

The public Matrix HTTPS virtual host must proxy:

- `/livekit/jwt` to the local JWT service;
- `/livekit/sfu` to the local LiveKit HTTP/WebSocket endpoint.

An nginx fragment is supplied at:

```text
deployment/mautrix-whatsapp-call/nginx-443-locations.conf
```

Render/use it for your Matrix hostname and reload nginx after validation.

LiveKit also needs its media ports reachable from Matrix clients:

- TCP `7881`
- UDP `7882`

Do not expose the local-only JWT port (`18080`) or LiveKit HTTP port (`7880`) directly; the Compose overlay binds them to `127.0.0.1`.

### 6. Enable calls

After installation succeeds, set:

```dotenv
MATRIXRTC_ENABLE_CALLS=1
```

Then rerun:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/install-or-upgrade.sh
bash mautrix-whatsapp-call/verify-worker.sh
```

Force-stop/reopen Matrix clients once after an upgrade so cached call eligibility is refreshed.

## Updating

There are three different update paths: project releases, ordinary container-image updates, and upstream dependency bumps. They are intentionally separate. See [`docs/UPDATING.md`](docs/UPDATING.md) for the complete procedure.

For a normal `mautrix-whatsapp-call` release upgrade, pull/build the newer source revision, copy the new runtime ZIP to the server, replace only the deployment files with `rsync` while excluding `generated/` and `worker-state/`, then rerun:

```bash
bash /opt/matrix/mautrix-whatsapp-call/install-or-upgrade.sh
```

The installer is designed to be idempotent and to preserve credentials/state.

When updating unrelated containers in the Matrix stack, always include this project's Compose overlay. A safe pull is:

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

`--ignore-buildable` deliberately skips the locally built call bridge and worker images. Do not run the base Compose file by itself when recreating `mautrix-whatsapp`, because that can recreate the ordinary upstream bridge image without the calling extension.

## Verification and tests

Source-only checks:

```bash
bash build/verify-source.sh
python3 -B tests/test_project.py
for test in tests/shell/*.sh; do bash "$test"; done
```

A normal build also runs focused Go tests, meowcaller tests, MatrixRTC core tests and RTC-worker tests before creating a runtime package.

## What is deliberately not committed

`.gitignore` excludes:

- `dist/`, `target/`, caches and logs;
- downloaded archives/toolchains;
- generated bridge/worker binaries;
- `.env`, generated registrations/secrets and worker crypto state;
- databases and IDE/OS state.

The repository **does** keep codec test vectors and other source fixtures required by the regression suite. Those are source/test data, not downloaded build dependencies.

## Security

Do not commit or share:

- `/opt/matrix/.env`;
- `generated/`;
- `worker-state/`;
- mautrix-whatsapp access tokens/registration secrets;
- Matrix access tokens, recovery keys or crypto stores.

The main call worker does not receive the mautrix-whatsapp appservice registration. Provisioning is separated into the dedicated provisioner container.

See `SECURITY.md` and `docs/ARCHITECTURE.md` for more details.

## Documentation

- `docs/BUILD.md`
- `docs/DEPLOY.md`
- `docs/MIGRATION.md`
- `docs/ARCHITECTURE.md`
- `docs/TESTING.md`
- `docs/PROVENANCE.md`

## Licensing

This repository contains modified source derived from projects with different licenses. See `LICENSES.md`, the retained upstream license files under `bridge/`, `bridge/third_party/meowcaller/` and `rtc/`, and `licenses/` before redistribution.
