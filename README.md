# mautrix-whatsapp-call

WhatsApp voice and video calling for Matrix, based on [mautrix-whatsapp](https://github.com/mautrix/whatsapp), MatrixRTC and LiveKit.

The project extends an existing mautrix-whatsapp installation. Messages continue to use the original WhatsApp bridge.

> Calling is experimental. Native builds and deployment checks pass, but end-to-end WhatsApp audio and video have not yet been verified.

## Features

- Incoming and outgoing audio/video calls
- Call acceptance, rejection and termination
- Microphone and camera controls
- WhatsApp call availability and business account detection
- Group call support with an aggregate bridge participant

See [client compatibility](docs/CLIENT-CALL-COMPATIBILITY.md) for limitations and experimental client extensions.

## Requirements

- Linux AMD64 (x86_64)
- Docker and Docker Compose
- An existing Synapse and mautrix-whatsapp installation
- A MatrixRTC-capable client, such as Element X
- HTTPS reverse proxy and reachable LiveKit media ports

The installer configures LiveKit and the Element JWT service. See the [installation guide](deployment/mautrix-whatsapp-call/INSTALLATION.md) for the supported topology and ports.

## Installation

Download the runtime ZIP and its matching `.sha256` file from [Releases](https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases).

On Linux, verify the archive before extracting it:

```bash
sha256sum -c mautrix-whatsapp-call-YYYYMMDD-HHMMSS.zip.sha256
```

Replace the example filename with the downloaded filename. Follow the [installation guide](deployment/mautrix-whatsapp-call/INSTALLATION.md) to configure your existing Matrix deployment.

For updates, see the [operations guide](deployment/mautrix-whatsapp-call/OPERATIONS.md). Preserve the existing configuration, generated credentials, WhatsApp sessions and worker state.

## Build from source

### Linux

```bash
git clone https://github.com/BERNARDO31P/mautrix-whatsapp-call.git
cd mautrix-whatsapp-call
bash build/build.sh
```

### Windows (WSL2)

From the source directory, run:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Bootstrap.ps1" -Jobs 4
```

For an existing Ubuntu WSL2 distribution:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Build.ps1" -Jobs 4
```

The build downloads the pinned toolchain and reuses cached dependencies. See [build instructions](docs/BUILD.md) for requirements, options and troubleshooting.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Client compatibility and extensions](docs/CLIENT-CALL-COMPATIBILITY.md)
- [Build instructions](docs/BUILD.md)
- [Installation](deployment/mautrix-whatsapp-call/INSTALLATION.md)
- [Operations](deployment/mautrix-whatsapp-call/OPERATIONS.md)
- [Migration](docs/MIGRATION.md)
- [Updating dependencies](docs/UPDATING.md)
- [Testing](docs/TESTING.md)
- [Publishing releases](docs/RELEASING.md)
- [Upstream sources and dependencies](docs/PROVENANCE.md)

## Security

See [SECURITY.md](SECURITY.md).

## Licence

See [LICENSES.md](LICENSES.md) for the applicable licences and upstream notices.
