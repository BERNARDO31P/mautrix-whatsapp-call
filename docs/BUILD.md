# Build

The checked-in `bridge/` and `rtc/` trees are the effective source. The build does not download or apply source patches.

For the complete release publishing, checksum and build troubleshooting workflow, see [RELEASING.md](RELEASING.md). Server operators can use prebuilt runtime ZIPs without this toolchain; see the [installation guide](../deployment/mautrix-whatsapp-call/INSTALLATION.md).

Pinned build inputs are prepared under `~/.cache/mautrix-whatsapp-call/`:

- Go 1.27.4
- Rust 1.100.0
- LLVM/Clang 21.1.8
- LiveKit WebRTC `webrtc-a545de4`

## Windows + existing WSL

```powershell
cd "$HOME\Downloads\mautrix-whatsapp-call"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Build.ps1" -DistroName "Ubuntu-22.04"
```

## Linux/WSL direct

```bash
export MAUTRIX_WHATSAPP_CALL_CACHE="$HOME/.cache/mautrix-whatsapp-call"
./build/build.sh
```

Do not set `MAUTRIX_WHATSAPP_CALL_SKIP_TESTS=1` for a release intended for deployment.

Output is written to `dist/mautrix-whatsapp-call-<timestamp>.zip` and `dist/mautrix-whatsapp-call-<timestamp>.zip.sha256`.
