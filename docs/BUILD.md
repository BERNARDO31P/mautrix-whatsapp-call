# Build

Builds use the checked-in Go and Rust source in `bridge/` and `rtc/`.

For release packaging, see [RELEASING.md](RELEASING.md). Runtime users do not need the native build toolchain.

Pinned build inputs are prepared under `~/.cache/mautrix-whatsapp-call/`:

- Go 1.27.4
- Rust 1.100.0
- LLVM/Clang 21.1.8
- LiveKit WebRTC `webrtc-a545de4`
- Opus 1.5.2

## Windows: new PC

Open ordinary 64-bit Windows PowerShell under the Windows account that will own the WSL distribution. If Windows system setup is needed, the bootstrap automatically opens a visible elevated setup window; approve UAC. It waits for setup to finish, then resumes under the original user. If setup fails, the elevated window retains the error until you press Enter. On a supported x64 Windows 10/11 PC with virtualization enabled:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Bootstrap.ps1" -BuilderRoot "$HOME\mautrix-whatsapp-call-builder" -Jobs 4
$LASTEXITCODE
```

- `0`: build (or requested preparation) completed.
- `3010`: restart Windows, then rerun the same command. Windows feature activation can require a restart; the bootstrap does not restart automatically.
- `1`: stop and inspect the printed error. Do not deploy a previously generated ZIP as if it were a successful new build.

A successful bootstrap creates or reuses `mautrix-whatsapp-call-Ubuntu-22.04`. It validates the downloaded Ubuntu archive against Canonical's HTTPS checksum manifest and uses the existing pinned toolchain checks. The build runs as root inside that distro, not as a privileged user on the production server. Internet access and adequate storage are required. It does not install a distribution from the Store or reset existing WSL instances.

## Windows + existing WSL2

```powershell
wsl --list --verbose
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Build.ps1" -DistroName "mautrix-whatsapp-call-Ubuntu-22.04" -OutputRoot "$HOME\mautrix-whatsapp-call-builder\dist" -Jobs 4
```

Use `Ubuntu-22.04` instead if that is the registered build distro. If it is WSL1, explicitly convert it with `wsl --set-version Ubuntu-22.04 2` before building. The build selects an AMD64 apt-based WSL2 distro and uses `wslpath` to resolve Windows paths. Use an ordinary local drive, not a UNC/network path. For low-memory PCs use `-Jobs 2` or `-Jobs 1`. Existing toolchain caches are reused. Do not run multiple builds in the same distro/cache simultaneously.

If WSL cannot start, run `wsl --status` and `wsl --list --verbose`; confirm virtualization and Windows updates, and see [Microsoft WSL troubleshooting](https://learn.microsoft.com/windows/wsl/troubleshooting). If an interrupted import leaves a nonempty but unregistered install directory, inspect and move it to a backup location before retrying. Do not unregister a working distro merely to resolve a build failure.

## Linux/WSL direct

```bash
export MAUTRIX_WHATSAPP_CALL_CACHE="$HOME/.cache/mautrix-whatsapp-call"
./build/build.sh
```

Release builds reject `MAUTRIX_WHATSAPP_CALL_SKIP_TESTS=1`. Each attempt retains logs and (only after successful tests and compilation) its build record under `~/.cache/mautrix-whatsapp-call/work/runs/`. The private stable scratch directory is removed on exit; a stale marked scratch directory is reclaimed under the build lock on the next build. Published ZIPs and checksums remain in `dist/`. Rust debug symbols and incremental compilation are disabled for verification builds. The focused Go test evidence is extracted from the single full connector suite. A cache lock excludes concurrent release builds; do not run standalone preparation against an active builder.

For direct builds, output defaults to `dist/mautrix-whatsapp-call-<timestamp>.zip` and `dist/mautrix-whatsapp-call-<timestamp>.zip.sha256`.

## Reclaiming build space on Windows

Run from the repository root. Preview first; the default compiler cache policy removes the Rust debug target on apply. This costs a one-time test rebuild, but preserves release compilation caches, pinned toolchains and dependency downloads.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Cleanup.ps1" -DistroName "Ubuntu-22.04"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Cleanup.ps1" -DistroName "Ubuntu-22.04" -Apply
```

Use your actual build distribution. `-CompilerCache keep` keeps all compiled caches; `-CompilerCache all` additionally removes Rust release and Go compiled caches. Cleanup preserves logs and build records but deletes old unpublished packages, source snapshots and intermediate binaries. Copy any unpublished artefacts needed for recovery before applying. Published ZIPs are not touched. Only the default project cache is cleaned by the Windows wrapper. No Docker or production state is changed. The reported bytes are inside Linux; WSL's dynamically allocated VHDX may not shrink immediately on Windows.
