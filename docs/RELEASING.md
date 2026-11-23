# Building and publishing a runtime release

Most operators should download a prebuilt runtime and follow the [installation guide](../deployment/mautrix-whatsapp-call/INSTALLATION.md). This page is for maintainers. See [BUILD.md](BUILD.md) for toolchain pins and [UPDATING.md](UPDATING.md) for source/dependency changes.

## Prepare and build

The Git repository is source-only. Keep your existing checkout and `.git` history; do not extract a source ZIP over it. Clone if you do not yet have it:

```powershell
git clone "https://github.com/BERNARDO31P/mautrix-whatsapp-call.git"
cd mautrix-whatsapp-call
```

For a prepared Windows WSL2 host, list available distros and build using your actual distro name:

```powershell
wsl.exe --list --verbose
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Build.ps1" -DistroName "Ubuntu-22.04"
```

For an unprepared machine run `Bootstrap.ps1` as described in the README. `-PrepareOnly` prepares without compiling. On Linux AMD64 use `bash build/build.sh`. The bootstrap downloads pinned toolchains and WebRTC inputs into `~/.cache/mautrix-whatsapp-call/` and checks their integrity. Allow adequate disk space and memory for the toolchains, dependency caches and build output. Do not commit the cache, binaries or `dist/`. `-Jobs` on Windows or `MAUTRIX_WHATSAPP_CALL_BUILD_JOBS` on Linux can reduce parallelism if memory is limited.

Do not skip release tests. The Go release build disables Git/VCS stamping and performs Go operations in a temporary source copy so the checked-in source checksums remain unchanged (the fixes present in the initial public release). If an older checkout fails with `error obtaining VCS status: exit status 128` or changes tracked Go module files, update to the corrected release commit; do not disable source verification or blindly recalculate its hashes. Check `git status` before and after building. Review local changes before restoring any files.

The build produces a timestamped Linux AMD64 ZIP and sibling checksum under `dist/`. The CI `source-check` workflow is a source/fixture check, not a full RTC release build or live-media test. Perform the full local build and runtime acceptance before publishing.

## Verify and publish

Select the runtime build you intend to publish. This PowerShell example selects the newest by write time; inspect the printed name and confirm it is the intended tested build.

```powershell
$Release = Get-ChildItem ".\dist\mautrix-whatsapp-call-*.zip" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
if (-not $Release) { throw "Runtime ZIP missing" }
$Checksum = "$($Release.FullName).sha256"
$Expected = ((Get-Content -LiteralPath $Checksum -Raw).Trim() -split "\s+")[0].ToLower()
$Actual = (Get-FileHash -LiteralPath $Release.FullName -Algorithm SHA256).Hash.ToLower()
Write-Host $Release.FullName
if ($Actual -ne $Expected) { throw "Runtime checksum mismatch" }
```

In Linux/WSL, `bash build/verify-package.sh /path/to/runtime.zip` checks the internal manifest and required executable layout. Test that exact verified artifact on a compatible deployment and record the results using the [acceptance checklist](../deployment/mautrix-whatsapp-call/OPERATIONS.md#5-live-acceptance-checklist-for-every-deployment-or-dependency-update).

Publish the two runtime assets together using the GitHub release UI or GitHub CLI. For example, after committing the intended source revision and authenticating `gh`, use a **new** release tag (substitute the real version and tested source commit):

```powershell
$Tag = "v0.1.1"
$Commit = (git rev-parse HEAD).Trim()
gh release create $Tag $Release.FullName $Checksum `
    --repo "BERNARDO31P/mautrix-whatsapp-call" `
    --target $Commit `
    --title "mautrix-whatsapp-call $Tag" `
    --notes "Prebuilt Linux AMD64 runtime. Verify the matching checksum and follow the installation guide."
```

Use the commit that actually produced the runtime, and ensure it exists on the remote. The sample tag is illustrative; do not recreate or overwrite an existing release. Attach the actual build output, not GitHub's automatic source archive. Release notes should mention the architecture, dependency versions, tested clients, changes, migration requirements and known limitations. Verify the public release page and downloadable assets. The stable entry point is <https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/latest>.

Documentation-only changes do not require a new binary build or replacement of existing release assets. Commit/push the Markdown changes; the new guides in `deployment/mautrix-whatsapp-call/` will be packaged automatically by subsequent runtime builds. An existing published ZIP is immutable unless explicitly replaced; its manifest covers the files that were shipped with it.
