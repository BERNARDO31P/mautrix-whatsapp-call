# Building and publishing a runtime release

Most operators should download a prebuilt runtime and follow the [installation guide](../deployment/mautrix-whatsapp-call/INSTALLATION.md). This page is for maintainers. See [BUILD.md](BUILD.md) for toolchain pins and [UPDATING.md](UPDATING.md) for source/dependency changes.

## Prepare and build

Clone the repository:

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

Release builds run the tests and use a verified source snapshot. Check `git status` before building.

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

In Linux/WSL, `bash build/verify-package.sh /path/to/runtime.zip` checks the exact allowed file inventory, complete internal manifest, matching binary copies, the mandatory format-2 build record, AMD64 ELF headers, executable modes and ZIP paths. The runtime packager never copies generated state or unknown local files into the archive. The source packager requires a clean committed tree. The build record binds the release binaries and source versions to the source snapshot and all required successful test/build log hashes. Before publication the builder rechecks the original checkout, snapshot, binaries and retained logs; the ZIP includes the record, not the potentially sensitive logs. Keep the private build directory for later comparison. Both checksums and the record are local integrity evidence, not independent attestation or authentication of an untrusted publisher. Older runtime packages without this record require the matching older verifier or a full rebuild. Test that exact verified artifact on a compatible deployment and record the results using the [acceptance checklist](../deployment/mautrix-whatsapp-call/OPERATIONS.md#5-live-acceptance-checklist-for-every-deployment-or-dependency-update).

Publish the two runtime assets together using the GitHub release UI or GitHub CLI. For example, after committing the intended source revision and authenticating `gh`, use a **new** release tag (substitute the real version and tested source commit):

```powershell
$Tag = "vX.Y.Z"
$Commit = (git rev-parse HEAD).Trim()
gh release create $Tag $Release.FullName $Checksum `
    --repo "BERNARDO31P/mautrix-whatsapp-call" `
    --target $Commit `
    --title "mautrix-whatsapp-call $Tag" `
    --notes "Prebuilt Linux AMD64 runtime. Verify the matching checksum and follow the installation guide."
```

Use the commit that actually produced the runtime, and ensure it exists on the remote. The sample tag is illustrative; do not recreate or overwrite an existing release. Attach the actual build output, not GitHub's automatic source archive. Release notes should mention the architecture, dependency versions, tested clients, changes, migration requirements and known limitations. Verify the public release page and downloadable assets. The stable entry point is <https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/latest>.

Changes to documentation outside the build input manifest do not require recompilation. Deployment documentation is packaged and included in the build input digest. Keep published archives and checksums unchanged.
