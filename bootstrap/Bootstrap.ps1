[CmdletBinding()]
param(
    [string]$BuilderRoot = (Join-Path $HOME "mautrix-whatsapp-call-builder"),
    [ValidatePattern("^[A-Za-z0-9][A-Za-z0-9._-]*$")][string]$DistroName = "mautrix-whatsapp-call-Ubuntu-22.04",
    [ValidateRange(0,256)][int]$Jobs = 4,
    [switch]$PrepareOnly
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$RepoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot "Windows.Common.ps1")

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Download-File([string]$Uri, [string]$OutFile) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutFile) | Out-Null
    if (Test-Path -LiteralPath $OutFile -PathType Leaf) { return }
    $Partial = "$OutFile.partial"
    Write-Host "Downloading: $Uri"
    $Curl = Get-Command "curl.exe" -ErrorAction SilentlyContinue
    if ($Curl) {
        & $Curl.Source --fail --location --retry 5 --retry-delay 3 --continue-at - --output $Partial $Uri
        if ($LASTEXITCODE -eq 0) {
            Move-Item -LiteralPath $Partial -Destination $OutFile -Force
            return
        }
        Remove-Item -LiteralPath $Partial -Force -ErrorAction SilentlyContinue
    }
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -UseBasicParsing -Uri $Uri -OutFile $Partial
    Move-Item -LiteralPath $Partial -Destination $OutFile -Force
}

try {
    Write-Host "mautrix-whatsapp-call Windows bootstrap"
    Write-Host "Repository: $RepoRoot"
    Write-Host "Builder: $BuilderRoot"
    Write-Host "Distribution: $DistroName"
    Assert-WindowsBuilder
    $BuilderRoot = [IO.Path]::GetFullPath($BuilderRoot)
    New-Item -ItemType Directory -Force -Path $BuilderRoot | Out-Null
    $OutputRoot = Join-Path $BuilderRoot "dist"
    $BuildScript = Join-Path $RepoRoot "build\Build.ps1"
    $Distros = @(Get-WslDistributions)

    Write-Host "[1/4] Checking Windows WSL2 prerequisites"
    $WorkingDistro = $false
    if (($Distros -contains $DistroName) -and (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
        & wsl.exe -d $DistroName --user root --exec test -r /proc/sys/kernel/osrelease
        $WorkingDistro = ($LASTEXITCODE -eq 0)
    }
    if ($WorkingDistro) {
        Write-Host "[2/4] Reusing registered distribution: $DistroName (no elevation needed)"
    } else {
        Write-Host "[2/4] Preparing Windows WSL2 runtime (administrator approval may be required)"
        $SetupCode = Invoke-WslSystemSetup -ScriptPath (Join-Path $PSScriptRoot "Enable-Wsl.ps1")
        if ($SetupCode -eq 3010 -or $SetupCode -eq 1641) {
            Write-Host "REBOOT REQUIRED (3010). Restart Windows and rerun the identical bootstrap command."
            exit 3010
        }
        if ($SetupCode -ne 0) { throw "Windows WSL system setup failed (exit $SetupCode). See the setup console." }
    }
    Assert-WindowsBuilder -RequireWsl
    # WSL distributions belong to the original Windows user, never the
    # administrator account that may have approved the system setup.
    $Distros = @(Get-WslDistributions)

    Write-Host "[3/4] Preparing Ubuntu build distribution"
    if (-not ($Distros -contains $DistroName)) {
        $WslFiles = Join-Path $BuilderRoot "wsl"
        $DistroInstall = Join-Path $WslFiles $DistroName
        $RootfsName = "ubuntu-jammy-wsl-amd64-wsl.rootfs.tar.gz"
        $Rootfs = Join-Path $WslFiles $RootfsName
        $Sums = Join-Path $WslFiles "SHA256SUMS"
        New-Item -ItemType Directory -Force -Path $WslFiles | Out-Null
        $Base = "https://cloud-images.ubuntu.com/wsl/releases/jammy/current"
        if (Test-Path -LiteralPath $Sums) { Remove-Item -LiteralPath $Sums -Force }
        Remove-Item -LiteralPath "$Sums.partial" -Force -ErrorAction SilentlyContinue
        Download-File "$Base/SHA256SUMS" $Sums
        $Expected = $null
        foreach ($Line in Get-Content -LiteralPath $Sums) {
            if ($Line -match "^([0-9a-fA-F]{64})\s+\*?ubuntu-jammy-wsl-amd64-wsl\.rootfs\.tar\.gz$") {
                $Expected = $Matches[1].ToLowerInvariant(); break
            }
        }
        if (-not $Expected) { throw "Could not parse the Ubuntu Jammy WSL rootfs checksum." }
        if ((Test-Path -LiteralPath $Rootfs) -and ((Get-Sha256 $Rootfs) -ne $Expected)) { Remove-Item -LiteralPath $Rootfs -Force }
        Download-File "$Base/$RootfsName" $Rootfs
        if ((Get-Sha256 $Rootfs) -ne $Expected) {
            Remove-Item -LiteralPath $Rootfs -Force
            throw "Ubuntu WSL rootfs checksum mismatch. Rerun to download a clean copy."
        }
        Write-Host "Ubuntu rootfs SHA256: PASS"
        if ((Test-Path -LiteralPath $DistroInstall) -and @(Get-ChildItem -LiteralPath $DistroInstall -Force).Count -gt 0) {
            throw "Unregistered distro directory is not empty: $DistroInstall. Preserve it and inspect it before retrying; no data has been deleted."
        }
        New-Item -ItemType Directory -Force -Path $DistroInstall | Out-Null
        Invoke-WslChecked -Arguments @("--import", $DistroName, $DistroInstall, $Rootfs, "--version", "2")
    }
    Write-Host "[4/4] Starting dependency preparation and build"
    $BuildArgs = @{ DistroName = $DistroName; OutputRoot = $OutputRoot; Jobs = $Jobs; PrepareOnly = [bool]$PrepareOnly }
    & $BuildScript @BuildArgs
    $Code = $LASTEXITCODE
    if ($Code -ne 0) { exit $Code }
    if ($PrepareOnly) { Write-Host "BOOTSTRAP PREPARATION: PASS" } else { Write-Host "BOOTSTRAP BUILD: PASS ($OutputRoot)" }
    exit 0
} catch {
    Write-Host "BOOTSTRAP FAILED: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
