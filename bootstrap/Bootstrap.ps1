[CmdletBinding()]
param(
    [string]$BuilderRoot = (Join-Path ([Environment]::GetFolderPath("MyDocuments")) "mautrix-whatsapp-call-builder"),
    [string]$DistroName = "mautrix-whatsapp-call-Ubuntu-22.04",
    [int]$Jobs = 0,
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$RepoRoot = Split-Path -Parent $PSScriptRoot

function Test-Administrator {
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $Principal = New-Object Security.Principal.WindowsPrincipal($Identity)
    return $Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Download-File([string]$Uri, [string]$OutFile) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutFile) | Out-Null
    if (Test-Path -LiteralPath $OutFile -PathType Leaf) { return }
    $Curl = Get-Command "curl.exe" -ErrorAction SilentlyContinue
    if ($Curl) {
        & $Curl.Source --fail --location --retry 8 --retry-all-errors --retry-delay 3 --continue-at - --output $OutFile $Uri
        if ($LASTEXITCODE -eq 0) { return }
        Remove-Item -LiteralPath $OutFile -Force -ErrorAction SilentlyContinue
    }
    Invoke-WebRequest -UseBasicParsing -Uri $Uri -OutFile $OutFile
}

if (-not (Test-Administrator)) {
    if (-not $PSCommandPath) { throw "Run this script from its .ps1 file so it can elevate itself." }
    $Arguments = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$PSCommandPath`"",
        "-BuilderRoot", "`"$BuilderRoot`"", "-DistroName", "`"$DistroName`"", "-Jobs", "$Jobs"
    )
    if ($PrepareOnly) { $Arguments += "-PrepareOnly" }
    $Process = Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList ($Arguments -join " ") -Wait -PassThru
    exit $Process.ExitCode
}

if (-not [Environment]::Is64BitOperatingSystem) { throw "mautrix-whatsapp-call requires 64-bit Windows." }
New-Item -ItemType Directory -Force -Path $BuilderRoot | Out-Null

$FeatureNames = @("Microsoft-Windows-Subsystem-Linux", "VirtualMachinePlatform")
$RebootRequired = $false
foreach ($FeatureName in $FeatureNames) {
    $Feature = Get-WindowsOptionalFeature -Online -FeatureName $FeatureName
    if ($Feature.State -ne "Enabled") {
        $Result = Enable-WindowsOptionalFeature -Online -FeatureName $FeatureName -All -NoRestart
        if ($Result.RestartNeeded) { $RebootRequired = $true }
    }
}
if ($RebootRequired) {
    Write-Host "Windows must reboot before WSL2 can be used. After reboot, run the same command again."
    exit 3010
}

if (-not (Get-Command "wsl.exe" -ErrorAction SilentlyContinue)) { throw "wsl.exe is unavailable." }
& wsl.exe --update --web-download
if ($LASTEXITCODE -ne 0) { & wsl.exe --update }
if ($LASTEXITCODE -ne 0) { throw "WSL update failed." }
& wsl.exe --set-default-version 2
if ($LASTEXITCODE -ne 0) { throw "Could not select WSL2 as the default version." }

$Distros = @(
    & wsl.exe --list --quiet |
        ForEach-Object { $_.Trim([char]0).Trim() } |
        Where-Object { $_ }
)

if (-not ($Distros -contains $DistroName)) {
    $WslFiles = Join-Path $BuilderRoot "wsl"
    $DistroInstall = Join-Path $WslFiles $DistroName
    $Rootfs = Join-Path $WslFiles "ubuntu-jammy-wsl-amd64-wsl.rootfs.tar.gz"
    $Sums = Join-Path $WslFiles "SHA256SUMS"
    New-Item -ItemType Directory -Force -Path $WslFiles | Out-Null
    $Base = "https://cloud-images.ubuntu.com/wsl/releases/jammy/current"
    if (Test-Path -LiteralPath $Sums) { Remove-Item -LiteralPath $Sums -Force }
    Download-File "$Base/SHA256SUMS" $Sums
    $Expected = $null
    foreach ($Line in Get-Content -LiteralPath $Sums) {
        if ($Line -match "^([0-9a-fA-F]{64})\s+\*?ubuntu-jammy-wsl-amd64-wsl\.rootfs\.tar\.gz$") {
            $Expected = $Matches[1].ToLowerInvariant(); break
        }
    }
    if (-not $Expected) { throw "Could not parse the Ubuntu Jammy WSL rootfs checksum." }
    if ((Test-Path -LiteralPath $Rootfs) -and ((Get-Sha256 $Rootfs) -ne $Expected)) { Remove-Item -LiteralPath $Rootfs -Force }
    Download-File "$Base/ubuntu-jammy-wsl-amd64-wsl.rootfs.tar.gz" $Rootfs
    if ((Get-Sha256 $Rootfs) -ne $Expected) { throw "Ubuntu WSL rootfs checksum mismatch." }
    if (Test-Path -LiteralPath $DistroInstall) { throw "Target distro directory already exists but is not registered: $DistroInstall" }
    New-Item -ItemType Directory -Force -Path $DistroInstall | Out-Null
    & wsl.exe --import $DistroName $DistroInstall $Rootfs --version 2
    if ($LASTEXITCODE -ne 0) { throw "Could not import Ubuntu 22.04 as $DistroName" }
}

$OutputRoot = Join-Path $BuilderRoot "dist"
$BuildScript = Join-Path $RepoRoot "build\Build.ps1"
$BuildArgs = @("-DistroName", $DistroName, "-OutputRoot", $OutputRoot, "-Jobs", $Jobs)
if ($PrepareOnly) { $BuildArgs += "-PrepareOnly" }
& $BuildScript @BuildArgs
exit 0
