[CmdletBinding()]
param(
    [string]$DistroName = "",
    [string]$OutputRoot = "",
    [int]$Jobs = 0,
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$RepoRoot = Split-Path -Parent $PSScriptRoot

function Convert-ToWslPath([string]$Path) {
    $Full = [System.IO.Path]::GetFullPath($Path)
    if ($Full -match "^([A-Za-z]):\\(.*)$") {
        $Drive = $Matches[1].ToLowerInvariant()
        $Rest = $Matches[2] -replace "\\", "/"
        return "/mnt/$Drive/$Rest"
    }
    throw "Unsupported Windows path for WSL conversion: $Full"
}

if (-not (Get-Command "wsl.exe" -ErrorAction SilentlyContinue)) {
    throw "WSL is not installed. Run .\Bootstrap.ps1 first."
}

$Distros = @(
    & wsl.exe --list --quiet |
        ForEach-Object { $_.Trim([char]0).Trim() } |
        Where-Object { $_ }
)
if ($Distros.Count -eq 0) {
    throw "No WSL distribution is installed. Run .\Bootstrap.ps1 first."
}
if (-not $DistroName) {
    $DistroName = $Distros[0]
}
if (-not ($Distros -contains $DistroName)) {
    throw "WSL distribution not found: $DistroName"
}
if (-not $OutputRoot) {
    $OutputRoot = Join-Path $RepoRoot "dist"
}
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$RepoWsl = Convert-ToWslPath $RepoRoot
$OutputWsl = Convert-ToWslPath $OutputRoot
$PrepareWsl = "$RepoWsl/bootstrap/prepare-build-inputs.sh"
$BuildWsl = "$RepoWsl/build/build.sh"
$CacheWsl = "/root/.cache/mautrix-whatsapp-call"

Write-Host "Project: mautrix-whatsapp-call"
Write-Host "WSL distro: $DistroName"
Write-Host "Repository: $RepoRoot"
Write-Host "Output: $OutputRoot"

if ($PrepareOnly) {
    & wsl.exe -d $DistroName --user root -- bash $PrepareWsl $CacheWsl
} else {
    $EnvArgs = @(
        "MAUTRIX_WHATSAPP_CALL_CACHE=$CacheWsl",
        "MAUTRIX_WHATSAPP_CALL_DIST=$OutputWsl"
    )
    if ($Jobs -gt 0) {
        $EnvArgs += "MAUTRIX_WHATSAPP_CALL_BUILD_JOBS=$Jobs"
    }
    & wsl.exe -d $DistroName --user root -- env @EnvArgs bash $BuildWsl
}
if ($LASTEXITCODE -ne 0) {
    throw "mautrix-whatsapp-call WSL build failed with exit code $LASTEXITCODE"
}
exit 0
