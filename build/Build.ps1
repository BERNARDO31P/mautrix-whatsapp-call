[CmdletBinding()]
param(
    [string]$DistroName = "",
    [string]$OutputRoot = "",
    [ValidateRange(0,256)][int]$Jobs = 4,
    [switch]$PrepareOnly
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$RepoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $RepoRoot "bootstrap\Windows.Common.ps1")

function Convert-ToWslPath([string]$Path) {
    $Full = [IO.Path]::GetFullPath($Path)
    if ($Full -notmatch "^[A-Za-z]:\\") { throw "Use a local Windows drive path: $Full" }
    $Result = @(& wsl.exe -d $DistroName --user root --exec wslpath -a -u $Full)
    $Code = $LASTEXITCODE
    if ($Code -ne 0 -or $Result.Count -ne 1) { throw "Could not translate Windows path into WSL: $Full (exit $Code)" }
    return $Result[0].Replace([string][char]0, "").Trim()
}

try {
    Write-Host "mautrix-whatsapp-call Windows build"
    Assert-WindowsBuilder -RequireWsl
    $Distros = @(Get-WslDistributions)
    if (-not $DistroName) {
        foreach ($Candidate in @("mautrix-whatsapp-call-Ubuntu-22.04", "Ubuntu-22.04")) {
            if ($Distros -contains $Candidate) { $DistroName = $Candidate; break }
        }
    }
    if (-not $DistroName -or -not ($Distros -contains $DistroName)) {
        throw "No selected Ubuntu WSL distribution found. Run .\Bootstrap.ps1 or specify -DistroName. Registered: $($Distros -join ", ")"
    }
    if (-not $OutputRoot) { $OutputRoot = Join-Path $RepoRoot "dist" }
    $OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
    Write-Host "WSL distro: $DistroName"
    Write-Host "Repository: $RepoRoot"
    Write-Host "Output: $OutputRoot"
    Write-Host "Build jobs: $Jobs (0 = automatic)"
    $Arch = @(& wsl.exe -d $DistroName --user root --exec uname -m)
    if ($LASTEXITCODE -ne 0 -or ($Arch -join "").Replace([string][char]0, "").Trim() -ne "x86_64") { throw "The selected WSL distribution must start successfully and use x86_64 Linux." }
    Invoke-WslChecked -Arguments @("-d", $DistroName, "--user", "root", "--exec", "test", "-x", "/usr/bin/apt-get")
    Invoke-WslChecked -Arguments @("-d", $DistroName, "--user", "root", "--exec", "grep", "-qi", "wsl2", "/proc/sys/kernel/osrelease")
    $RepoWsl = Convert-ToWslPath $RepoRoot
    $OutputWsl = Convert-ToWslPath $OutputRoot
    $PrepareWsl = "$RepoWsl/bootstrap/prepare-build-inputs.sh"
    $BuildWsl = "$RepoWsl/build/build.sh"
    $CacheWsl = "/root/.cache/mautrix-whatsapp-call"
    Invoke-WslChecked -Arguments @("-d", $DistroName, "--user", "root", "--exec", "test", "-r", $BuildWsl)
    if ($PrepareOnly) {
        Invoke-WslChecked -Arguments @("-d", $DistroName, "--user", "root", "--exec", "bash", $PrepareWsl, $CacheWsl)
    } else {
        $WslArgs = @("-d", $DistroName, "--user", "root", "--exec", "env", "MAUTRIX_WHATSAPP_CALL_CACHE=$CacheWsl", "MAUTRIX_WHATSAPP_CALL_DIST=$OutputWsl")
        if ($Jobs -gt 0) { $WslArgs += "MAUTRIX_WHATSAPP_CALL_BUILD_JOBS=$Jobs" }
        $WslArgs += @("bash", $BuildWsl)
        # Preserve the native build status (including incomplete export 21).
        & wsl.exe @WslArgs
        $BuildExit = [int]$LASTEXITCODE
        if ($BuildExit -ne 0) {
            Write-Host "WINDOWS BUILD FAILED: native build exited $BuildExit. See the output above." -ForegroundColor Red
            exit $BuildExit
        }
    }
    if ($PrepareOnly) { Write-Host "WINDOWS PREPARATION: PASS" } else { Write-Host "WINDOWS BUILD: PASS" }
    exit 0
} catch {
    Write-Host "WINDOWS BUILD FAILED: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
