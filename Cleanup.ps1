[CmdletBinding()]
param(
    [string]$DistroName = "",
    [ValidateSet("keep","debug","all")][string]$CompilerCache = "debug",
    [switch]$Apply
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot "bootstrap\Windows.Common.ps1")
try {
    Assert-WindowsBuilder -RequireWsl
    $Distros = @(Get-WslDistributions)
    if (-not $DistroName) {
        foreach ($Candidate in @("mautrix-whatsapp-call-Ubuntu-22.04", "Ubuntu-22.04")) {
            if ($Distros -contains $Candidate) { $DistroName = $Candidate; break }
        }
    }
    if (-not $DistroName -or -not ($Distros -contains $DistroName)) { throw "Select the registered build distribution using -DistroName." }
    $Script = Join-Path $PSScriptRoot "build\cleanup.py"
    $Result = @(& wsl.exe -d $DistroName --user root --exec wslpath -a -u $Script)
    if ($LASTEXITCODE -ne 0 -or $Result.Count -ne 1) { throw "Could not translate cleanup script path." }
    $WslPath = $Result[0].Replace([string][char]0, "").Trim()
    $Arguments = @("-d", $DistroName, "--user", "root", "--exec", "python3", "-B", $WslPath, "--cache", "/root/.cache/mautrix-whatsapp-call", "--compiler-cache", $CompilerCache)
    if ($Apply) { $Arguments += "--apply" }
    & wsl.exe @Arguments
    exit $LASTEXITCODE
} catch {
    Write-Error $_.Exception.Message
    exit 1
}
