[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
foreach ($Script in Get-ChildItem -Path $Root -Recurse -Filter "*.ps1" -File) {
    $Tokens = $null
    $ParseErrors = $null
    [void][System.Management.Automation.Language.Parser]::ParseFile($Script.FullName, [ref]$Tokens, [ref]$ParseErrors)
    if ($ParseErrors.Count -gt 0) { throw "PowerShell syntax errors in $($Script.FullName): $ParseErrors" }
}
$Temp = Join-Path ([IO.Path]::GetTempPath()) ("matrixrtc wrapper test " + [guid]::NewGuid())
New-Item -ItemType Directory -Path $Temp | Out-Null
try {
    Copy-Item -LiteralPath (Join-Path $Root "Bootstrap.ps1") -Destination $Temp
    Copy-Item -LiteralPath (Join-Path $Root "Build.ps1") -Destination $Temp
    foreach ($Folder in @("bootstrap", "build")) { New-Item -ItemType Directory -Path (Join-Path $Temp $Folder) | Out-Null }
    $StubBootstrap = @"
param([string]`$BuilderRoot,[string]`$DistroName,[int]`$Jobs,[switch]`$PrepareOnly)
@{BuilderRoot=`$BuilderRoot;DistroName=`$DistroName;Jobs=`$Jobs;PrepareOnly=[bool]`$PrepareOnly} | ConvertTo-Json | Set-Content -LiteralPath `"$Temp\result.json`"
exit 3010
"@
    $StubBuild = @"
param([string]`$OutputRoot,[string]`$DistroName,[int]`$Jobs,[switch]`$PrepareOnly)
@{OutputRoot=`$OutputRoot;DistroName=`$DistroName;Jobs=`$Jobs;PrepareOnly=[bool]`$PrepareOnly} | ConvertTo-Json | Set-Content -LiteralPath `"$Temp\result.json`"
exit 17
"@
    Set-Content -LiteralPath (Join-Path $Temp "bootstrap\Bootstrap.ps1") -Value $StubBootstrap -Encoding UTF8
    Set-Content -LiteralPath (Join-Path $Temp "build\Build.ps1") -Value $StubBuild -Encoding UTF8
    $PowerShell = Join-Path $PSHOME "powershell.exe"
    $Target = Join-Path $Temp "builder with spaces"
    & $PowerShell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Temp "Bootstrap.ps1") -BuilderRoot $Target -DistroName "test-distro" -Jobs 2 -PrepareOnly
    if ($LASTEXITCODE -ne 3010) { throw "Bootstrap did not propagate reboot exit code" }
    $Result = Get-Content -Raw -LiteralPath (Join-Path $Temp "result.json") | ConvertFrom-Json
    if ($Result.BuilderRoot -ne $Target -or $Result.DistroName -ne "test-distro" -or $Result.Jobs -ne 2 -or -not $Result.PrepareOnly) { throw "Bootstrap parameter forwarding failed" }
    & $PowerShell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Temp "Build.ps1") -OutputRoot $Target -DistroName "test-distro" -Jobs 3
    if ($LASTEXITCODE -ne 17) { throw "Build did not propagate failure exit code" }
    $Result = Get-Content -Raw -LiteralPath (Join-Path $Temp "result.json") | ConvertFrom-Json
    if ($Result.OutputRoot -ne $Target -or $Result.DistroName -ne "test-distro" -or $Result.Jobs -ne 3 -or $Result.PrepareOnly) { throw "Build parameter forwarding failed" }
    # Exercise the WSL command adapter with a fake native executable function.
    . (Join-Path $Root "bootstrap\Windows.Common.ps1")
    function wsl.exe { $global:LASTEXITCODE = 19 }
    $Failed = $false
    try { Invoke-WslChecked -Arguments @("--status") } catch { $Failed = $_.Exception.Message -match "exit 19" }
    if (-not $Failed) { throw "WSL command adapter hid a native failure" }
    # Exercise elevation without showing UAC or touching Windows features.
    $Setup = Join-Path $Root "bootstrap\Enable-Wsl.ps1"
    function Test-Administrator { return $false }
    function Start-Process {
        [CmdletBinding()]
        param([string]$FilePath,[string[]]$ArgumentList,[string]$WorkingDirectory,[string]$Verb,[switch]$Wait,[switch]$PassThru)
        if ($Verb -ne "RunAs" -or -not $Wait -or -not $PassThru) { throw "Elevation did not wait for a real exit code" }
        if ($ArgumentList.Count -ne 5 -or $ArgumentList[3] -ne "-EncodedCommand") { throw "Unexpected setup arguments" }
        $Decoded = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($ArgumentList[4]))
        $Source = Get-Content -LiteralPath $Setup -Raw
        if ($Decoded -ne ("& {`r`n" + $Source + "`r`n} -KeepOpenOnFailure")) { throw "Elevation did not preserve the setup script" }
        return [pscustomobject]@{ExitCode=3010}
    }
    if ((Invoke-WslSystemSetup -ScriptPath $Setup) -ne 3010) { throw "Elevated setup lost reboot exit code" }
    function Start-Process { throw "fixture UAC cancellation" }
    $Cancelled = $false
    try { Invoke-WslSystemSetup -ScriptPath $Setup } catch { $Cancelled = $_.Exception.Message -match "cancelled or could not be requested" }
    if (-not $Cancelled) { throw "UAC cancellation was not surfaced" }
    Remove-Item Function:Start-Process
    function Test-Administrator { return $true }
    $SetupStub = Join-Path $Temp "setup stub.ps1"
    Set-Content -LiteralPath $SetupStub -Value "exit 17" -Encoding UTF8
    if ((Invoke-WslSystemSetup -ScriptPath $SetupStub) -ne 17) { throw "Direct elevated setup lost exit code" }

    # Exercise the actual inner bootstrap without Windows feature access.
    Copy-Item -LiteralPath (Join-Path $Root "bootstrap\Bootstrap.ps1") -Destination (Join-Path $Temp "bootstrap\Bootstrap.ps1") -Force
    $FakeCommon = @"
function Assert-WindowsBuilder { param([switch]`$RequireWsl) }
function Get-WslDistributions { if (`$env:MATRIXRTC_TEST_EXISTING -eq "1") { "test-distro" } }
function wsl.exe { `$global:LASTEXITCODE = 0 }
function Invoke-WslSystemSetup { param([string]`$ScriptPath) if (`$env:MATRIXRTC_TEST_EXISTING -eq "1") { throw "Unexpected elevation for working distro" }; return 3010 }
"@
    Set-Content -LiteralPath (Join-Path $Temp "bootstrap\Windows.Common.ps1") -Value $FakeCommon -Encoding UTF8
    $env:MATRIXRTC_TEST_EXISTING = "0"
    & $PowerShell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Temp "Bootstrap.ps1") -BuilderRoot $Target -DistroName "test-distro" -Jobs 2 -PrepareOnly
    if ($LASTEXITCODE -ne 3010) { throw "Actual bootstrap did not stop on pending reboot" }
    $env:MATRIXRTC_TEST_EXISTING = "1"
    & $PowerShell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Temp "Bootstrap.ps1") -BuilderRoot $Target -DistroName "test-distro" -Jobs 2 -PrepareOnly
    if ($LASTEXITCODE -ne 17) { throw "Actual bootstrap did not reuse distro and propagate build failure" }
    $Result = Get-Content -Raw -LiteralPath (Join-Path $Temp "result.json") | ConvertFrom-Json
    if ($Result.OutputRoot -ne (Join-Path $Target "dist") -or $Result.DistroName -ne "test-distro" -or $Result.Jobs -ne 2 -or -not $Result.PrepareOnly) { throw "Actual bootstrap failed to forward build parameters" }
    Remove-Item Env:MATRIXRTC_TEST_EXISTING
    # Execute the actual build wrapper; simulate only WSL operations.
    Copy-Item -LiteralPath (Join-Path $Root "build\Build.ps1") -Destination (Join-Path $Temp "build\Build.ps1") -Force
    $FakeBuildCommon = @"
function Assert-WindowsBuilder { param([switch]`$RequireWsl) }
function Get-WslDistributions { "test-distro" }
function Invoke-WslChecked { param([string[]]`$Arguments) }
function wsl.exe {
    if (`$args -contains "uname") { "x86_64"; `$global:LASTEXITCODE = 0; return }
    if (`$args -contains "wslpath") { "/fixture/path with spaces"; `$global:LASTEXITCODE = 0; return }
    if (`$args -contains "env") { `$global:LASTEXITCODE = [int]`$env:MATRIXRTC_TEST_BUILD_EXIT; return }
    throw "Unexpected fixture WSL invocation"
}
"@
    Set-Content -LiteralPath (Join-Path $Temp "bootstrap\Windows.Common.ps1") -Value $FakeBuildCommon -Encoding UTF8
    foreach ($Expected in @(0, 20, 21, 26)) {
        $env:MATRIXRTC_TEST_BUILD_EXIT = [string]$Expected
        & $PowerShell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Temp "Build.ps1") -DistroName "test-distro" -OutputRoot $Target
        if ($LASTEXITCODE -ne $Expected) { throw "Actual build wrapper lost native status $Expected; got $LASTEXITCODE" }
    }
    Remove-Item Env:MATRIXRTC_TEST_BUILD_EXIT
    Write-Host "Windows wrapper regression tests: PASS"
} finally {
    Remove-Item -LiteralPath $Temp -Recurse -Force
}
