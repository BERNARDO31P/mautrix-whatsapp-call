[CmdletBinding()]
param([switch]$KeepOpenOnFailure)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

try {
    Write-Host "mautrix-whatsapp-call: Windows WSL2 system setup"
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $Principal = New-Object Security.Principal.WindowsPrincipal($Identity)
    if (-not $Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Windows WSL system setup requires administrator privileges."
    }
    $RebootRequired = $false
    foreach ($FeatureName in @("Microsoft-Windows-Subsystem-Linux", "VirtualMachinePlatform")) {
        $Feature = Get-WindowsOptionalFeature -Online -FeatureName $FeatureName
        if ($Feature.State -eq "EnablePending" -or $Feature.State -eq "DisablePending") {
            $RebootRequired = $true
        } elseif ($Feature.State -ne "Enabled") {
            Write-Host "Enabling $FeatureName"
            $Result = Enable-WindowsOptionalFeature -Online -FeatureName $FeatureName -All -NoRestart
            if ($Result.RestartNeeded -eq $true -or [string]$Result.RestartNeeded -eq "Yes") { $RebootRequired = $true }
        }
    }
    if ($RebootRequired) {
        Write-Host "Windows features require a reboot."
        exit 3010
    }
    if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
        throw "wsl.exe is unavailable. Install Windows updates and enable WSL2 first."
    }
    & wsl.exe --version
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Installing WSL without a distribution"
        & wsl.exe --install --no-distribution --web-download
        $Code = $LASTEXITCODE
        if ($Code -eq 3010 -or $Code -eq 1641) { exit 3010 }
        if ($Code -ne 0) { throw "WSL installation failed (exit $Code)." }
    }
    # Do not set a per-user WSL default or register a distro here. The parent
    # explicitly imports WSL2 under the original Windows user after this step.
    & wsl.exe --version
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Updating WSL runtime"
        & wsl.exe --update --web-download
        $Code = $LASTEXITCODE
        if ($Code -eq 3010 -or $Code -eq 1641) { exit 3010 }
        if ($Code -ne 0) {
            & wsl.exe --update
            $Code = $LASTEXITCODE
            if ($Code -eq 3010 -or $Code -eq 1641) { exit 3010 }
            if ($Code -ne 0) { throw "WSL update failed (exit $Code)." }
        }
        & wsl.exe --version
        if ($LASTEXITCODE -ne 0) { throw "WSL runtime still unavailable after installation/update. Restart Windows if required and retry." }
    }
    Write-Host "Windows WSL2 system setup: PASS"
    exit 0
} catch {
    Write-Host "WSL SYSTEM SETUP FAILED: $($_.Exception.Message)" -ForegroundColor Red
    if ($KeepOpenOnFailure) { [void](Read-Host "Press Enter to return to the original console") }
    exit 1
}
