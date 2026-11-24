# Shared Windows PowerShell 5.1 helpers. No external modules required.
function Get-WslDistributions {
    # WSL registrations are per Windows user. Avoid parsing localized output
    # from wsl --list when there are no registered distributions.
    $Key = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss"
    if (Test-Path -LiteralPath $Key) {
        foreach ($Item in Get-ChildItem -LiteralPath $Key) {
            $Properties = Get-ItemProperty -LiteralPath $Item.PSPath
            if ($Properties.PSObject.Properties["DistributionName"] -and $Properties.DistributionName) {
                $Properties.DistributionName
            }
        }
    }
}

function Invoke-WslChecked {
    param([Parameter(Mandatory=$true)][string[]]$Arguments)
    & wsl.exe @Arguments
    $Code = $LASTEXITCODE
    if ($Code -ne 0) {
        throw "wsl.exe $($Arguments -join " ") failed (exit $Code). See the output above."
    }
}

function Assert-WindowsBuilder {
    param([switch]$RequireWsl)
    if (-not [Environment]::Is64BitProcess -or $env:PROCESSOR_ARCHITECTURE -ne "AMD64") {
        throw "Use 64-bit Windows PowerShell on an x64 Windows PC. The pinned Linux toolchain is AMD64 only."
    }
    if ($RequireWsl -and -not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
        throw "wsl.exe is unavailable. Install Windows updates and enable WSL2 first."
    }
}

function Test-Administrator {
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $Principal = New-Object Security.Principal.WindowsPrincipal($Identity)
    return $Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Invoke-WslSystemSetup {
    param([Parameter(Mandatory=$true)][string]$ScriptPath)
    if (Test-Administrator) {
        # Host output must not become part of the numeric return value.
        & $ScriptPath | Out-Host
        return [int]$LASTEXITCODE
    }

    # Only the OS setup runs elevated. Encode this fixed local helper (no user
    # parameters) so spaces and alternate administrator profile ACLs do not
    # break argument forwarding. The calling user retains the WSL registration.
    $Source = Get-Content -LiteralPath $ScriptPath -Raw -ErrorAction Stop
    $Command = "& {`r`n" + $Source + "`r`n} -KeepOpenOnFailure"
    $Encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($Command))
    $PowerShell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
    Write-Host "Approve the Windows UAC prompt. The setup window will show progress; this console will wait and continue automatically."
    try {
        $Process = Start-Process -FilePath $PowerShell -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-EncodedCommand", $Encoded) -WorkingDirectory (Join-Path $env:SystemRoot "System32") -Verb RunAs -Wait -PassThru -ErrorAction Stop
    } catch {
        throw "Administrator approval was cancelled or could not be requested: $($_.Exception.Message)"
    }
    return [int]$Process.ExitCode
}
