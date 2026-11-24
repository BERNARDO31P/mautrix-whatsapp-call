[CmdletBinding()]
param(
    [string]$BuilderRoot = (Join-Path $HOME "mautrix-whatsapp-call-builder"),
    [string]$DistroName = "mautrix-whatsapp-call-Ubuntu-22.04",
    [ValidateRange(0,256)][int]$Jobs = 4,
    [switch]$PrepareOnly
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Parameters = @{ BuilderRoot=$BuilderRoot; DistroName=$DistroName; Jobs=$Jobs }
if ($PrepareOnly) { $Parameters.PrepareOnly=$true }
& (Join-Path $PSScriptRoot "bootstrap\Bootstrap.ps1") @Parameters
exit $LASTEXITCODE
