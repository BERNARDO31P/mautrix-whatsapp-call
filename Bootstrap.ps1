[CmdletBinding()]
param(
    [string]$BuilderRoot = (Join-Path ([Environment]::GetFolderPath("MyDocuments")) "mautrix-whatsapp-call-builder"),
    [string]$DistroName = "mautrix-whatsapp-call-Ubuntu-22.04",
    [int]$Jobs = 0,
    [switch]$PrepareOnly
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Parameters = @{ BuilderRoot=$BuilderRoot; DistroName=$DistroName; Jobs=$Jobs }
if ($PrepareOnly) { $Parameters.PrepareOnly=$true }
& (Join-Path $PSScriptRoot "bootstrap\Bootstrap.ps1") @Parameters
