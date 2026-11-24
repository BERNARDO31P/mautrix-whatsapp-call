[CmdletBinding()]
param(
    [string]$DistroName = "",
    [string]$OutputRoot = "",
    [ValidateRange(0,256)][int]$Jobs = 4,
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Parameters = @{
    DistroName = $DistroName
    OutputRoot = $OutputRoot
    Jobs = $Jobs
}
if ($PrepareOnly) { $Parameters.PrepareOnly = $true }
& (Join-Path $PSScriptRoot "build\Build.ps1") @Parameters
exit $LASTEXITCODE
