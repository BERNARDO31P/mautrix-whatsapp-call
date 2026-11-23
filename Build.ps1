[CmdletBinding()]
param(
    [string]$DistroName = "",
    [string]$OutputRoot = "",
    [int]$Jobs = 0,
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
