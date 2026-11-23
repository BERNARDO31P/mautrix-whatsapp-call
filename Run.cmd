@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Bootstrap.ps1" %*
exit /b %ERRORLEVEL%
