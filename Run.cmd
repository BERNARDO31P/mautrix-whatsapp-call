@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Bootstrap.ps1" %*
set "RC=%ERRORLEVEL%"
if "%RC%"=="3010" echo Restart Windows and rerun the same command.
if not "%RC%"=="0" echo Bootstrap exit code: %RC%
exit /b %RC%
