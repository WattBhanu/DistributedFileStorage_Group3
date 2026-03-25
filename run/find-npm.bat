@echo off
setlocal EnableDelayedExpansion

REM This script finds npm even if it's not in PATH
set "NPM_FOUND="

REM Check if npm is in PATH first
where npm >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    set "NPM_FOUND=npm"
    goto :found
)

REM Try common Node.js installation locations
if exist "C:\Program Files\nodejs\npm.cmd" (
    set "NPM_FOUND=C:\Program Files\nodejs\npm.cmd"
    goto :found
)

if exist "C:\Program Files (x86)\nodejs\npm.cmd" (
    set "NPM_FOUND=C:\Program Files (x86)\nodejs\npm.cmd"
    goto :found
)

if exist "%APPDATA%\npm\npm.cmd" (
    set "NPM_FOUND=%APPDATA%\npm\npm.cmd"
    goto :found
)

if exist "%LOCALAPPDATA%\Microsoft\WindowsApps\npm.cmd" (
    set "NPM_FOUND=%LOCALAPPDATA%\Microsoft\WindowsApps\npm.cmd"
    goto :found
)

REM If we get here, npm was not found
echo X Error: npm is not found in PATH or common installation locations
echo.
echo Please install Node.js from: https://nodejs.org/
echo.
echo After installing Node.js, please run this script again.
exit /b 1

:found
echo [OK] Found npm at: %NPM_FOUND%
endlocal & set "NPM_CMD=%NPM_FOUND%"
goto :eof
