@echo off
echo ========================================
echo   Stopping All Distributed File Storage Nodes
echo ========================================
echo.

echo Stopping all node.exe processes...
taskkill /F /IM node.exe 2>nul
if %ERRORLEVEL% EQU 0 (
    echo [OK] All node.exe processes stopped
) else (
    echo [INFO] No node.exe processes were running
)

echo.
echo Stopping Process Manager...
taskkill /F /IM manager.exe 2>nul
if %ERRORLEVEL% EQU 0 (
    echo [OK] Process Manager stopped
) else (
    echo [INFO] Process Manager was not running
)

echo.
echo Stopping React frontend...
taskkill /F /FI "WINDOWTITLE eq npm*" 2>nul
taskkill /F /FI "WINDOWTITLE eq *node*" 2>nul
taskkill /F /FI "WINDOWTITLE eq cmd*" 2>nul
pkill -f "npm start" 2>nul
pkill -f "node.exe" 2>nul
if %ERRORLEVEL% EQU 0 (
    echo [OK] React frontend stopped
) else (
    echo [INFO] React frontend was not running
)

echo.
echo Forcing cleanup of any remaining processes...
taskkill /F /FI "IMAGENAME eq node.exe" 2>nul
taskkill /F /FI "IMAGENAME eq manager.exe" 2>nul

echo.
echo ========================================
echo   System Shutdown Complete
echo ========================================
echo.
echo All nodes and services have been stopped.
echo This script stops both single-node and multi-node setups.
echo.
pause
