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
echo Stopping React frontend...
taskkill /F /FI "WINDOWTITLE eq npm*" 2>nul
taskkill /F /FI "WINDOWTITLE eq *node*" 2>nul
if %ERRORLEVEL% EQU 0 (
    echo [OK] React frontend stopped
) else (
    echo [INFO] React frontend was not running
)

echo.
echo ========================================
echo   System Shutdown Complete
echo ========================================
echo.
echo All nodes and services have been stopped.
echo This script stops both single-node and multi-node setups.
echo.
pause
