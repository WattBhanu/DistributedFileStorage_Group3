@echo off
setlocal EnableDelayedExpansion

echo ========================================
echo   Distributed File Storage - Single Node
echo ========================================
echo.



REM Check if Go is installed
where go >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo X Error: Go is not installed. Please install Go first.
    exit /b 1
)

echo [OK] Go detected
go version
echo.

REM Get absolute path to workspace
set "WORKSPACE_DIR=%~dp0.."

REM Build the node from workspace root
echo Building Go node...
cd /D "%WORKSPACE_DIR%"
go build -o node.exe .\cmd\node
if %ERRORLEVEL% NEQ 0 (
    echo X Build failed!
    exit /b 1
)
echo [OK] Build successful!
echo.

REM Install React dependencies (only if not already installed)
echo Checking React dependencies...
cd /D "%WORKSPACE_DIR%\frontend-react"
if not exist "node_modules" (
    echo Installing React dependencies...
    
    REM Find npm location
    call "%WORKSPACE_DIR%\run\find-npm.bat"
    if %ERRORLEVEL% NEQ 0 (
        exit /b 1
    )
    
    echo Running: %NPM_CMD% install
    call %NPM_CMD% install
    if %ERRORLEVEL% NEQ 0 (
        echo X npm install failed!
        exit /b 1
    )
    echo [OK] Dependencies installed
) else (
    echo [OK] Dependencies already present
)
cd /D "%WORKSPACE_DIR%"
echo.

REM Start React app in background
echo Starting React frontend...
cd /D "%WORKSPACE_DIR%\frontend-react"

REM Find npm location if not already set
if "%NPM_CMD%"=="" (
    call "%WORKSPACE_DIR%\run\find-npm.bat"
)
start /B %NPM_CMD% start
cd /D "%WORKSPACE_DIR%"

echo Waiting for React to start (5 seconds)...
timeout /t 5 /nobreak >nul
echo.


REM Get absolute path to workspace
set "WORKSPACE_DIR=%~dp0.."

REM Start Go node in background with correct working directory
cd /D "%WORKSPACE_DIR%"
start /B cmd /c "node.exe -id=node1 -addr=localhost -port=8080 -data=%WORKSPACE_DIR%\data-node1"

REM Keep script running until interrupted
echo.
echo ========================================
echo   System Running!
echo ========================================
echo.
echo Frontend: http://localhost:3000
echo Monitor:  http://localhost:3000/monitor
echo Backend:  http://localhost:8080
echo.
echo Press Ctrl+C to stop all services
echo.

