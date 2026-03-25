@echo off
setlocal EnableDelayedExpansion

echo ========================================
echo   Distributed File Storage - Multi-Node Cluster
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

REM Build the process manager
echo Building Process Manager...
go build -o manager.exe .\cmd\manager
if %ERRORLEVEL% NEQ 0 (
    echo X Manager build failed!
    exit /b 1
)
echo [OK] Manager built successfully!
echo.

REM Create data directories
echo Creating data directories...
if not exist "data-node1" mkdir data-node1
if not exist "data-node2" mkdir data-node2
if not exist "data-node3" mkdir data-node3
echo [OK] Directories ready
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
start "React Frontend" %NPM_CMD% start
cd /D "%WORKSPACE_DIR%"

echo Waiting for React to start (5 seconds)...
timeout /t 5 /nobreak >nul
echo.

REM Get absolute path to workspace
set "WORKSPACE_DIR=%~dp0.."

echo Press Ctrl+C to stop all nodes
echo.
echo Starting Process Manager for automatic recovery...
echo.

REM Start Process Manager (which will start and monitor all nodes)
cd /D "%WORKSPACE_DIR%"
start /B cmd /c "manager.exe"
timeout /t 3 /nobreak >nul

REM Keep script running until interrupted
echo.
echo ========================================
echo   Cluster Running with Auto-Recovery!
echo ========================================
echo.
echo LEADER:    node1 (port 8080)
echo FOLLOWER 1: node2 (port 8081)
echo FOLLOWER 2: node3 (port 8082)
echo.
echo Frontend: http://localhost:3000
echo Monitor:  http://localhost:3000/monitor
echo.
echo Files uploaded to leader will auto-replicate to followers
echo.
echo AUTOMATIC RECOVERY ENABLED:
echo - Dead nodes will auto-restart within 30 seconds
echo - Max 3 restart attempts per node
echo - Health checks every 5 seconds
echo.

