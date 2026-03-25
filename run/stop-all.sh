#!/bin/bash

# Cleanup function
cleanup() {
    echo ""
    echo "========================================"
    echo "  Stopping All Distributed File Storage Nodes"
    echo "========================================"
    
    # Kill all node.exe processes
    pkill -9 -f "node.exe" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "[OK] All node.exe processes stopped"
    else
        echo "[INFO] No node.exe processes were running"
    fi
    
    # Kill Process Manager
    pkill -9 -f "manager.exe" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "[OK] Process Manager stopped"
    else
        echo "[INFO] Process Manager was not running"
    fi
    
    # Kill React process
    pkill -9 -f "npm start" 2>/dev/null
    pkill -9 -f "npm" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "[OK] React frontend stopped"
    else
        echo "[INFO] React frontend was not running"
    fi
    
    # Force kill any remaining processes
    pkill -9 -f "node" 2>/dev/null
    pkill -9 -f "manager" 2>/dev/null
    
    echo ""
    echo "========================================"
    echo "  System Shutdown Complete"
    echo "========================================"
    echo ""
    echo "All nodes and services have been stopped."
    echo "This script stops both single-node and multi-node setups."
    exit 0
}

# Set up trap for cleanup
trap cleanup INT TERM EXIT

echo "========================================"
echo "  Stopping All Distributed File Storage Nodes"
echo "========================================"
echo ""

# Check if any node processes are running
if pgrep -f "node.exe" > /dev/null; then
    echo "Stopping all node.exe processes..."
    pkill -9 -f "node.exe"
    echo "[OK] All node.exe processes stopped"
else
    echo "[INFO] No node.exe processes were running"
fi

echo ""

# Check if Process Manager is running
if pgrep -f "manager.exe" > /dev/null; then
    echo "Stopping Process Manager..."
    pkill -9 -f "manager.exe"
    echo "[OK] Process Manager stopped"
else
    echo "[INFO] Process Manager was not running"
fi

echo ""

# Check if React is running
if pgrep -f "npm start" > /dev/null; then
    echo "Stopping React frontend..."
    pkill -9 -f "npm start"
    pkill -9 -f "npm"
    echo "[OK] React frontend stopped"
else
    echo "[INFO] React frontend was not running"
fi

echo ""
echo "========================================"
echo "  System Shutdown Complete"
echo "========================================"
echo ""
echo "All nodes and services have been stopped."
echo "This script stops both single-node and multi-node setups."
