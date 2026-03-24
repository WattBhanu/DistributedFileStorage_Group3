#!/bin/bash

# Cleanup function
cleanup() {
    echo ""
    echo "========================================"
    echo "  Stopping All Distributed File Storage Nodes"
    echo "========================================"
    
    # Kill all node.exe processes
    pkill -f "node.exe" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "[OK] All node.exe processes stopped"
    else
        echo "[INFO] No node.exe processes were running"
    fi
    
    # Kill React process
    pkill -f "npm start" 2>/dev/null
    if [ $? -eq 0 ]; then
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
    pkill -f "node.exe"
    echo "[OK] All node.exe processes stopped"
else
    echo "[INFO] No node.exe processes were running"
fi

echo ""

# Check if React is running
if pgrep -f "npm start" > /dev/null; then
    echo "Stopping React frontend..."
    pkill -f "npm start"
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
