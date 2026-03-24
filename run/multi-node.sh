#!/bin/bash

# Cleanup function
cleanup() {
    echo ""
    echo "========================================"
    echo "  Shutting down all services..."
    echo "========================================"
    
    # Kill React process
    if [ ! -z "$REACT_PID" ]; then
        kill $REACT_PID 2>/dev/null
        echo "React frontend stopped"
    fi
    
    # Kill process manager (which will stop all nodes)
    if [ ! -z "$MANAGER_PID" ]; then
        kill $MANAGER_PID 2>/dev/null
        echo "Process Manager stopped"
    fi
    
    # Also kill by port as backup
    pkill -f "npm start" 2>/dev/null
    pkill -f "manager.exe" 2>/dev/null
    pkill -f "node.exe" 2>/dev/null
    
    echo "All services stopped."
    exit 0
}

# Set up trap for cleanup on exit
trap cleanup INT TERM EXIT

echo "========================================"
echo "  Distributed File Storage - Multi-Node Cluster"
echo "========================================"
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "❌ Error: Go is not installed. Please install Go first."
    exit 1
fi

echo "✅ Go detected: $(go version)"
echo ""

# Build the node
echo "🔨 Building Go node..."
go build -o node.exe ./cmd/node
if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi
echo "✅ Build successful!"
echo ""

# Build the process manager
echo "🔨 Building Process Manager..."
go build -o manager.exe ./cmd/manager
if [ $? -ne 0 ]; then
    echo "❌ Manager build failed!"
    exit 1
fi
echo "✅ Manager built successfully!"
echo ""

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data-node1 data-node2 data-node3
echo "✅ Directories ready"
echo ""

# Install React dependencies (only if not already installed)
echo "📦 Checking React dependencies..."
cd frontend-react
if [ ! -d "node_modules" ]; then
    echo "Installing React dependencies..."
    npm install
    if [ $? -ne 0 ]; then
        echo "❌ npm install failed!"
        exit 1
    fi
    echo "✅ Dependencies installed"
else
    echo "✅ Dependencies already present"
fi
cd ..
echo ""

# Start React app in background
echo "🚀 Starting React frontend..."
cd frontend-react
npm start &
REACT_PID=$!
cd ..

echo "⏳ Waiting for React to start (5 seconds)..."
sleep 5
echo ""

# Start Master Node
echo "🚀 Starting Process Manager with auto-recovery..."
./manager.exe &
MANAGER_PID=$!
sleep 5

echo ""
echo "========================================"
echo "  Cluster Running with Auto-Recovery!"
echo "========================================"
echo ""
echo "👑 MASTER:  node1 (port 8080)"
echo "👤 SLAVE 1: node2 (port 8081)"
echo "👤 SLAVE 2: node3 (port 8082)"
echo ""
echo "📱 Frontend: http://localhost:3000"
echo "📊 Monitor:  http://localhost:3000/monitor"
echo ""
echo "🔄 Files uploaded to master will auto-replicate to slaves"
echo ""
echo "AUTOMATIC RECOVERY ENABLED:"
echo "- Dead nodes will auto-restart within 30 seconds"
echo "- Max 3 restart attempts per node"
echo "- Health checks every 5 seconds"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# Wait for interrupt
trap "kill $REACT_PID $MANAGER_PID 2>/dev/null; exit" INT TERM EXIT
