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
    
    # Kill Go node process
    if [ ! -z "$NODE_PID" ]; then
        kill $NODE_PID 2>/dev/null
        echo "Go backend node stopped"
    fi
    
    # Also kill by port as backup
    pkill -f "npm start" 2>/dev/null
    pkill -f "node.exe" 2>/dev/null
    
    echo "All services stopped."
    exit 0
}

# Set up trap for cleanup on exit
trap cleanup INT TERM EXIT

echo "========================================"
echo "  Distributed File Storage - Single Node"
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

# Start Go node
echo "🚀 Starting Go backend node..."
./node.exe -id=node1 -addr=localhost -port=8080 -data=./data-node1
NODE_PID=$!

echo ""
echo "========================================"
echo "  System Running!"
echo "========================================"
echo ""
echo "📱 Frontend: http://localhost:3000"
echo "📊 Monitor:  http://localhost:3000/monitor"
echo "🔧 Backend:  http://localhost:8080"
echo ""
echo "Press Ctrl+C to stop all services"
echo ""

# Wait for interrupt
trap "kill $REACT_PID $NODE_PID 2>/dev/null; exit" INT TERM EXIT
