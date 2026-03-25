# Distributed File Storage System

## Team Members

| Registration Number | Name                    | Email                       |
|---------------------|-------------------------|-----------------------------|
| IT24610328          | Wattegama B. C.         | wattbhanu@gmail.com         |
| IT24101848          | Perera H. L. P. S.      | praveensanjaya006@gmail.com |
| IT24103444          | De Silva J. W. D. J. O. | oneldesilva868@gmail.com    |
| IT24103469          | Withanage W. D. D. R.   | dinugaransen451@gmail.com   |

## Group
Group 3

---

## 🎯 Quick Start

**Prerequisites:**
- Go 1.26.1 or later
- Node.js 18+

**Note:** `node_modules` is excluded from Git repository. The run scripts will automatically install React dependencies on first run.

### 🚀 One-Command Run (Recommended)

**For Windows:**

Single Node:
```bash
run\single-node.bat
```

Multi-Node Cluster:
```bash
run\multi-node.bat
```

**For Linux/Mac:**

Single Node:
```bash
chmod +x run/*.sh
./run/single-node.sh
```

Multi-Node Cluster:
```bash
chmod +x run/*.sh
./run/multi-node.sh
```

### What Happens When You Run the Scripts?

The automated scripts will:
1. ✅ Check if Go is installed
2. 🔨 Build the Go backend automatically
3. 📁 Create data directories (for multi-node)
4. 📦 Install React dependencies **only if not already present** (first run only)
5. 🚀 Start React frontend in a new window
6. ⏳ Wait 5 seconds for React to initialize
7. 🚀 Start Go node(s) with proper configuration
8. 📊 Display all access URLs

**Then just open your browser:**
- Frontend: http://localhost:3000
- Monitor Dashboard: http://localhost:3000/monitor
- Backend API: http://localhost:8080

**💡 Pro Tip:** Subsequent runs skip dependency installation, making startup faster!

### 🛑 How to Stop the System

**For Windows:**
```batch
run\stop-all.bat
```

**For Linux/Mac:**
```bash
chmod +x run/stop-all.sh
./run/stop-all.sh
```

**What the stop script does:**
- ✅ Stops all node.exe processes
- ✅ Stops React frontend
- ✅ Cleans up all resources

**Alternative Manual Stop:**
- In the terminal where the batch/sh script is running, press **Ctrl+C**
- If that doesn't work, close the terminal window
- Or use the `stop-all.bat` / `stop-all.sh` scripts above

---

### ⚠️ Manual Failsafe - Troubleshooting Startup Issues

If the automated scripts fail (e.g., npm not found, build errors), use these manual steps:

#### **Prerequisites Check**
1. **Install Node.js**: Download from https://nodejs.org/
2. **Verify npm**: Open new terminal and run `npm --version`
3. **Install Go**: Download from https://go.dev/
4. **Verify Go**: Run `go version`

#### **Manual Startup - Single Node (Windows)**
```batch
REM Terminal 1 - Build and start Go backend
cd C:\path\to\DistributedFileStorage_Group3
go build -o node.exe .\cmd\node
node.exe -id=node1 -addr=localhost -port=8080 -data=.

REM Terminal 2 - Install and start React frontend
cd C:\path\to\DistributedFileStorage_Group3\frontend-react
npm install
npm start
```

#### **Manual Startup - Multi-Node Cluster (Windows)**
```batch
REM Terminal 1 - Leader node
go build -o node.exe .\cmd\node
node.exe -id=node1 -addr=localhost -port=8080 -peers=node2=localhost:8081,node3=localhost:8082

REM Terminal 2 - Follower 1
node.exe -id=node2 -addr=localhost -port=8081 -peers=node1=localhost:8080,node3=localhost:8082

REM Terminal 3 - Follower 2
node.exe -id=node3 -addr=localhost -port=8082 -peers=node1=localhost:8080,node2=localhost:8081

REM Terminal 4 - React frontend
cd frontend-react
npm install
npm start
```

#### **Manual Startup - Linux/Mac**
```bash
# Terminal 1 - Backend
cd /path/to/DistributedFileStorage_Group3
go build -o node ./cmd/node
./node -id=node1 -addr=localhost -port=8080

# Terminal 2 - Frontend
cd frontend-react
npm install
npm start
```

#### **Manual Process Cleanup (If Scripts Fail)**
**Windows:**
```batch
REM Kill all node processes
taskkill /F /IM node.exe
taskkill /F /IM manager.exe

REM Kill npm/React processes
for /f "tokens=5" %a in ('netstat -aon ^| findstr :3000') do taskkill /F /PID %a
```

**Linux/Mac:**
```bash
# Kill all node processes
pkill -9 -f node
pkill -9 -f npm

# Kill process on port 3000
lsof -ti:3000 | xargs kill -9
```

#### **Common Issues & Solutions**

**Issue: "npm is not recognized"**
- **Solution**: Install Node.js from https://nodejs.org/ and restart terminal
- **Check**: `where npm` (Windows) or `which npm` (Linux/Mac)

**Issue: Port already in use**
- **Solution**: Kill process using the port
  ```batch
  # Windows - Find PID
  netstat -ano | findstr :8080
  # Kill it (replace PID)
  taskkill /F /PID <PID>
  ```

**Issue: React won't build**
- **Solution**: Clear cache and reinstall
  ```bash
  cd frontend-react
  rm -rf node_modules package-lock.json
  npm install
  npm start
  ```

**Issue: Go compilation errors**
- **Solution**: Clean build cache
  ```bash
  go clean -cache
  go build ./cmd/node
  ```

---

### Manual Running (Advanced)

If you prefer manual control, see detailed instructions in IMPLEMENTATION_SUMMARY.md

---

## 📖 Command Line Flags

| Flag | Description | Default | Example |
|------|-------------|---------|---------|
| `-id` | Unique node identifier | `node1` | `-id=node-master` |
| `-addr` | Node address (IP/hostname) | `localhost` | `-addr=192.168.1.100` |
| `-port` | HTTP server port | `8080` | `-port=9000` |
| `-data` | Data directory path | `./data` | `-data=/storage/data` |
| `-peers` | Comma-separated peer list | `` | `-peers=node1=ip:port,node2=ip:port` |

### Peer List Format
```
node1=192.168.1.100:8080,node2=192.168.1.101:8081,node3=192.168.1.102:8082
```

---

## 🌐 Using the Application

### File Management Interface (http://localhost:3000)

#### Upload Files
1. Click **"Upload"** button
2. Select file(s) from your computer
3. Wait for success notification
4. Files automatically replicate to all nodes

#### View Files
- **Tree View**: Hierarchical folder structure
  - Click folders to expand/collapse
  - See nested file organization
- **List View**: Table with details
  - Shows: Type, Name, Size, Created Date
  - Click column headers to sort

#### Download Files
- Click download icon (⬇️) next to any file
- File downloads to your browser's download folder

#### Search Files
1. Type in search bar at top
2. Results filter in real-time
3. Works in both tree and list views

#### Refresh File List
- Click **Refresh** button (🔄)
- Auto-refreshes every 30 seconds

---

### Algorithm Monitor Dashboard (http://localhost:3000/monitor)

#### Consensus Panel (Raft)
- **Current State**: Leader or Follower
- **Leader Node**: Which node is currently the leader
- **Role**: Whether this node is LEADER or FOLLOWER
- **Term**: Current Raft election term
- **Vote Count**: Votes received in current election

#### Replication Panel
- **Files Stored**: Total files in distributed storage
- **Known Peers**: Number of peer nodes in cluster
- **Peer List**: List of all peer node IDs and addresses

#### Fault Tolerance Panel
- **Healthy Nodes**: Nodes operating normally (green)
- **Suspected Nodes**: Possibly failed nodes (yellow)
- **Failed Nodes**: Confirmed down nodes (red)
- **Node Status Details**: Detailed status for each node

#### Time Synchronization Panel
- **Protocol**: Berkeley (for followers) or Cristian
- **Last Sync**: When clocks were last synchronized

---

## 🔧 Admin Operations

### Simulate Node Failure

**Via UI:**
1. Go to File Manager
2. Scroll to "Node Management" section
3. Enter node ID (e.g., `node2`)
4. Click **"Kill Node"**

**Via API:**
```bash
curl -X DELETE http://localhost:8080/api/admin/kill/node2
```

### Recover Failed Node

**Via UI:**
1. Enter node ID in Node Management section
2. Click **"Heal Node"**

**Via API:**
```bash
curl -X POST http://localhost:8080/api/admin/heal/node2
```

---

## 📊 Monitoring & Debugging

### Check Node Status
```bash
curl http://localhost:8080/api/status
```

Response:
```json
{
  "node_id": "node1",
  "role": "Leader",
  "raft_state": "Leader",
  "files_count": 5,
  "timestamp": 1234567890
}
```

### Get Metrics
```bash
curl http://localhost:8080/api/metrics
```

### View Logs
Go logs to console automatically. Look for:
- `[node1] Starting node...`
- `[node1] API server listening on port 8080`
- `[node1] Replication manager started`

---

## ⚠️ Troubleshooting

### Port Already in Use
```bash
# Windows - Find process using port 8080
netstat -ano | findstr :8080

# Kill process (replace PID)
taskkill /PID <PID> /F
```

### React Won't Start
```bash
# Clear cache and reinstall
cd frontend-react
rm -rf node_modules package-lock.json
npm install
npm start
```

### Go Compilation Errors
```bash
# Clean build cache
go clean -cache
go build ./cmd/node
```

### Can't Connect to Node
1. Check if node is running (look for log messages)
2. Verify port isn't blocked by firewall
3. Try `curl http://localhost:8080/health`

---

## 🎯 Common Use Cases

### Scenario 1: Test Single Node
```bash
# Terminal 1
go run cmd/node/main.go

# Terminal 2
cd frontend-react && npm start

# Browser: http://localhost:3000
# Upload files, test features
```

### Scenario 2: Test Replication
```bash
# Terminal 1 - Leader
go run cmd/node/main.go -id=node1 -port=8080

# Terminal 2 - Follower
go run cmd/node/main.go -id=node2 -port=8081 -peers=node1=localhost:8080

# Terminal 3 - React
cd frontend-react && npm start

# Browser: Upload file to leader
# Verify: File appears on follower too
```

### Scenario 3: Test Fault Tolerance
```bash
# Start 3 nodes (see Method 2 above)

# Via UI: Kill node2
# Observe: System continues operating
# Via UI: Heal node2
# Observe: Node rejoins cluster
```

---

## ✅ Verification Checklist

After setup, verify:
- [ ] Go node starts without errors
- [ ] React app loads in browser
- [ ] Can upload a file
- [ ] Can download the same file
- [ ] File appears in both tree and list views
- [ ] Search functionality works
- [ ] Monitor dashboard shows metrics
- [ ] All icons display correctly
- [ ] No console errors in browser

---

## 📝 System Features

### Backend
- HTTP-based communication (no terminal commands)
- Raft consensus algorithm
- Primary-backup replication
- Fault tolerance with heartbeat detection
- Berkeley time synchronization
- SHA256 checksum verification
- Write-Ahead Logging (WAL)

### Frontend
- Modern React 18 application
- Built-in SVG icons (no external dependencies)
- Real-time updates (2s polling for monitor, 30s for files)
- Beautiful gradient themes
- Responsive design
- Component-based architecture

---

## 📈 Performance Tips

1. **Use SSD Storage** for data directories
2. **Increase HTTP Timeout** for large files (edit `network/request_sender.go`)
3. **Adjust Refresh Intervals** in React components if needed
4. **Limit Concurrent Uploads** to prevent overload
5. **Monitor Log Size** in production deployments

---

## 🔐 Security Notes

- CORS configured for development
- XSS prevention enabled
- Input validation on all endpoints
- File uploads limited to 32MB

---

## 🆘 Getting Help

If you encounter issues:
1. Review error messages carefully
2. Verify all prerequisites installed
3. Ensure ports aren't blocked by firewall
4. Check Go and Node.js versions
5. See troubleshooting section above

---

**System Version:** 1.0  
**Status:** Production Ready  
**Last Updated:** March 24, 2026

For detailed implementation details, see IMPLEMENTATION_SUMMARY.md