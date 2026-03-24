package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

// NodeInfo tracks a node's process information
type NodeInfo struct {
	NodeID       string
	Port         string
	Cmd          *exec.Cmd
	RestartCount int
	LastRestart  time.Time
	Pid          int
}

// ProcessManager monitors and automatically restarts failed nodes
type ProcessManager struct {
	mu           sync.RWMutex
	nodes        map[string]*NodeInfo
	restartDelay time.Duration
	maxRestarts  int
	stopChan     chan struct{}
}

func NewProcessManager(restartDelay time.Duration, maxRestarts int) *ProcessManager {
	return &ProcessManager{
		nodes:        make(map[string]*NodeInfo),
		restartDelay: restartDelay,
		maxRestarts:  maxRestarts,
		stopChan:     make(chan struct{}),
	}
}

func (pm *ProcessManager) StartNode(nodeID, port string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	log.Printf("[MANAGER] Starting node %s on port %s", nodeID, port)

	// Build peers list based on which node this is
	var peersArg string
	switch nodeID {
	case "node1":
		peersArg = "-peers=node2=localhost:8081,node3=localhost:8082"
	case "node2":
		peersArg = "-peers=node1=localhost:8080,node3=localhost:8082"
	case "node3":
		peersArg = "-peers=node1=localhost:8080,node2=localhost:8081"
	}

	// Set data directory for each node
	dataDir := fmt.Sprintf("-data=data-%s", nodeID)

	// Set address (use localhost for all nodes in local cluster)
	addr := "-addr=localhost"

	// Use compiled node.exe instead of 'go run' for better performance and process visibility
	cmd := exec.Command("./node.exe", 
		fmt.Sprintf("-id=%s", nodeID),
		addr,
		fmt.Sprintf("-port=%s", port),
		dataDir,
		peersArg)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start node %s: %v", nodeID, err)
	}

	pm.nodes[nodeID] = &NodeInfo{
		NodeID:       nodeID,
		Port:         port,
		Cmd:          cmd,
		RestartCount: 0,
		Pid:          cmd.Process.Pid,
	}

	log.Printf("[MANAGER] ✓ Node %s started with PID %d", nodeID, cmd.Process.Pid)
	return nil
}

func (pm *ProcessManager) MonitorAll() {
	log.Printf("[MANAGER] Starting health monitoring loop...")
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.checkNodes()
		case <-pm.stopChan:
			log.Printf("[MANAGER] Monitoring stopped")
			return
		}
	}
}

func (pm *ProcessManager) checkNodes() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for nodeID, node := range pm.nodes {
		// Check if process is still running
		if node.Cmd != nil && node.Cmd.Process != nil {
			// Try to find process - if it doesn't exist, it died
			process, err := os.FindProcess(node.Pid)
			if err != nil || process == nil {
				log.Printf("[MANAGER] ✗ Node %s (PID %d) not found - restarting...", nodeID, node.Pid)
				pm.restartNode(nodeID, node.Port)
				continue
			}

			// On Windows, always try to restart if we can't determine status reliably
			// Windows FindProcess always succeeds, so we use a different approach
			// We'll rely on checking if the API port is responding instead
			if !pm.isNodeResponding(node.Port) {
				log.Printf("[MANAGER] ✗ Node %s (port %s) not responding - restarting...", nodeID, node.Port)
				pm.restartNode(nodeID, node.Port)
			}
		}
	}
}

func (pm *ProcessManager) isNodeResponding(port string) bool {
	// Simple check: try to connect to the node's API
	cmd := exec.Command("curl", "-s", "-o", "nul", "-w", "%{http_code}", 
		fmt.Sprintf("http://localhost:%s/api/status", port))
	
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	
	// Check if response is 200 OK
	return string(output) == "200"
}

func (pm *ProcessManager) restartNode(nodeID, port string) {
	node := pm.nodes[nodeID]
	
	if node.RestartCount >= pm.maxRestarts {
		log.Printf("[MANAGER] ✗ Node %s exceeded max restarts (%d), not restarting", nodeID, pm.maxRestarts)
		return
	}

	// Wait before restarting
	if !node.LastRestart.IsZero() {
		waitTime := pm.restartDelay - time.Since(node.LastRestart)
		if waitTime > 0 {
			log.Printf("[MANAGER] Waiting %v before restarting node %s", waitTime.Round(time.Second), nodeID)
			time.Sleep(waitTime)
		}
	}

	node.RestartCount++
	node.LastRestart = time.Now()

	log.Printf("[MANAGER] 🔄 Restarting node %s on port %s (attempt %d/%d)...", nodeID, port, node.RestartCount, pm.maxRestarts)

	// Kill old process if still around
	if node.Cmd != nil && node.Cmd.Process != nil {
		node.Cmd.Process.Kill()
	}

	// Build peers list based on which node this is
	var peersArg string
	switch nodeID {
	case "node1":
		peersArg = "-peers=node2=localhost:8081,node3=localhost:8082"
	case "node2":
		peersArg = "-peers=node1=localhost:8080,node3=localhost:8082"
	case "node3":
		peersArg = "-peers=node1=localhost:8080,node2=localhost:8081"
	}

	// Set data directory for each node
	dataDir := fmt.Sprintf("-data=data-%s", nodeID)

	// Set address (use localhost for all nodes in local cluster)
	addr := "-addr=localhost"

	// Use compiled node.exe instead of 'go run' for better performance and process visibility
	cmd := exec.Command("./node.exe",
		fmt.Sprintf("-id=%s", nodeID),
		addr,
		fmt.Sprintf("-port=%s", port),
		dataDir,
		peersArg)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Printf("[MANAGER] ✗ Failed to restart node %s: %v", nodeID, err)
		return
	}

	node.Cmd = cmd
	node.Pid = cmd.Process.Pid

	log.Printf("[MANAGER] ✓ Node %s restarted successfully with PID %d", nodeID, node.Pid)
}

func (pm *ProcessManager) Shutdown() {
	close(pm.stopChan)
	
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, node := range pm.nodes {
		if node.Cmd != nil && node.Cmd.Process != nil {
			node.Cmd.Process.Kill()
		}
	}

	log.Printf("[MANAGER] All nodes stopped")
}

func main() {
	log.Println("🚀 Process Manager Starting...")

	// Create manager: 30 second delay between restarts, max 3 restarts
	manager := NewProcessManager(30*time.Second, 3)

	// Start all nodes
	nodes := []struct{
		id   string
		port string
	}{
		{"node1", "8080"},
		{"node2", "8081"},
		{"node3", "8082"},
	}

	for _, node := range nodes {
		if err := manager.StartNode(node.id, node.port); err != nil {
			log.Fatalf("Failed to start %s: %v", node.id, err)
		}
		time.Sleep(1 * time.Second) // Stagger starts
	}

	// Give nodes time to initialize
	time.Sleep(5 * time.Second)

	// Start monitoring
	go manager.MonitorAll()

	log.Println("✅ All nodes started, monitoring for failures...")

	// Keep running until Ctrl+C
	select {}
}
