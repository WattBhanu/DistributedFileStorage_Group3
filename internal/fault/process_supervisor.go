package fault

import (
	"fmt"
	"log"
	"os/exec"
	"sync"
	"time"
)

// ProcessSupervisor monitors and automatically restarts failed nodes
type ProcessSupervisor struct {
	mu            sync.RWMutex
	nodeProcesses map[string]*NodeProcess
	restartDelay  time.Duration
	maxRestarts   int
}

// NodeProcess tracks a node's process information
type NodeProcess struct {
	NodeID      string
	Command     *exec.Cmd
	RestartCount int
	LastRestart time.Time
	StopChan    chan struct{}
}

// NewProcessSupervisor creates a new process supervisor
func NewProcessSupervisor(restartDelay time.Duration, maxRestarts int) *ProcessSupervisor {
	return &ProcessSupervisor{
		nodeProcesses: make(map[string]*NodeProcess),
		restartDelay:  restartDelay,
		maxRestarts:   maxRestarts,
	}
}

// RegisterNode registers a node process for monitoring
func (ps *ProcessSupervisor) RegisterNode(nodeID string, cmd *exec.Cmd) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.nodeProcesses[nodeID] = &NodeProcess{
		NodeID:      nodeID,
		Command:     cmd,
		RestartCount: 0,
		StopChan:    make(chan struct{}),
	}
	log.Printf("[SUPERVISOR] Registered node %s for monitoring", nodeID)
}

// OnNodeFailure is called by the fault detector when a node fails
func (ps *ProcessSupervisor) OnNodeFailure(detector *Detector, nodeID string, address string) {
	ps.mu.RLock()
	nodeProc, exists := ps.nodeProcesses[nodeID]
	ps.mu.RUnlock()

	if !exists {
		log.Printf("[SUPERVISOR] Node %s not registered for auto-restart", nodeID)
		return
	}

	ps.mu.Lock()
	if nodeProc.RestartCount >= ps.maxRestarts {
		log.Printf("[SUPERVISOR] Node %s exceeded max restarts (%d), not restarting", nodeID, ps.maxRestarts)
		ps.mu.Unlock()
		return
	}
	ps.mu.Unlock()

	// Start recovery process
	go ps.restartNode(detector, nodeID, address)
}

// restartNode attempts to restart a failed node
func (ps *ProcessSupervisor) restartNode(detector *Detector, nodeID string, address string) {
	ps.mu.Lock()
	nodeProc := ps.nodeProcesses[nodeID]
	
	// Check if we should wait before restarting
	if !nodeProc.LastRestart.IsZero() {
		waitTime := ps.restartDelay - time.Since(nodeProc.LastRestart)
		if waitTime > 0 {
			log.Printf("[SUPERVISOR] Waiting %v before restarting node %s", waitTime.Round(time.Second), nodeID)
			select {
			case <-time.After(waitTime):
			case <-nodeProc.StopChan:
				log.Printf("[SUPERVISOR] Restart cancelled for node %s", nodeID)
				ps.mu.Unlock()
				return
			}
		}
	}
	
	nodeProc.RestartCount++
	nodeProc.LastRestart = time.Now()
	ps.mu.Unlock()

	log.Printf("[SUPERVISOR] Attempting to restart node %s (attempt %d/%d)", nodeID, nodeProc.RestartCount, ps.maxRestarts)

	// Mark node as recovering
	if detector != nil {
		detector.mu.Lock()
		if node, ok := detector.nodes[nodeID]; ok {
			detector.updateStatusLocked(node, Recovering, "automatic restart initiated", time.Now())
		}
		detector.mu.Unlock()
	}

	// Execute restart command
	cmd := exec.Command("cmd", "/C", 
		fmt.Sprintf("start \"\" /B go run cmd/node/main.go --node-id=%s --port=%s", 
			nodeID, extractPort(address)))
	
	if err := cmd.Start(); err != nil {
		log.Printf("[SUPERVISOR] Failed to restart node %s: %v", nodeID, err)
		
		// Mark restart as failed
		if detector != nil {
			detector.mu.Lock()
			if node, ok := detector.nodes[nodeID]; ok {
				detector.updateStatusLocked(node, Failed, "restart failed", time.Now())
			}
			detector.mu.Unlock()
		}
		return
	}

	// Update process reference
	ps.mu.Lock()
	nodeProc.Command = cmd
	ps.mu.Unlock()

	log.Printf("[SUPERVISOR] ✓ Node %s restarted successfully", nodeID)

	// Wait for recovery to complete
	time.Sleep(5 * time.Second)
	
	// Mark as healthy after successful restart
	if detector != nil {
		detector.mu.Lock()
		if node, ok := detector.nodes[nodeID]; ok {
			node.LastHeartbeat = time.Now()
			node.MissedHeartbeats = 0
			detector.updateStatusLocked(node, Healthy, "automatic restart completed", time.Now())
		}
		detector.mu.Unlock()
	}
}

// StopMonitoring stops monitoring a specific node
func (ps *ProcessSupervisor) StopMonitoring(nodeID string) {
	ps.mu.RLock()
	nodeProc, exists := ps.nodeProcesses[nodeID]
	ps.mu.RUnlock()

	if exists {
		close(nodeProc.StopChan)
		log.Printf("[SUPERVISOR] Stopped monitoring node %s", nodeID)
	}
}

// Shutdown stops all monitoring
func (ps *ProcessSupervisor) Shutdown() {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, nodeProc := range ps.nodeProcesses {
		close(nodeProc.StopChan)
	}
	log.Printf("[SUPERVISOR] Process supervisor shutdown complete")
}

// GetRestartCount returns the restart count for a node
func (ps *ProcessSupervisor) GetRestartCount(nodeID string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if nodeProc, exists := ps.nodeProcesses[nodeID]; exists {
		return nodeProc.RestartCount
	}
	return 0
}

// extractPort extracts port number from address (e.g., "localhost:8080" -> "8080")
func extractPort(address string) string {
	for i := len(address) - 1; i >= 0; i-- {
		if address[i] == ':' {
			return address[i+1:]
		}
	}
	return "8080" // default
}
