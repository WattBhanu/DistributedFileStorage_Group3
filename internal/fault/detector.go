package fault

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Detector struct {
	cfg        DetectorConfig
	checker    HealthChecker
	listener   EventListener
	supervisor *ProcessSupervisor

	mu    sync.RWMutex
	nodes map[string]*NodeHealth
}

func NewDetector(cfg DetectorConfig, checker HealthChecker, listener EventListener) *Detector {
	return &Detector{
		cfg:      cfg,
		checker:  checker,
		listener: listener,
		nodes:    make(map[string]*NodeHealth),
	}
}

// NewDetectorWithSupervisor creates a detector with automatic restart capability
func NewDetectorWithSupervisor(cfg DetectorConfig, checker HealthChecker, listener EventListener, supervisor *ProcessSupervisor) *Detector {
	return &Detector{
		cfg:        cfg,
		checker:    checker,
		listener:   listener,
		supervisor: supervisor,
		nodes:      make(map[string]*NodeHealth),
	}
}

func (d *Detector) AddNode(nodeID, address string) {
	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	d.nodes[nodeID] = &NodeHealth{
		NodeID:        nodeID,
		Address:       address,
		Status:        Healthy,
		LastHeartbeat: now,
		LastChanged:   now,
	}
	log.Printf("[FAULT] Added node %s at %s to health monitoring", nodeID, address)
}

func (d *Detector) RemoveNode(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.nodes, nodeID)
}

func (d *Detector) GetNode(nodeID string) (NodeHealth, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	node, ok := d.nodes[nodeID]
	if !ok {
		return NodeHealth{}, false
	}

	return *node, true
}

func (d *Detector) ListNodes() []NodeHealth {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodes := make([]NodeHealth, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, *node)
	}

	return nodes
}

func (d *Detector) CheckNode(nodeID string) {
	d.mu.Lock()
	node, ok := d.nodes[nodeID]
	if !ok {
		d.mu.Unlock()
		return
	}

	addr := node.Address
	d.mu.Unlock()

	now := time.Now()
	err := d.checker.Ping(addr)

	d.mu.Lock()
	defer d.mu.Unlock()

	node, ok = d.nodes[nodeID]
	if !ok {
		return
	}

	if err == nil {
		node.LastHeartbeat = now
		node.MissedHeartbeats = 0
		if node.Status != Healthy {
			log.Printf("[FAULT] Node %s recovered: %s", nodeID, "node responded to health check")
			d.updateStatusLocked(node, Healthy, "node responded to health check", now)
		} else {
			log.Printf("[FAULT] Health check OK for node %s", nodeID)
		}
		return
	}

	log.Printf("[FAULT] Health check FAILED for node %s: %v", nodeID, err)
	d.recordMissedHeartbeatLocked(node)

	if node.Status == Healthy && d.shouldSuspect(node, now) {
		log.Printf("[FAULT] Node %s marked as SUSPECTED: %s", nodeID, fmt.Sprintf("health check failed: %v", err))
		d.updateStatusLocked(node, Suspected, fmt.Sprintf("health check failed: %v", err), now)
	}

	if node.Status == Suspected && d.shouldFail(node, now) {
		log.Printf("[FAULT] Node %s marked as FAILED: %s", nodeID, fmt.Sprintf("node exceeded failure timeout: %v", err))
		d.updateStatusLocked(node, Failed, fmt.Sprintf("node exceeded failure timeout: %v", err), now)
		
		// Trigger automatic restart if supervisor is available
		if d.supervisor != nil {
			go d.supervisor.OnNodeFailure(d, nodeID, addr)
		}
	}
}

func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.HeartbeatInterval)
	defer ticker.Stop()

	log.Printf("[FAULT] Fault detector started with interval %v", d.cfg.HeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[FAULT] Fault detector stopping...")
			return
		case <-ticker.C:
			for _, node := range d.ListNodes() {
				d.CheckNode(node.NodeID)
			}
		}
	}
}

func (d *Detector) shouldSuspect(node *NodeHealth, now time.Time) bool {
	if node.MissedHeartbeats >= d.cfg.MaxMissedBeats {
		return true
	}

	return now.Sub(node.LastHeartbeat) >= d.cfg.SuspectTimeout
}

func (d *Detector) shouldFail(node *NodeHealth, now time.Time) bool {
	return now.Sub(node.LastHeartbeat) >= d.cfg.FailureTimeout
}

func (d *Detector) recordMissedHeartbeatLocked(node *NodeHealth) {
	node.MissedHeartbeats++
}

func (d *Detector) updateStatusLocked(node *NodeHealth, status HealthStatus, reason string, now time.Time) {
	oldStatus := node.Status
	node.Status = status
	node.LastChanged = now

	if d.listener != nil && oldStatus != status {
		d.listener.OnStateChange(FailureEvent{
			NodeID:    node.NodeID,
			OldStatus: oldStatus,
			NewStatus: status,
			Reason:    reason,
			At:        now,
		})
	}
}
