package fault

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Detector struct {
	cfg      DetectorConfig
	checker  HealthChecker
	listener EventListener

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

	if node.Status == Failed {
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
			d.updateStatusLocked(node, Healthy, "node responded to health check", now)
		}
		return
	}

	node.MissedHeartbeats++

	if node.Status == Healthy && d.shouldSuspect(node, now) {
		d.updateStatusLocked(node, Suspected, fmt.Sprintf("health check failed: %v", err), now)
	}

	if node.Status == Suspected && d.shouldFail(node, now) {
		d.updateStatusLocked(node, Failed, fmt.Sprintf("node exceeded failure timeout: %v", err), now)
	}
}

func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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
