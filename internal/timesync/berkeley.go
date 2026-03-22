package timesync

import (
	"sync"
	"time"
)

// BerkeleyNode represents a node participating in Berkeley algorithm
type BerkeleyNode struct {
	mu        sync.RWMutex
	nodeID    string
	clock     *MonotonicClock
	isLeader  bool
	delta     time.Duration
	stopCh    chan struct{}
	wg        sync.WaitGroup
	interval  time.Duration
}

// NewBerkeleyNode creates a new Berkeley algorithm node
func NewBerkeleyNode(nodeID string, clock *MonotonicClock, isLeader bool, interval time.Duration) *BerkeleyNode {
	return &BerkeleyNode{
		nodeID:   nodeID,
		clock:    clock,
		isLeader: isLeader,
		delta:    0,
		stopCh:   make(chan struct{}),
		interval: interval,
	}
}

// TimeSample represents a time reading from a node
type TimeSample struct {
	NodeID    string
	LocalTime time.Time
	Delta     time.Duration
}

// BerkeleyLeader coordinates time synchronization across the cluster
type BerkeleyLeader struct {
	mu          sync.RWMutex
	clock       *MonotonicClock
	nodes       map[string]*BerkeleyNode
	interval    time.Duration
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewBerkeleyLeader creates a new Berkeley leader
func NewBerkeleyLeader(clock *MonotonicClock, interval time.Duration) *BerkeleyLeader {
	return &BerkeleyLeader{
		clock:    clock,
		nodes:    make(map[string]*BerkeleyNode),
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// RegisterNode adds a node to the cluster
func (bl *BerkeleyLeader) RegisterNode(node *BerkeleyNode) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.nodes[node.nodeID] = node
}

// UnregisterNode removes a node from the cluster
func (bl *BerkeleyLeader) UnregisterNode(nodeID string) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	delete(bl.nodes, nodeID)
}

// CollectTimeSamples gathers time from all registered nodes
// In real impl, this would poll nodes via HTTP; here we simulate
func (bl *BerkeleyLeader) CollectTimeSamples(getNodeTime func(string) (time.Time, error)) []TimeSample {
	bl.mu.RLock()
	defer bl.mu.RUnlock()

	samples := make([]TimeSample, 0, len(bl.nodes)+1)

	// Leader's own time
	leaderTime := bl.clock.Now()
	samples = append(samples, TimeSample{
		NodeID:    "leader",
		LocalTime: leaderTime,
		Delta:     0,
	})

	// Collect from each node
	for id := range bl.nodes {
		nodeTime, err := getNodeTime(id)
		if err != nil {
			continue // Skip unreachable nodes
		}
		delta := nodeTime.Sub(leaderTime)
		samples = append(samples, TimeSample{
			NodeID:    id,
			LocalTime: nodeTime,
			Delta:     delta,
		})
	}

	return samples
}

// CalculateAverage computes the average time delta across all samples
func (bl *BerkeleyLeader) CalculateAverage(samples []TimeSample) time.Duration {
	if len(samples) == 0 {
		return 0
	}

	var totalDelta time.Duration
	for _, s := range samples {
		totalDelta += s.Delta
	}

	return totalDelta / time.Duration(len(samples))
}

// ComputeAdjustments calculates the offset each node should apply
func (bl *BerkeleyLeader) ComputeAdjustments(samples []TimeSample, averageDelta time.Duration) map[string]time.Duration {
	adjustments := make(map[string]time.Duration)

	for _, s := range samples {
		// Adjustment = averageDelta - node'sDelta
		// Positive = node is behind, needs to speed up
		// Negative = node is ahead, needs to slow down
		adjustments[s.NodeID] = averageDelta - s.Delta
	}

	return adjustments
}

// BroadcastAdjustments sends offset adjustments to all nodes
// In real impl, this would send via HTTP; here we apply directly for simulation
func (bl *BerkeleyLeader) BroadcastAdjustments(adjustments map[string]time.Duration) {
	bl.mu.RLock()
	defer bl.mu.RUnlock()

	// Apply leader's own adjustment
	if adj, ok := adjustments["leader"]; ok {
		bl.clock.AdjustOffset(adj)
	}

	// Apply to registered nodes
	for id, node := range bl.nodes {
		if adj, ok := adjustments[id]; ok {
			node.applyAdjustment(adj)
		}
	}
}

// PerformRound executes one full Berkeley synchronization round
func (bl *BerkeleyLeader) PerformRound(getNodeTime func(string) (time.Time, error)) {
	samples := bl.CollectTimeSamples(getNodeTime)
	if len(samples) == 0 {
		return
	}

	averageDelta := bl.CalculateAverage(samples)
	adjustments := bl.ComputeAdjustments(samples, averageDelta)
	bl.BroadcastAdjustments(adjustments)
}

// Start begins periodic Berkeley synchronization
func (bl *BerkeleyLeader) Start(getNodeTime func(string) (time.Time, error)) {
	bl.wg.Add(1)
	go func() {
		defer bl.wg.Done()
		ticker := time.NewTicker(bl.interval)
		defer ticker.Stop()

		// Initial sync
		bl.PerformRound(getNodeTime)

		for {
			select {
			case <-ticker.C:
				bl.PerformRound(getNodeTime)
			case <-bl.stopCh:
				return
			}
		}
	}()
}

// Stop halts periodic synchronization
func (bl *BerkeleyLeader) Stop() {
	close(bl.stopCh)
	bl.wg.Wait()
}

// applyAdjustment updates the node's clock offset
func (bn *BerkeleyNode) applyAdjustment(adj time.Duration) {
	bn.mu.Lock()
	defer bn.mu.Unlock()
	bn.delta = adj
	bn.clock.AdjustOffset(adj)
}

// GetDelta returns the last applied delta
func (bn *BerkeleyNode) GetDelta() time.Duration {
	bn.mu.RLock()
	defer bn.mu.RUnlock()
	return bn.delta
}
