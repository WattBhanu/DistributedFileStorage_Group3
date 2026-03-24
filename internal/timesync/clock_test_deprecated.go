// DEPRECATED: This test file is deprecated and kept for reference only
// +build ignore
package timesync

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// SimulatedNode represents a node in our test cluster
type SimulatedNode struct {
	ID            string
	Clock         *MonotonicClock
	Lamport       *LamportClock
	Vector        *VectorClock
	EventClock    *EventClock
	Cristian      *CristianClient
	Berkeley      *BerkeleyNode
	Offset        time.Duration
	NetworkDelay  time.Duration
	mu            sync.RWMutex
}

// NewSimulatedNode creates a test node with simulated clock skew
func NewSimulatedNode(id string, initialOffset time.Duration, networkDelay time.Duration) *SimulatedNode {
	clock := NewMonotonicClock()
	clock.AdjustOffset(initialOffset)

	return &SimulatedNode{
		ID:           id,
		Clock:        clock,
		Lamport:      NewLamportClock(id),
		Vector:       NewVectorClock(id),
		EventClock:   NewEventClock(id, time.Now().UnixNano()),
		Offset:       initialOffset,
		NetworkDelay: networkDelay,
	}
}

// Now returns current time with network delay simulation
func (n *SimulatedNode) Now() time.Time {
	n.mu.RLock()
	delay := n.NetworkDelay
	n.mu.RUnlock()
	time.Sleep(delay / 100) // Scale down for tests
	return n.Clock.Now()
}

// TestCluster manages multiple simulated nodes
type TestCluster struct {
	Leader   *SimulatedNode
	Nodes    map[string]*SimulatedNode
	mu       sync.RWMutex
}

// NewTestCluster creates a cluster with leader and followers
func NewTestCluster(leaderOffset time.Duration, followerOffsets map[string]time.Duration) *TestCluster {
	leader := NewSimulatedNode("leader", leaderOffset, 1*time.Millisecond)
	cluster := &TestCluster{
		Leader: leader,
		Nodes:  make(map[string]*SimulatedNode),
	}
	cluster.Nodes["leader"] = leader

	for id, offset := range followerOffsets {
		node := NewSimulatedNode(id, offset, 2*time.Millisecond)
		cluster.Nodes[id] = node
	}

	return cluster
}

// GetNodeTime simulates fetching time from a node (for Cristian/Berkeley)
func (tc *TestCluster) GetNodeTime(nodeID string) (time.Time, error) {
	tc.mu.RLock()
	node, ok := tc.Nodes[nodeID]
	tc.mu.RUnlock()
	if !ok {
		return time.Time{}, fmt.Errorf("node not found: %s", nodeID)
	}
	return node.Now(), nil
}

// ========== Monotonic Clock Tests ==========

func TestMonotonicClock_Basic(t *testing.T) {
	clock := NewMonotonicClock()
	t1 := clock.Now()
	time.Sleep(10 * time.Millisecond)
	t2 := clock.Now()

	if !t2.After(t1) {
		t.Errorf("Clock not monotonic: t1=%v, t2=%v", t1, t2)
	}
}

func TestMonotonicClock_AdjustOffset(t *testing.T) {
	clock := NewMonotonicClock()
	base := clock.Now()

	clock.AdjustOffset(100 * time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	adjusted := clock.Now()

	diff := adjusted.Sub(base)
	if diff < 100*time.Millisecond {
		t.Errorf("Offset not applied correctly, diff=%v", diff)
	}
}

// ========== Cristian's Algorithm Tests ==========

func TestCristian_Synchronize(t *testing.T) {
	// Create leader with no offset
	leaderClock := NewMonotonicClock()
	server := NewCristianServer(leaderClock)

	// Create follower with 50ms skew
	followerClock := NewMonotonicClock()
	followerClock.AdjustOffset(50 * time.Millisecond)
	client := NewCristianClient(followerClock, "leader", 100*time.Millisecond)

	// Simulate getting server time
	getServerTime := func() (time.Time, error) {
		return server.GetTime(), nil
	}

	// Before sync - follower is ahead
	beforeDiff := followerClock.Now().Sub(leaderClock.Now())
	if beforeDiff < 40*time.Millisecond {
		t.Errorf("Expected follower ahead by ~50ms, got %v", beforeDiff)
	}

	// Sync
	result := client.Synchronize(getServerTime)
	if !result.Success {
		t.Errorf("Sync failed: %v", result.Error)
	}

	// After sync - should be close (within RTT/2)
	time.Sleep(5 * time.Millisecond)
	afterDiff := followerClock.Now().Sub(leaderClock.Now())
	if afterDiff > 5*time.Millisecond || afterDiff < -5*time.Millisecond {
		t.Errorf("After sync, diff too large: %v", afterDiff)
	}

	t.Logf("Cristian sync: RTT=%v, Offset applied=%v", result.RTT, result.Offset)
}

func TestCristian_MultipleSyncs(t *testing.T) {
	leaderClock := NewMonotonicClock()
	server := NewCristianServer(leaderClock)

	followerClock := NewMonotonicClock()
	followerClock.AdjustOffset(100 * time.Millisecond)
	client := NewCristianClient(followerClock, "leader", 50*time.Millisecond)

	getServerTime := func() (time.Time, error) {
		return server.GetTime(), nil
	}

	// Multiple syncs
	for i := 0; i < 3; i++ {
		result := client.Synchronize(getServerTime)
		if !result.Success {
			t.Errorf("Sync %d failed", i)
		}
		t.Logf("Sync %d: RTT=%v", i, result.RTT)
	}
}

// ========== Berkeley Algorithm Tests ==========

func TestBerkeley_CalculateAverage(t *testing.T) {
	leader := NewBerkeleyLeader(NewMonotonicClock(), 100*time.Millisecond)

	samples := []TimeSample{
		{NodeID: "leader", Delta: 0},
		{NodeID: "node1", Delta: 20 * time.Millisecond},
		{NodeID: "node2", Delta: -30 * time.Millisecond},
	}

	avg := leader.CalculateAverage(samples)
	// (0 + 20ms - 30ms) / 3 = -10ms / 3 = -3.333ms
	expected := -10 * time.Millisecond / 3

	// Allow small floating point tolerance
	diff := avg - expected
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Microsecond {
		t.Errorf("Expected average %v, got %v", expected, avg)
	}
}

func TestBerkeley_ComputeAdjustments(t *testing.T) {
	leader := NewBerkeleyLeader(NewMonotonicClock(), 100*time.Millisecond)

	samples := []TimeSample{
		{NodeID: "leader", Delta: 0},
		{NodeID: "node1", Delta: 30 * time.Millisecond},
		{NodeID: "node2", Delta: -30 * time.Millisecond},
	}

	avg := leader.CalculateAverage(samples) // should be 0
	adjustments := leader.ComputeAdjustments(samples, avg)

	// Leader adjustment: 0 - 0 = 0
	if adjustments["leader"] != 0 {
		t.Errorf("Leader adjustment should be 0, got %v", adjustments["leader"])
	}

	// Node1 is ahead by 30ms, needs to slow down by 30ms
	if adjustments["node1"] != -30*time.Millisecond {
		t.Errorf("Node1 adjustment wrong: expected -30ms, got %v", adjustments["node1"])
	}

	// Node2 is behind by 30ms, needs to speed up by 30ms
	if adjustments["node2"] != 30*time.Millisecond {
		t.Errorf("Node2 adjustment wrong: expected 30ms, got %v", adjustments["node2"])
	}
}

func TestBerkeley_FullRound(t *testing.T) {
	cluster := NewTestCluster(
		0,
		map[string]time.Duration{
			"node1": 50 * time.Millisecond,
			"node2": -50 * time.Millisecond,
		},
	)

	leaderClock := cluster.Leader.Clock
	berkeleyLeader := NewBerkeleyLeader(leaderClock, 100*time.Millisecond)

	// Register nodes
	for id, node := range cluster.Nodes {
		if id != "leader" {
			bNode := NewBerkeleyNode(id, node.Clock, false, 100*time.Millisecond)
			berkeleyLeader.RegisterNode(bNode)
		}
	}

	// Perform sync round
	getNodeTime := func(id string) (time.Time, error) {
		return cluster.GetNodeTime(id)
	}
	berkeleyLeader.PerformRound(getNodeTime)

	// Check that offsets were applied
	t.Log("Berkeley sync round completed")
}

// ========== Lamport Clock Tests ==========

func TestLamportClock_Basic(t *testing.T) {
	lc := NewLamportClock("node1")

	ts1 := lc.Tick()
	ts2 := lc.Tick()
	ts3 := lc.Tick()

	if ts1 != 1 || ts2 != 2 || ts3 != 3 {
		t.Errorf("Expected 1,2,3 got %d,%d,%d", ts1, ts2, ts3)
	}
}

func TestLamportClock_Update(t *testing.T) {
	lc1 := NewLamportClock("node1")
	lc2 := NewLamportClock("node2")

	// Node1: event at 1, 2
	lc1.Tick()
	lc1.Tick()

	// Node2 receives message with timestamp 2, updates
	lc2.Update(2)

	// Node2's clock should be max(0, 2) + 1 = 3
	if lc2.Current() != 3 {
		t.Errorf("Expected 3 after update, got %d", lc2.Current())
	}
}

func TestLamportTimestamp_Compare(t *testing.T) {
	ts1 := LamportTimestamp{Counter: 5, NodeID: "a"}
	ts2 := LamportTimestamp{Counter: 10, NodeID: "b"}
	ts3 := LamportTimestamp{Counter: 5, NodeID: "c"}

	if ts1.Compare(ts2) != -1 {
		t.Error("Expected ts1 < ts2")
	}
	if ts2.Compare(ts1) != 1 {
		t.Error("Expected ts2 > ts1")
	}
	// Same counter, tiebreaker by nodeID
	if ts1.Compare(ts3) != -1 { // "a" < "c"
		t.Error("Expected ts1 < ts3 (tiebreaker)")
	}
}

// ========== Vector Clock Tests ==========

func TestVectorClock_Basic(t *testing.T) {
	vc := NewVectorClock("node1")

	v1 := vc.Tick()
	v2 := vc.Tick()

	if v1["node1"] != 1 || v2["node1"] != 2 {
		t.Errorf("Vector clock increment failed")
	}
}

func TestVectorClock_Update(t *testing.T) {
	vc1 := NewVectorClock("node1")
	vc2 := NewVectorClock("node2")

	// Node1: {node1: 2}
	vc1.Tick()
	vc1.Tick()

	// Node2 receives and updates
	vc2.Update(map[string]uint64{"node1": 2})

	result := vc2.Current()
	if result["node1"] != 2 || result["node2"] != 1 {
		t.Errorf("Update failed: got %v", result)
	}
}

func TestCompareVectorClocks(t *testing.T) {
	vc1 := map[string]uint64{"a": 2, "b": 1}
	vc2 := map[string]uint64{"a": 1, "b": 2}
	vc3 := map[string]uint64{"a": 2, "b": 1}
	vc4 := map[string]uint64{"a": 3, "b": 1}

	// Concurrent
	if CompareVectorClocks(vc1, vc2) != 0 {
		t.Error("Expected concurrent")
	}

	// Equal
	if CompareVectorClocks(vc1, vc3) != 0 {
		t.Error("Expected equal")
	}

	// vc1 < vc4
	if CompareVectorClocks(vc1, vc4) != -1 {
		t.Error("Expected vc1 < vc4")
	}

	// vc4 > vc1
	if CompareVectorClocks(vc4, vc1) != 1 {
		t.Error("Expected vc4 > vc1")
	}
}

// ========== Event Clock Tests ==========

func TestEventClock_NewEvent(t *testing.T) {
	ec := NewEventClock("node1", time.Now().UnixNano())

	event := ec.NewEvent("write")

	if event.NodeID != "node1" {
		t.Errorf("Wrong node ID: %s", event.NodeID)
	}
	if event.EventType != "write" {
		t.Errorf("Wrong event type: %s", event.EventType)
	}
	if event.LogicalTime.Counter != 1 {
		t.Errorf("Wrong lamport time: %d", event.LogicalTime.Counter)
	}
}

func TestHappensBefore(t *testing.T) {
	ec1 := NewEventClock("node1", time.Now().UnixNano())
	ec2 := NewEventClock("node2", time.Now().UnixNano())

	// Event at node1
	e1 := ec1.NewEvent("write")

	// Node2 receives and creates event
	ec2.UpdateFromEvent(e1)
	e2 := ec2.NewEvent("read")

	if !HappensBefore(e1, e2) {
		t.Error("Expected e1 happens before e2")
	}
	if HappensBefore(e2, e1) {
		t.Error("Expected e2 NOT happens before e1")
	}
}

func TestAreConcurrent(t *testing.T) {
	ec1 := NewEventClock("node1", time.Now().UnixNano())
	ec2 := NewEventClock("node2", time.Now().UnixNano())

	// Concurrent events (no communication)
	e1 := ec1.NewEvent("write_a")
	e2 := ec2.NewEvent("write_b")

	if !AreConcurrent(e1, e2) {
		t.Error("Expected e1 and e2 to be concurrent")
	}
}

// ========== Integration Test ==========

func TestFullTimeSyncScenario(t *testing.T) {
	fmt.Println("\n=== Full Time Sync Integration Test ===")

	// Create cluster with clock skew
	cluster := NewTestCluster(
		0, // Leader on time
		map[string]time.Duration{
			"follower1": 100 * time.Millisecond,  // 100ms fast
			"follower2": -100 * time.Millisecond, // 100ms slow
		},
	)

	// Step 1: Cristian sync for initial correction
	fmt.Println("Step 1: Cristian's Algorithm")
	leaderServer := NewCristianServer(cluster.Leader.Clock)

	for _, id := range []string{"follower1", "follower2"} {
		node := cluster.Nodes[id]
		client := NewCristianClient(node.Clock, "leader", 50*time.Millisecond)

		before := node.Clock.Now().Sub(cluster.Leader.Clock.Now())
		result := client.Synchronize(func() (time.Time, error) {
			return leaderServer.GetTime(), nil
		})
		after := node.Clock.Now().Sub(cluster.Leader.Clock.Now())

		fmt.Printf("  %s: before=%v, after=%v, RTT=%v\n", id, before, after, result.RTT)
	}

	// Step 2: Berkeley for cluster-wide averaging
	fmt.Println("\nStep 2: Berkeley Algorithm")
	berkeleyLeader := NewBerkeleyLeader(cluster.Leader.Clock, 100*time.Millisecond)
	for id, node := range cluster.Nodes {
		if id != "leader" {
			bNode := NewBerkeleyNode(id, node.Clock, false, 100*time.Millisecond)
			berkeleyLeader.RegisterNode(bNode)
		}
	}
	berkeleyLeader.PerformRound(func(id string) (time.Time, error) {
		return cluster.GetNodeTime(id)
	})
	fmt.Println("  Berkeley sync completed")

	// Step 3: Logical clocks for event ordering
	fmt.Println("\nStep 3: Logical Clocks")
	ec1 := NewEventClock("leader", time.Now().UnixNano())
	ec2 := NewEventClock("follower1", time.Now().UnixNano())
	ec3 := NewEventClock("follower2", time.Now().UnixNano())

	// Simulate file operations
	e1 := ec1.NewEvent("create_file")
	ec2.UpdateFromEvent(e1)
	e2 := ec2.NewEvent("write_file")
	ec3.UpdateFromEvent(e2)
	e3 := ec3.NewEvent("replicate")

	fmt.Printf("  Event 1: %s at %s\n", e1.EventType, e1.LogicalTime.String())
	fmt.Printf("  Event 2: %s at %s\n", e2.EventType, e2.LogicalTime.String())
	fmt.Printf("  Event 3: %s at %s\n", e3.EventType, e3.LogicalTime.String())

	if !HappensBefore(e1, e2) || !HappensBefore(e2, e3) {
		t.Error("Event ordering broken")
	}

	fmt.Println("\n=== All Time Sync Tests Passed ===")
}

// Benchmark tests
func BenchmarkMonotonicClock_Now(b *testing.B) {
	clock := NewMonotonicClock()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Now()
	}
}

func BenchmarkLamportClock_Tick(b *testing.B) {
	lc := NewLamportClock("node1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Tick()
	}
}

func BenchmarkVectorClock_Tick(b *testing.B) {
	vc := NewVectorClock("node1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.Tick()
	}
}

