package consensus

import (
	"testing"
	"time"
)

// Extended simulation to test node failures and recovery
func TestNodeFailures(t *testing.T) {
	cluster := &Cluster{}

	// Create 5 nodes
	for i := 1; i <= 5; i++ {
		node := NewRaft(i)
		cluster.AddNode(node)
		node.Run()
	}

	// Let cluster run initially
	time.Sleep(1 * time.Second)

	// Step 1: Detect current leader
	var leader *Raft
	for _, node := range cluster.nodes {
		node.mutex.Lock()
		if node.state == Leader {
			leader = node
		}
		node.mutex.Unlock()
	}

	if leader != nil {
		t.Logf("Stopping leader Node %d", leader.id)
		leader.Stop() // simulate leader failure
	}

	// Step 2: Let followers elect a new leader
	time.Sleep(2 * time.Second)

	// Step 3: Optional - stop a follower to simulate multiple failures
	follower := cluster.nodes[0]
	if follower != leader {
		t.Logf("Stopping follower Node %d", follower.id)
		follower.Stop()
	}

	// Step 4: Let cluster stabilize
	time.Sleep(2 * time.Second)

	// Step 5: Restart failed nodes to simulate recovery
	if leader != nil {
		t.Logf("Restarting former leader Node %d", leader.id)
		newLeader := NewRaft(leader.id)
		cluster.AddNode(newLeader)
		newLeader.Run()
	}

	if follower != nil {
		t.Logf("Restarting former follower Node %d", follower.id)
		newFollower := NewRaft(follower.id)
		cluster.AddNode(newFollower)
		newFollower.Run()
	}

	// Step 6: Let cluster stabilize
	time.Sleep(2 * time.Second)
}