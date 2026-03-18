package consensus

import (
	"math/rand"
	"testing"
	"time"
)

// Extended test: network delays + multiple failures
func TestRaftPerformance(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	cluster := &Cluster{}

	// Step 1: Create 5 nodes
	for i := 1; i <= 5; i++ {
		node := NewRaft(i)
		cluster.AddNode(node)
		node.Run()
	}

	// Step 2: Random network delays in heartbeats
	t.Log("Simulating network delays...")
	for _, node := range cluster.nodes {
		go func(n *Raft) {
			for {
				time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond) // 100-600ms
				n.sendHeartbeat() // match the method in raft.go
			}
		}(node)
	}

	// Step 3: Let cluster stabilize for 2 seconds
	time.Sleep(2 * time.Second)

	// Step 4: Stop leader + a follower to simulate multiple failures
	var leader, follower *Raft
	for _, node := range cluster.nodes {
		node.mutex.Lock()
		if node.state == Leader {
			leader = node
		} else if follower == nil {
			follower = node
		}
		node.mutex.Unlock()
	}

	if leader != nil {
		t.Logf("Stopping leader Node %d", leader.id)
		leader.Stop()
	}
	if follower != nil {
		t.Logf("Stopping follower Node %d", follower.id)
		follower.Stop()
	}

	// Step 5: Let followers elect a new leader
	time.Sleep(3 * time.Second)

	// Step 6: Restart failed nodes
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

	// Step 7: Let cluster stabilize with random heartbeats
	time.Sleep(3 * time.Second)
}