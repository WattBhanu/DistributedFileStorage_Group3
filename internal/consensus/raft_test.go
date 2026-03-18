package consensus

import (
	"testing"
	"time"
)

// Simulation test to see multi-node Raft in action
func TestSimulation(t *testing.T) {
	cluster := &Cluster{}

	// Create 5 nodes
	for i := 1; i <= 5; i++ {
		node := NewRaft(i)
		cluster.AddNode(node)
		node.Run()
	}

	// Let the simulation run for 2 seconds
	time.Sleep(2 * time.Second)
}