package consensus

import "time"

func RunSimulation() {
	cluster := &Cluster{}

	for i := 1; i <= 5; i++ {
		node := NewRaft(i)
		cluster.AddNode(node)
		node.Run()
	}

	time.Sleep(2 * time.Second)
}