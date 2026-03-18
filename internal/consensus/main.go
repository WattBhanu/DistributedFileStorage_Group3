package main

func main() {
	node := NewRaft(1) // create node

	node.StartElection() // node starts election
	node.BecomeLeader()  // node becomes leader
	node.SendHeartbeat() // node sends heartbeat
}
