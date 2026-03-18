package main

import "fmt"

// Node states
type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// Raft node structure
type Raft struct {
	id       int
	state    State
	term     int
	votedFor int
}

// Constructor
func NewRaft(id int) *Raft {
	return &Raft{
		id:       id,
		state:    Follower,
		term:     0,
		votedFor: -1,
	}
}

// Start an election (candidate)
func (r *Raft) StartElection() {
	r.state = Candidate
	r.term++
	r.votedFor = r.id
	fmt.Println("Node", r.id, "started election for term", r.term)
}

// Request vote from another node (simplified)
func (r *Raft) RequestVote(candidateID int, term int) bool {
	if term > r.term || r.votedFor == -1 {
		r.term = term
		r.votedFor = candidateID
		r.state = Follower
		return true
	}
	return false
}

// Become leader
func (r *Raft) BecomeLeader() {
	r.state = Leader
	fmt.Println("Node", r.id, "became leader")
}

// Send heartbeat (only leader)
func (r *Raft) SendHeartbeat() {
	if r.state == Leader {
		fmt.Println("Leader", r.id, "sending heartbeat")
	}
}