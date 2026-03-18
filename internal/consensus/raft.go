package consensus

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Node states
type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// Raft node
type Raft struct {
	id             int
	state          State
	term           int
	votedFor       int
	cluster        *Cluster
	mutex          sync.Mutex
	election       chan bool
	heartbeat      chan bool
	stop           chan bool
	heartbeatCount int // Count heartbeats sent
}

// Cluster of Raft nodes
type Cluster struct {
	nodes []*Raft
	mutex sync.Mutex
}

// Create a new Raft node
func NewRaft(id int) *Raft {
	return &Raft{
		id:        id,
		state:     Follower,
		term:      0,
		votedFor:  -1,
		election:  make(chan bool),
		heartbeat: make(chan bool),
		stop:      make(chan bool),
	}
}

// Add node to cluster
func (c *Cluster) AddNode(node *Raft) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	node.cluster = c
	c.nodes = append(c.nodes, node)
}

// Start node operations
func (r *Raft) Run() {
	r.mutex.Lock()
	r.heartbeatCount = 0
	r.mutex.Unlock()
	go r.electionTimer()
	go r.listenHeartbeats()
}

// Randomized election timeout
func (r *Raft) electionTimer() {
	for {
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		select {
		case <-time.After(timeout):
			r.startElection()
		case <-r.stop:
			return
		}
	}
}

// Listen for heartbeats from leader
func (r *Raft) listenHeartbeats() {
	for {
		select {
		case <-r.heartbeat:
			r.mutex.Lock()
			r.state = Follower
			r.mutex.Unlock()
		case <-r.stop:
			return
		}
	}
}

// Start election for this node
func (r *Raft) startElection() {
	r.mutex.Lock()
	r.state = Candidate
	r.term++
	r.votedFor = r.id
	r.mutex.Unlock()

	fmt.Printf("Node %d starts election for term %d\n", r.id, r.term)
	votes := 1 // vote for self

	// Request votes from other nodes
	for _, node := range r.cluster.nodes {
		if node.id != r.id {
			if node.requestVote(r.id, r.term) {
				votes++
			}
		}
	}

	// If majority, become leader
	if votes > len(r.cluster.nodes)/2 {
		r.mutex.Lock()
		r.state = Leader
		r.mutex.Unlock()
		fmt.Printf("Node %d became leader for term %d\n", r.id, r.term)
		go r.sendHeartbeats()
	}
}

// Request vote from this node
func (r *Raft) requestVote(candidateID int, term int) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if term > r.term && (r.votedFor == -1 || r.votedFor == candidateID) {
		r.votedFor = candidateID
		r.term = term
		r.state = Follower
		return true
	}
	return false
}

// Leader sends heartbeat to all followers
func (r *Raft) sendHeartbeats() {
	for {
		r.mutex.Lock()
		if r.state != Leader {
			r.mutex.Unlock()
			return
		}
		r.heartbeatCount++ // Increment heartbeat counter
		r.mutex.Unlock()

		r.cluster.mutex.Lock()
		for _, node := range r.cluster.nodes {
			if node.id != r.id {
				node.heartbeat <- true
			}
		}
		r.cluster.mutex.Unlock()

		fmt.Printf("Leader %d sending heartbeat\n", r.id)
		time.Sleep(100 * time.Millisecond) // heartbeat interval
	}
}

// Stop the node (simulate failures)
func (r *Raft) Stop() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	select {
	case <-r.stop:
		// already closed
	default:
		close(r.stop)
	}
}