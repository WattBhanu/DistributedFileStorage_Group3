package consensus

import (
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
	heartbeatCount int
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
	go r.electionTimer()
	go r.listenHeartbeats()
}

// Randomized election timeout with backoff to reduce election storms
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
	// Random backoff to reduce multiple elections at the same time
	backoff := time.Duration(rand.Intn(50)+20) * time.Millisecond
	time.Sleep(backoff)

	r.mutex.Lock()
	r.state = Candidate
	r.term++
	r.votedFor = r.id
	r.mutex.Unlock()

	votes := 1 // vote for self

	for _, node := range r.cluster.nodes {
		if node.id != r.id {
			if node.requestVote(r.id, r.term) {
				votes++
			}
		}
	}

	if votes > len(r.cluster.nodes)/2 {
		r.mutex.Lock()
		r.state = Leader
		r.mutex.Unlock()
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

// Leader sends heartbeat with adaptive interval
func (r *Raft) sendHeartbeats() {
	interval := 100 * time.Millisecond

	for {
		r.mutex.Lock()
		if r.state != Leader {
			r.mutex.Unlock()
			return
		}
		r.heartbeatCount++
		r.mutex.Unlock()

		r.cluster.mutex.Lock()
		for _, node := range r.cluster.nodes {
			if node.id != r.id {
				select {
				case node.heartbeat <- true:
				default:
					// skip if follower's channel is full to reduce blocking
				}
			}
		}
		r.cluster.mutex.Unlock()

		time.Sleep(interval)

		// Adaptive heartbeat: increase interval slightly if cluster is stable
		if r.isClusterStable() {
			interval += 10 * time.Millisecond
			if interval > 300*time.Millisecond {
				interval = 300 * time.Millisecond
			}
		} else {
			interval = 100 * time.Millisecond
		}
	}
}

// Simple check: majority of nodes are followers and alive
func (r *Raft) isClusterStable() bool {
	count := 0
	r.cluster.mutex.Lock()
	for _, node := range r.cluster.nodes {
		node.mutex.Lock()
		if node.state == Follower {
			count++
		}
		node.mutex.Unlock()
	}
	r.cluster.mutex.Unlock()
	return count >= len(r.cluster.nodes)/2
}

// Stop the node (simulate failures)
func (r *Raft) Stop() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}