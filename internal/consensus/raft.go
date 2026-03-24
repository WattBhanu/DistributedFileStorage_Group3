package consensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Node states
type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// Raft node
type Raft struct {
	id             int
	state          State
	term           int
	votedFor       int
	cluster        *Cluster
	peers          map[string]string // nodeID -> address (for networked Raft)
	httpClient     *http.Client
	mutex          sync.Mutex
	election       chan bool
	heartbeat      chan bool
	stop           chan bool
	heartbeatCount int
	knownLeader    string // Track who the current leader is
	leaderTerm     int    // Track the leader's current term (not just follower's term)
}

// Cluster of Raft nodes
type Cluster struct {
	nodes []*Raft
	mutex sync.Mutex
}

// Create a new Raft node with peer addresses
func NewRaft(id int, peers map[string]string) *Raft {
	return &Raft{
		id:        id,
		state:     Follower,
		term:      0,
		votedFor:  -1,
		peers:     peers,
		httpClient: &http.Client{Timeout: 2 * time.Second},
		election:  make(chan bool),
		heartbeat: make(chan bool),
		stop:      make(chan bool),
		knownLeader: "", // Don't know leader yet
		leaderTerm:  0,  // No leader term yet
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
	log.Printf("[RAFT] Raft consensus initialized for node %d", r.id)
	go r.electionTimer()
	go r.listenHeartbeats()
	log.Printf("[RAFT] Election timer and heartbeat listener started")
}

// Randomized election timeout with backoff to reduce election storms
func (r *Raft) electionTimer() {
	log.Printf("[RAFT] [NODE %d] Election timer started with timeout range 300-800ms", r.id)
	
	for {
		// Election timeout: if no heartbeat received within this time, start election
		timeout := time.Duration(300+rand.Intn(500)) * time.Millisecond
		
		select {
		case <-time.After(timeout):
			// No heartbeat received within timeout - start election
			r.mutex.Lock()
			if r.state != Leader { // Don't start election if already leader
				r.mutex.Unlock()
				log.Printf("[RAFT] [NODE %d] Election timeout: no heartbeat received, starting election", r.id)
				r.startElection()
			} else {
				r.mutex.Unlock()
			}
		case <-r.heartbeat:
			// Heartbeat received - reset timer (do nothing, loop will restart timer)
			log.Printf("[RAFT] [NODE %d] Heartbeat received, resetting election timer", r.id)
		case <-r.stop:
			log.Printf("[RAFT] [NODE %d] Election timer stopping", r.id)
			return
		}
	}
}

// Listen for heartbeats from leader
func (r *Raft) listenHeartbeats() {
	log.Printf("[RAFT] Heartbeat listener started")
	for {
		select {
		case <-r.heartbeat:
			r.mutex.Lock()
			r.state = Follower
			r.mutex.Unlock()
			log.Printf("[RAFT] ♡ Received heartbeat from leader")
		case <-r.stop:
			log.Printf("[RAFT] Heartbeat listener stopping")
			return
		}
	}
}

// Start election for this node
func (r *Raft) startElection() {
	log.Printf("[RAFT] [NODE %d] Starting election - transitioning to CANDIDATE state", r.id)
	// Random backoff to reduce multiple elections at the same time
	backoff := time.Duration(rand.Intn(100)+50) * time.Millisecond
	time.Sleep(backoff)

	r.mutex.Lock()
	r.state = Candidate
	r.term++
	r.votedFor = r.id
	r.leaderTerm = r.term // When candidate, track own term
	log.Printf("[RAFT] [NODE %d] Term %d: Self-voted as candidate", r.id, r.term)
	r.mutex.Unlock()

	votes := 1 // vote for self

	// Request votes from peers via HTTP
	for peerID, peerAddr := range r.peers {
		if peerID != fmt.Sprintf("node%d", r.id) {
			log.Printf("[RAFT] [NODE %d] Requesting vote from peer %s at %s", r.id, peerID, peerAddr)
			if r.requestVoteHTTP(peerAddr, r.id, r.term) {
				votes++
				log.Printf("[RAFT] [NODE %d] Received vote from peer %s (total: %d)", r.id, peerID, votes)
			} else {
				log.Printf("[RAFT] [NODE %d] Vote rejected by peer %s", r.id, peerID)
			}
		}
	}

	totalNodes := len(r.peers) + 1  // self + peers
	needed := totalNodes/2 + 1      // Need majority (>50%)
	if votes >= needed {
		r.mutex.Lock()
		r.state = Leader
		r.leaderTerm = r.term // As leader, track current term
		r.mutex.Unlock()
		log.Printf("[RAFT] [NODE %d] ✓ ELECTED as LEADER with %d/%d votes", r.id, votes, totalNodes)
		go r.sendHeartbeatsNetwork()
	} else {
		log.Printf("[RAFT] [NODE %d] Election failed - only got %d/%d votes (needed %d)", r.id, votes, totalNodes, needed)
	}
}

// Request vote from this node (deprecated - use HandleVoteRequest instead)
func (r *Raft) requestVote(candidateID int, term int) bool {
	// This is now handled via HTTP in networked Raft
	log.Printf("[RAFT] Direct requestVote call deprecated - using HTTP RPC")
	return false
}

// Leader sends heartbeat with adaptive interval (networked version)
func (r *Raft) sendHeartbeatsNetwork() {
	log.Printf("[RAFT] [NODE %d] ✓ ELECTED - Starting heartbeat transmission to followers via HTTP", r.id)
	interval := 50 * time.Millisecond  // Faster initial heartbeat to establish leadership
	heartbeatCount := 0
	failedHeartbeats := 0
	maxFailedHeartbeats := 10  // After 10 failed heartbeats (~1.5s), trigger re-election

	for {
		r.mutex.Lock()
		if r.state != Leader {
			r.mutex.Unlock()
			log.Printf("[RAFT] [NODE %d] Stopping heartbeats - no longer leader (sent %d heartbeats)", r.id, heartbeatCount)
			return
		}
		r.heartbeatCount++
		heartbeatCount++
		r.mutex.Unlock()

		heartbeatSent := 0
		// Send heartbeats to all peers via HTTP
		for peerID, peerAddr := range r.peers {
			if r.sendHeartbeatHTTP(peerAddr, r.id, r.term) {
				heartbeatSent++
				log.Printf("[RAFT] [NODE %d] ♥ Heartbeat #%d sent to peer %s", r.id, heartbeatCount, peerID)
			} else {
				log.Printf("[RAFT] [NODE %d] ✗ Heartbeat #%d failed to peer %s", r.id, heartbeatCount, peerID)
			}
		}

		if heartbeatSent > 0 {
			log.Printf("[RAFT] [NODE %d] ♥ Heartbeat #%d sent to %d peers", r.id, heartbeatCount, heartbeatSent)
			failedHeartbeats = 0 // Reset on successful send
		} else {
			log.Printf("[RAFT] [NODE %d] ♥ Heartbeat #%d (no peers in cluster)", r.id, heartbeatCount)
		}

		time.Sleep(interval)

		// Adaptive heartbeat: stabilize at 150ms for better cluster stability
		if heartbeatSent == len(r.peers) {
			interval = 150 * time.Millisecond  // Stable interval
		} else {
			interval = 50 * time.Millisecond  // Try faster if failing
			failedHeartbeats++
			
			// If too many heartbeats failed, step down and trigger re-election
			if failedHeartbeats >= maxFailedHeartbeats && len(r.peers) > 0 {
				log.Printf("[RAFT] [NODE %d] Lost quorum - %d consecutive failed heartbeats, stepping down", r.id, failedHeartbeats)
				r.mutex.Lock()
				r.state = Follower
				r.mutex.Unlock()
				// Don't return - let election timer pick up and start new election
				return
			}
		}
	}
}


// GetState returns the current Raft node state (LEADER, FOLLOWER, or CANDIDATE)
func (r *Raft) GetState() State {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.state
}

// GetTerm returns the current Raft term
func (r *Raft) GetTerm() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.term
}

// IsLeader returns true if this node is the Raft leader
func (r *Raft) IsLeader() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.state == Leader
}

// GetKnownLeader returns the node ID of the known leader (if follower)
func (r *Raft) GetKnownLeader() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.state == Leader {
		return fmt.Sprintf("node%d", r.id)
	}
	return r.knownLeader
}

// GetLeaderTerm returns the current leader's term (cluster-wide term)
func (r *Raft) GetLeaderTerm() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.state == Leader {
		return r.term // This node is leader, return its term
	}
	return r.leaderTerm // Follower returns the leader's term it knows about
}

// requestVoteHTTP sends vote request to peer via HTTP
func (r *Raft) requestVoteHTTP(peerAddr string, candidateID int, term int) bool {
	if peerAddr == "" {
		return false
	}
	
	reqBody := map[string]interface{}{
		"Term":        term,
		"CandidateID": candidateID,
		"LastLogIndex": 0,
		"LastLogTerm":  0,
	}
	
	url := fmt.Sprintf("http://%s/internal/request-vote", peerAddr)
	jsonData, _ := json.Marshal(reqBody)
	
	resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("[RAFT] Vote request to %s failed: %v", peerAddr, err)
		return false
	}
	defer resp.Body.Close()
	
	var voteResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		log.Printf("[RAFT] Vote response decode failed: %v", err)
		return false
	}
	
	granted, ok := voteResp["VoteGranted"].(bool)
	return ok && granted
}

// sendHeartbeatHTTP sends heartbeat to peer via HTTP
func (r *Raft) sendHeartbeatHTTP(peerAddr string, leaderID int, term int) bool {
	if peerAddr == "" {
		return false
	}
	
	reqBody := map[string]interface{}{
		"Term":         term,
		"LeaderID":     fmt.Sprintf("node%d", leaderID), // Send as "nodeX" format
		"Entries":      []interface{}{},
		"LeaderCommit": 0,
		"LeaderTerm":   term, // Explicitly send leader's current term
	}
	
	url := fmt.Sprintf("http://%s/internal/append-entries", peerAddr)
	jsonData, _ := json.Marshal(reqBody)
	
	resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("[RAFT] Heartbeat to %s failed: %v", peerAddr, err)
		return false
	}
	defer resp.Body.Close()
	
	var hbResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&hbResp); err != nil {
		log.Printf("[RAFT] Heartbeat response decode failed: %v", err)
		return false
	}
	
	success, ok := hbResp["Success"].(bool)
	return ok && success
}

// HandleVoteRequest processes incoming vote request from candidate
func (r *Raft) HandleVoteRequest(candidateID string, term int) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	cid := 0
	fmt.Sscanf(candidateID, "%d", &cid)
	
	// Grant vote if:
	// 1. Candidate's term is higher than our term, OR
	// 2. Same term and we haven't voted yet, OR
	// 3. Same term and we already voted for this candidate
	if term > r.term {
		// Higher term - update our term and vote for candidate
		r.votedFor = cid
		r.term = term
		r.leaderTerm = term // Track the new term
		r.state = Follower
		log.Printf("[RAFT] [NODE %d] Voted for node %s in term %d (higher term)", r.id, candidateID, term)
		return true
	} else if term == r.term && (r.votedFor == -1 || r.votedFor == cid) {
		// Same term - vote if we haven't voted or already voted for this candidate
		if r.votedFor == -1 {
			r.votedFor = cid
			log.Printf("[RAFT] [NODE %d] Voted for node %s in term %d (first vote)", r.id, candidateID, term)
		} else {
			log.Printf("[RAFT] [NODE %d] Re-voting for node %s in term %d", r.id, candidateID, term)
		}
		r.state = Follower
		r.leaderTerm = term // Track the term
		return true
	}
	
	log.Printf("[RAFT] [NODE %d] Rejected vote for node %s (term %d, my term %d, voted %d)", r.id, candidateID, term, r.term, r.votedFor)
	return false
}

// HandleHeartbeat processes incoming heartbeat from leader
func (r *Raft) HandleHeartbeat(leaderID string, term int) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	if term >= r.term {
		r.state = Follower
		r.term = term
		r.knownLeader = leaderID // Remember who sent the heartbeat
		r.leaderTerm = term      // Remember the leader's current term
		log.Printf("[RAFT] ♡ Received heartbeat from leader %s (term %d)", leaderID, term)
		
		// Signal heartbeat received to reset election timer
		select {
		case r.heartbeat <- true:
			// Heartbeat signal sent
		default:
			// Channel full, ignore (non-blocking)
		}
		
		return true
	}
	return false
}

// Stop the node (simulate failures)
func (r *Raft) Stop() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}