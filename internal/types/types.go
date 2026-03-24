package types

type NodeState string

const (
	Follower  NodeState = "FOLLOWER"
	Candidate NodeState = "CANDIDATE"
	Leader    NodeState = "LEADER"
	Dead      NodeState = "DEAD"
)

type LogEntry struct {
	Term      int64
	Index     int64
	Timestamp int64
	Op        string
	Filename  string
	Data      []byte
	Checksum  string
}

type NodeInfo struct {
	ID       string
	Addr     string
	State    NodeState
	Term     int64
	LogIndex int64
	Alive    bool
}

type FileMetadata struct {
	Filename  string
	Size      int64
	CreatedAt int64
	Nodes     []string
}

type VoteRequest struct {
	Term         int64
	CandidateID  string
	LastLogIndex int64
	LastLogTerm  int64
}

type VoteResponse struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     string
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesResponse struct {
	Term    int64
	Success bool
}

// Replication Protocol Types
type ReplicateRequest struct {
	Filename  string
	Data      []byte
	Timestamp int64
	Checksum  string
	Version   int64
	NodeID    string
	Operation string
	Op        string
}

type ReplicateResponse struct {
	Filename string
	NodeID   string
	Success  bool
	Checksum string
	Error    string
}

// Consistency Verification Types
type SyncRequest struct {
	NodeID   string
	Filename string
}

type SyncResponse struct {
	Filename string
	Found    bool
	Checksum string
	Version  int64
}
