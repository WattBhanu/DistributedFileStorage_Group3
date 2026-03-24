package fault

import "time"

type HealthStatus string

const (
	Healthy    HealthStatus = "HEALTHY"
	Suspected  HealthStatus = "SUSPECTED"
	Failed     HealthStatus = "FAILED"
	Recovering HealthStatus = "RECOVERING"
)

type NodeHealth struct {
	NodeID           string
	Address          string
	Status           HealthStatus
	LastHeartbeat    time.Time
	MissedHeartbeats int
	LastChanged      time.Time
}

type FailureEvent struct {
	NodeID    string
	OldStatus HealthStatus
	NewStatus HealthStatus
	Reason    string
	At        time.Time
}

type RecoveryState struct {
	NodeID      string
	Status      HealthStatus
	NeedsSync   bool
	RecoveredAt time.Time
}
