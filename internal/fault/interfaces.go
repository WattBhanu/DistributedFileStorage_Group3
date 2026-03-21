package fault

type HealthChecker interface {
	Ping(addr string) error
}

type RecoverySyncer interface {
	RequestSync(nodeID string) error
}

type EventListener interface {
	OnStateChange(event FailureEvent)
}
