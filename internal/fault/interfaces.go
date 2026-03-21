package fault

type HealthChecker interface {
	Ping(addr string) error
}

type EventListener interface {
	OnStateChange(event FailureEvent)
}
