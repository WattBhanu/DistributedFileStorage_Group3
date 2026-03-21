package fault

import "time"

type DetectorConfig struct {
	HeartbeatInterval time.Duration
	SuspectTimeout    time.Duration
	FailureTimeout    time.Duration
	RecoveryTimeout   time.Duration
	MaxMissedBeats    int
}

func DefaultConfig() DetectorConfig {
	return DetectorConfig{
		HeartbeatInterval: 2 * time.Second,
		SuspectTimeout:    4 * time.Second,
		FailureTimeout:    8 * time.Second,
		RecoveryTimeout:   10 * time.Second,
		MaxMissedBeats:    2,
	}
}
