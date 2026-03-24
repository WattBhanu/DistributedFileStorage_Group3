package timesync

import (
	"sync"
	"time"
)

// Clock defines the interface for time synchronization
type Clock interface {
	Now() time.Time
	Offset() time.Duration
}

// MonotonicClock provides a local monotonic clock that never goes backwards
type MonotonicClock struct {
	mu       sync.RWMutex
	baseTime time.Time
	offset   time.Duration
}

// NewMonotonicClock creates a new monotonic clock
func NewMonotonicClock() *MonotonicClock {
	return &MonotonicClock{
		baseTime: time.Now(),
		offset:   0,
	}
}

// Now returns the current adjusted time
func (m *MonotonicClock) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.baseTime.Add(time.Since(m.baseTime)).Add(m.offset)
}

// Offset returns the current time offset
func (m *MonotonicClock) Offset() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.offset
}

// AdjustOffset updates the clock offset (used by sync algorithms)
func (m *MonotonicClock) AdjustOffset(delta time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.offset += delta
}

// Elapsed returns time since clock creation
func (m *MonotonicClock) Elapsed() time.Duration {
	return time.Since(m.baseTime)
}
