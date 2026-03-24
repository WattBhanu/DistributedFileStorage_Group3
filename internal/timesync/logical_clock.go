package timesync

import (
	"fmt"
	"sync"
)

// LamportClock implements Lamport's logical clock for causal ordering
type LamportClock struct {
	mu      sync.RWMutex
	counter uint64
	nodeID  string
}

// NewLamportClock creates a new Lamport clock
func NewLamportClock(nodeID string) *LamportClock {
	return &LamportClock{
		counter: 0,
		nodeID:  nodeID,
	}
}

// Tick increments the clock and returns the new timestamp
// Called before sending a message or performing a local event
func (lc *LamportClock) Tick() uint64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.counter++
	return lc.counter
}

// Update adjusts the clock based on received timestamp
// Called when receiving a message: max(local, received) + 1
func (lc *LamportClock) Update(received uint64) uint64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if received > lc.counter {
		lc.counter = received
	}
	lc.counter++
	return lc.counter
}

// Current returns the current clock value without incrementing
func (lc *LamportClock) Current() uint64 {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return lc.counter
}

// LamportTimestamp represents a logical timestamp with node info
type LamportTimestamp struct {
	Counter uint64
	NodeID  string
}

// String returns string representation
func (lt LamportTimestamp) String() string {
	return fmt.Sprintf("%s:%d", lt.NodeID, lt.Counter)
}

// Compare returns -1 if lt < other, 0 if concurrent/equal, 1 if lt > other
// Note: Lamport timestamps can be concurrent (no ordering)
func (lt LamportTimestamp) Compare(other LamportTimestamp) int {
	if lt.Counter < other.Counter {
		return -1
	}
	if lt.Counter > other.Counter {
		return 1
	}
	// Same counter - use nodeID as tiebreaker for deterministic ordering
	if lt.NodeID < other.NodeID {
		return -1
	}
	if lt.NodeID > other.NodeID {
		return 1
	}
	return 0
}

// VectorClock implements vector clocks for detecting concurrency
type VectorClock struct {
	mu      sync.RWMutex
	vectors map[string]uint64
	nodeID  string
}

// NewVectorClock creates a new vector clock
func NewVectorClock(nodeID string) *VectorClock {
	vc := &VectorClock{
		vectors: make(map[string]uint64),
		nodeID:  nodeID,
	}
	vc.vectors[nodeID] = 0
	return vc
}

// Tick increments local entry and returns copy
func (vc *VectorClock) Tick() map[string]uint64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.vectors[vc.nodeID]++
	return vc.copy()
}

// Update merges received vector with local
func (vc *VectorClock) Update(received map[string]uint64) map[string]uint64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	for node, ts := range received {
		if current, ok := vc.vectors[node]; !ok || ts > current {
			vc.vectors[node] = ts
		}
	}
	vc.vectors[vc.nodeID]++
	return vc.copy()
}

// Current returns copy of current vector
func (vc *VectorClock) Current() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.copy()
}

func (vc *VectorClock) copy() map[string]uint64 {
	copy := make(map[string]uint64, len(vc.vectors))
	for k, v := range vc.vectors {
		copy[k] = v
	}
	return copy
}

// CompareVectorClocks returns -1 if vc1 < vc2, 0 if concurrent, 1 if vc1 > vc2
func CompareVectorClocks(vc1, vc2 map[string]uint64) int {
	allKeys := make(map[string]struct{})
	for k := range vc1 {
		allKeys[k] = struct{}{}
	}
	for k := range vc2 {
		allKeys[k] = struct{}{}
	}

	vc1Less := false
	vc2Less := false

	for k := range allKeys {
		v1, ok1 := vc1[k]
		v2, ok2 := vc2[k]

		if !ok1 {
			v1 = 0
		}
		if !ok2 {
			v2 = 0
		}

		if v1 < v2 {
			vc1Less = true
		} else if v1 > v2 {
			vc2Less = true
		}
	}

	if vc1Less && !vc2Less {
		return -1
	}
	if vc2Less && !vc1Less {
		return 1
	}
	if !vc1Less && !vc2Less {
		return 0 // Equal
	}
	return 0 // Concurrent
}

// TimestampedEvent represents an event with both physical and logical timestamps
type TimestampedEvent struct {
	PhysicalTime int64
	LogicalTime  LamportTimestamp
	VectorTime   map[string]uint64
	NodeID       string
	EventType    string
}

// EventClock manages event ordering for file operations
type EventClock struct {
	mu           sync.RWMutex
	lamport      *LamportClock
	vector       *VectorClock
	physicalBase int64
}

// NewEventClock creates a new event clock
func NewEventClock(nodeID string, physicalBase int64) *EventClock {
	return &EventClock{
		lamport:      NewLamportClock(nodeID),
		vector:       NewVectorClock(nodeID),
		physicalBase: physicalBase,
	}
}

// NewEvent creates a new timestamped event
func (ec *EventClock) NewEvent(eventType string) TimestampedEvent {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	lamportTs := ec.lamport.Tick()
	vectorTs := ec.vector.Tick()

	return TimestampedEvent{
		PhysicalTime: ec.physicalBase,
		LogicalTime: LamportTimestamp{
			Counter: lamportTs,
			NodeID:  ec.lamport.nodeID,
		},
		VectorTime: vectorTs,
		NodeID:     ec.lamport.nodeID,
		EventType:  eventType,
	}
}

// UpdateFromEvent updates clocks based on received event
func (ec *EventClock) UpdateFromEvent(event TimestampedEvent) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.lamport.Update(event.LogicalTime.Counter)
	ec.vector.Update(event.VectorTime)
}

// HappensBefore determines if event a happened before event b
func HappensBefore(a, b TimestampedEvent) bool {
	// Use vector clock for accurate detection
	cmp := CompareVectorClocks(a.VectorTime, b.VectorTime)
	return cmp == -1
}

// AreConcurrent checks if two events are concurrent
func AreConcurrent(a, b TimestampedEvent) bool {
	cmp := CompareVectorClocks(a.VectorTime, b.VectorTime)
	return cmp == 0 && !EqualEvents(a, b)
}

// EqualEvents checks if two events are the same
func EqualEvents(a, b TimestampedEvent) bool {
	if a.NodeID != b.NodeID {
		return false
	}
	if a.LogicalTime.Counter != b.LogicalTime.Counter {
		return false
	}
	return true
}

// GetState returns current counters for Lamport and Vector clocks
func (ec *EventClock) GetState() (uint64, map[string]uint64) {
	return ec.lamport.Current(), ec.vector.Current()
}
