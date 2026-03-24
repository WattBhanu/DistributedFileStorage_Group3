package timesync

import (
	"fmt"
	"sync"
	"time"
)

// CristianClient implements Cristian's algorithm for time synchronization
// It synchronizes with a reference server (leader) to estimate network delay
type CristianClient struct {
	mu           sync.RWMutex
	clock        *MonotonicClock
	serverNodeID string
	rtt          time.Duration
	offset       time.Duration
	syncInterval time.Duration
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewCristianClient creates a new Cristian algorithm client
func NewCristianClient(clock *MonotonicClock, serverNodeID string, syncInterval time.Duration) *CristianClient {
	return &CristianClient{
		clock:        clock,
		serverNodeID: serverNodeID,
		syncInterval: syncInterval,
		rtt:          0,
		offset:       0,
		stopCh:       make(chan struct{}),
	}
}

// SyncResult contains the result of a synchronization attempt
type SyncResult struct {
	Success   bool
	Offset    time.Duration
	RTT       time.Duration
	Timestamp time.Time
	Error     error
}

// Synchronize performs a single Cristian synchronization with the server
// In real implementation, this would make HTTP request to server
// For simulation, we use a provided time function
func (c *CristianClient) Synchronize(getServerTime func() (time.Time, error)) SyncResult {
	t0 := c.clock.Now()
	serverTime, err := getServerTime()
	t1 := c.clock.Now()

	if err != nil {
		return SyncResult{
			Success:   false,
			Timestamp: t0,
			Error:     fmt.Errorf("failed to get server time: %w", err),
		}
	}

	// Calculate round-trip time
	rtt := t1.Sub(t0)

	// Estimate one-way delay (assuming symmetric)
	delay := rtt / 2

	// Calculate offset: serverTime + delay - localTime
	localTime := t1
	estimatedServerTime := serverTime.Add(delay)
	offset := estimatedServerTime.Sub(localTime)

	// Apply the offset to our clock
	c.clock.AdjustOffset(offset)

	c.mu.Lock()
	c.rtt = rtt
	c.offset = offset
	c.mu.Unlock()

	return SyncResult{
		Success:   true,
		Offset:    offset,
		RTT:       rtt,
		Timestamp: estimatedServerTime,
	}
}

// Start begins periodic synchronization
func (c *CristianClient) Start(getServerTime func() (time.Time, error)) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.syncInterval)
		defer ticker.Stop()

		// Initial sync
		c.Synchronize(getServerTime)

		for {
			select {
			case <-ticker.C:
				c.Synchronize(getServerTime)
			case <-c.stopCh:
				return
			}
		}
	}()
}

// Stop halts periodic synchronization
func (c *CristianClient) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// GetRTT returns the last measured round-trip time
func (c *CristianClient) GetRTT() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rtt
}

// GetOffset returns the last applied offset calculation
func (c *CristianClient) GetOffset() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.offset
}

// CristianServer represents the server side of Cristian's algorithm
type CristianServer struct {
	clock *MonotonicClock
}

// NewCristianServer creates a new Cristian server
func NewCristianServer(clock *MonotonicClock) *CristianServer {
	return &CristianServer{clock: clock}
}

// GetTime returns the current server time
func (s *CristianServer) GetTime() time.Time {
	return s.clock.Now()
}
