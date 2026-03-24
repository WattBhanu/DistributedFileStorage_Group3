package fault

import (
	"errors"
	"testing"
	"time"
)

type stubHealthChecker struct {
	err error
}

func (s stubHealthChecker) Ping(addr string) error {
	return s.err
}

func TestCheckNodeKeepsHealthyNodeHealthy(t *testing.T) {
	detector := NewDetector(DefaultConfig(), stubHealthChecker{}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.CheckNode("node-1")

	node, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node to exist")
	}

	if node.Status != Healthy {
		t.Fatalf("expected status %q, got %q", Healthy, node.Status)
	}

	if node.MissedHeartbeats != 0 {
		t.Fatalf("expected missed heartbeats to reset, got %d", node.MissedHeartbeats)
	}
}

func TestCheckNodeMarksNodeSuspectedAfterMissedHeartbeats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMissedBeats = 1

	detector := NewDetector(cfg, stubHealthChecker{err: errors.New("node unreachable")}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.CheckNode("node-1")

	node, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node to exist")
	}

	if node.Status != Suspected {
		t.Fatalf("expected status %q, got %q", Suspected, node.Status)
	}

	if node.MissedHeartbeats != 1 {
		t.Fatalf("expected 1 missed heartbeat, got %d", node.MissedHeartbeats)
	}
}

func TestCheckNodeMarksNodeFailedAfterFailureTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMissedBeats = 1
	cfg.FailureTimeout = 5 * time.Second

	detector := NewDetector(cfg, stubHealthChecker{err: errors.New("node unreachable")}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	node, ok := detector.nodes["node-1"]
	if !ok {
		t.Fatal("expected node to exist")
	}

	now := time.Now()
	node.Status = Suspected
	node.LastHeartbeat = now.Add(-10 * time.Second)
	node.LastChanged = now.Add(-6 * time.Second)

	detector.CheckNode("node-1")

	updated, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node to exist")
	}

	if updated.Status != Failed {
		t.Fatalf("expected status %q, got %q", Failed, updated.Status)
	}
}
