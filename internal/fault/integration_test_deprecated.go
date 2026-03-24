// DEPRECATED: This test file is deprecated and kept for reference only
// +build ignore
package fault

import (
	"errors"
	"testing"
	"time"
)

func TestDetectorHandlesSingleNodeFailureInCluster(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMissedBeats = 1

	detector := NewDetector(cfg, stubHealthChecker{err: errors.New("node unreachable")}, nil)
	detector.AddNode("node-1", "127.0.0.1:8081")
	detector.AddNode("node-2", "127.0.0.1:8082")
	detector.AddNode("node-3", "127.0.0.1:8083")

	detector.CheckNode("node-2")

	node, ok := detector.GetNode("node-2")
	if !ok {
		t.Fatal("expected node-2 to exist")
	}

	if node.Status != Suspected {
		t.Fatalf("expected node-2 to become %q, got %q", Suspected, node.Status)
	}
}

func TestDetectorRecoversNodeAfterSuccessfulSync(t *testing.T) {
	detector := NewDetector(DefaultConfig(), stubHealthChecker{}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.mu.Lock()
	detector.nodes["node-1"].Status = Failed
	detector.mu.Unlock()

	manager := NewRecoveryManager(&stubRecoverySyncer{})

	if err := manager.BeginRecovery(detector, "node-1"); err != nil {
		t.Fatalf("expected recovery to begin, got error: %v", err)
	}

	if err := manager.CompleteRecovery(detector, "node-1"); err != nil {
		t.Fatalf("expected recovery to complete, got error: %v", err)
	}

	node, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node-1 to exist")
	}

	if node.Status != Healthy {
		t.Fatalf("expected node-1 to become %q, got %q", Healthy, node.Status)
	}
}

func TestTemporaryDelayDoesNotForceImmediateFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMissedBeats = 3
	cfg.FailureTimeout = 30 * time.Second

	detector := NewDetector(cfg, stubHealthChecker{err: errors.New("temporary timeout")}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.CheckNode("node-1")

	node, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node-1 to exist")
	}

	if node.Status == Failed {
		t.Fatalf("expected temporary delay to avoid %q state", Failed)
	}
}
