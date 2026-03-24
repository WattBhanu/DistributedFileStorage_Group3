// DEPRECATED: This test file is deprecated and kept for reference only
// +build ignore
package fault

import (
	"errors"
	"testing"
)

type stubRecoverySyncer struct {
	err       error
	requested []string
}

func (s *stubRecoverySyncer) RequestSync(nodeID string) error {
	s.requested = append(s.requested, nodeID)
	return s.err
}

func TestBeginRecoveryMarksFailedNodeRecovering(t *testing.T) {
	detector := NewDetector(DefaultConfig(), stubHealthChecker{}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.mu.Lock()
	detector.nodes["node-1"].Status = Failed
	detector.mu.Unlock()

	syncer := &stubRecoverySyncer{}
	manager := NewRecoveryManager(syncer)

	if err := manager.BeginRecovery(detector, "node-1"); err != nil {
		t.Fatalf("expected recovery to start, got error: %v", err)
	}

	node, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node to exist")
	}

	if node.Status != Recovering {
		t.Fatalf("expected status %q, got %q", Recovering, node.Status)
	}

	if len(syncer.requested) != 1 || syncer.requested[0] != "node-1" {
		t.Fatalf("expected sync request for node-1, got %v", syncer.requested)
	}
}

func TestBeginRecoveryReturnsNodeToFailedWhenSyncFails(t *testing.T) {
	detector := NewDetector(DefaultConfig(), stubHealthChecker{}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.mu.Lock()
	detector.nodes["node-1"].Status = Failed
	detector.mu.Unlock()

	manager := NewRecoveryManager(&stubRecoverySyncer{err: errors.New("sync failed")})

	err := manager.BeginRecovery(detector, "node-1")
	if err == nil {
		t.Fatal("expected recovery start to fail")
	}

	node, ok := detector.GetNode("node-1")
	if !ok {
		t.Fatal("expected node to exist")
	}

	if node.Status != Failed {
		t.Fatalf("expected status %q after sync failure, got %q", Failed, node.Status)
	}
}

func TestCompleteRecoveryMarksNodeHealthy(t *testing.T) {
	detector := NewDetector(DefaultConfig(), stubHealthChecker{}, nil)
	detector.AddNode("node-1", "127.0.0.1:8080")

	detector.mu.Lock()
	detector.nodes["node-1"].Status = Recovering
	detector.nodes["node-1"].MissedHeartbeats = 3
	detector.mu.Unlock()

	manager := NewRecoveryManager(nil)

	if err := manager.CompleteRecovery(detector, "node-1"); err != nil {
		t.Fatalf("expected recovery completion to succeed, got error: %v", err)
	}

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
