package fault

import (
	"errors"
	"time"
)

type RecoveryManager struct {
	syncer RecoverySyncer
}

func NewRecoveryManager(syncer RecoverySyncer) *RecoveryManager {
	return &RecoveryManager{syncer: syncer}
}

func (r *RecoveryManager) BeginRecovery(detector *Detector, nodeID string) error {
	if detector == nil {
		return errors.New("detector is required")
	}

	detector.mu.Lock()
	node, ok := detector.nodes[nodeID]
	if !ok {
		detector.mu.Unlock()
		return errors.New("node not found")
	}

	if node.Status != Failed {
		detector.mu.Unlock()
		return errors.New("node is not in failed state")
	}

	now := time.Now()
	detector.updateStatusLocked(node, Recovering, "node is rejoining and waiting for sync", now)
	detector.mu.Unlock()

	if r.syncer == nil {
		return nil
	}

	if err := r.syncer.RequestSync(nodeID); err != nil {
		detector.mu.Lock()
		if currentNode, ok := detector.nodes[nodeID]; ok {
			detector.updateStatusLocked(currentNode, Failed, "recovery sync failed", time.Now())
		}
		detector.mu.Unlock()
		return err
	}

	return nil
}

func (r *RecoveryManager) CompleteRecovery(detector *Detector, nodeID string) error {
	if detector == nil {
		return errors.New("detector is required")
	}

	detector.mu.Lock()
	defer detector.mu.Unlock()

	node, ok := detector.nodes[nodeID]
	if !ok {
		return errors.New("node not found")
	}

	if node.Status != Recovering {
		return errors.New("node is not recovering")
	}

	now := time.Now()
	node.LastHeartbeat = now
	node.MissedHeartbeats = 0
	detector.updateStatusLocked(node, Healthy, "node recovery completed", now)

	return nil
}
