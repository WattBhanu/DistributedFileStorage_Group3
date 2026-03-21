package fault

import "time"

func (d *Detector) RecordHeartbeat(nodeID string, at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	node, ok := d.nodes[nodeID]
	if !ok {
		return
	}

	node.LastHeartbeat = at
	node.MissedHeartbeats = 0
}

func (d *Detector) MissedHeartbeat(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	node, ok := d.nodes[nodeID]
	if !ok {
		return
	}

	node.MissedHeartbeats++
}

func (d *Detector) LastSeen(nodeID string) (time.Time, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	node, ok := d.nodes[nodeID]
	if !ok {
		return time.Time{}, false
	}

	return node.LastHeartbeat, true
}
