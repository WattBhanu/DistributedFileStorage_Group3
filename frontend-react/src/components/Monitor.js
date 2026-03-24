import React, { useState, useEffect } from 'react';
import './Monitor.css';

const API_BASE = 'http://localhost:8080';

function Monitor() {
  const [selectedNode, setSelectedNode] = useState('node1');
  const [apiBase, setApiBase] = useState(API_BASE);
  const [leaderPort, setLeaderPort] = useState('8080'); // Track actual leader port
  const [metrics, setMetrics] = useState({
    consensus: { state: '-', leader: '-', term: 0 },
    replication: { files_replicated: 0, peer_count: 0, replica_status: [] },
    fault: { healthy_nodes: 0, suspected_nodes: 0, failed_nodes: 0, node_states: [] },
    time_sync: { 
      last_sync: 0, 
      protocol: '-', 
      clock_offset: 0,
      is_coordinator: false,
      sync_interval: 10,
      peers_synced: 0,
      peer_details: [],
      cristian_offset: 0,
      cristian_rtt: 0,
      lamport_counter: 0,
      vector_clock: {}
    }
  });
  const [lastUpdate, setLastUpdate] = useState(new Date());

  useEffect(() => {
    console.log('useEffect triggered - apiBase:', apiBase);
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 2000); // Update every 2s
    return () => clearInterval(interval);
  }, [apiBase]);

  const handleNodeChange = (e) => {
    const url = e.target.value;
    console.log('Dropdown changed:', { url });
    setSelectedNode(url); // Just use URL as identifier
    setApiBase(url);
  };

  // Simple node name for dropdown options (no state)
  const getNodeName = (url) => {
    const port = url.split(':').pop();
    // Extract node number from port (8080->1, 8081->2, 8082->3)
    const nodeNum = parseInt(port.replace('808', '')) + 1;
    return `Node ${nodeNum}`;
  };

  // Full label with role for currently selected node
  const getNodeLabel = (url, state) => {
    const port = url.split(':').pop();
    const nodeNum = parseInt(port.replace('808', '')) + 1;
    const nodeName = `Node ${nodeNum}`;
    if (state === 'Leader') {
      return `${nodeName} (LEADER)`;
    } else if (state === 'Follower') {
      return `${nodeName} (FOLLOWER)`;
    } else {
      return `${nodeName} (${state || 'unknown'})`;
    }
  };

  const fetchMetrics = async () => {
    try {
      console.log(`Fetching from: ${apiBase}/api/metrics`);
      const response = await fetch(`${apiBase}/api/metrics`);
      const data = await response.json();
      console.log('Received metrics:', data);
      setMetrics(data);
      setLastUpdate(new Date());
      
      // Query ALL nodes to find the actual leader (not just the selected node)
      const nodes = ['http://localhost:8080', 'http://localhost:8081', 'http://localhost:8082'];
      let foundLeaderPort = null;
      
      for (const nodeUrl of nodes) {
        try {
          const nodeResponse = await fetch(`${nodeUrl}/api/metrics`);
          if (nodeResponse.ok) {
            const nodeData = await nodeResponse.json();
            // If this node is the leader, update leaderPort
            if (nodeData.consensus && nodeData.consensus.state === 'Leader') {
              const port = nodeUrl.split(':').pop();
              foundLeaderPort = port;
              console.log(`Leader found: ${nodeUrl} (port ${port})`);
              break;
            }
            // Or if this node knows who the leader is
            if (nodeData.consensus && nodeData.consensus.leader && nodeData.consensus.leader !== 'unknown') {
              const leaderMap = {
                'node1': '8080',
                'node2': '8081',
                'node3': '8082'
              };
              if (leaderMap[nodeData.consensus.leader]) {
                foundLeaderPort = leaderMap[nodeData.consensus.leader];
                console.log(`Leader known: ${nodeData.consensus.leader} (port ${foundLeaderPort})`);
                break;
              }
            }
          }
        } catch (error) {
          // Skip offline nodes
        }
      }
      
      // Update leader port
      if (foundLeaderPort) {
        setLeaderPort(foundLeaderPort);
      }
    } catch (error) {
      console.error('Failed to fetch metrics:', error);
    }
  };

  return (
    <div className="monitor-dashboard">
      <header className="monitor-header">
        <h1>📊 Real-Time Algorithm Monitor</h1>
        <div className="monitor-status">
          <span className="status-indicator">● Monitoring - Leader: <strong>localhost:{leaderPort}</strong></span>
          <span>Last update: {lastUpdate.toLocaleTimeString()}</span>
          <select 
            key={apiBase}
            value={apiBase} 
            onChange={handleNodeChange}
            className="node-selector"
          >
            {/* Show full label with role for selected node, simple name for others */}
            <option value="http://localhost:8080">
              {apiBase === 'http://localhost:8080' ? getNodeLabel('http://localhost:8080', metrics.consensus.state) : getNodeName('http://localhost:8080')}
            </option>
            <option value="http://localhost:8081">
              {apiBase === 'http://localhost:8081' ? getNodeLabel('http://localhost:8081', metrics.consensus.state) : getNodeName('http://localhost:8081')}
            </option>
            <option value="http://localhost:8082">
              {apiBase === 'http://localhost:8082' ? getNodeLabel('http://localhost:8082', metrics.consensus.state) : getNodeName('http://localhost:8082')}
            </option>
          </select>
        </div>
      </header>

      <div className="dashboard-grid">
        {/* Consensus Panel */}
        <section className="monitor-panel consensus-panel">
          <div className="panel-header">
            <h2>⚙️ Consensus (Raft)</h2>
          </div>
          <div className="panel-content">
            <div className="metrics-grid">
              <MetricCard label="Raft State" value={metrics.consensus.state}>
                <span className={`state-indicator ${metrics.consensus.state.toLowerCase()}`}></span>
              </MetricCard>
              <MetricCard label="Leader Node" value={metrics.consensus.leader} />
              <MetricCard label="Term" value={metrics.consensus.term || '-'} />
            </div>
          </div>
        </section>

        {/* Replication Panel */}
        <section className="monitor-panel replication-panel">
          <div className="panel-header">
            <h2>📋 Replication</h2>
          </div>
          <div className="panel-content">
            <div className="metrics-grid">
              <MetricCard label="Files Stored" value={metrics.replication.files_replicated} />
              <MetricCard label="Known Peers" value={metrics.replication.peer_count} />
              {metrics.replication.replica_status && metrics.replication.replica_status.length > 0 && (
                <div className="metric-card full-width">
                  <div className="metric-label">Peer Nodes</div>
                  <div className="peer-list">
                    {metrics.replication.replica_status.map((peer, idx) => (
                      <div key={idx} className="peer-item">
                        <span className="peer-name">{peer.node_id}</span>
                        <span className="peer-address">{peer.address}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </section>

        {/* Fault Tolerance Panel */}
        <section className="monitor-panel fault-panel">
          <div className="panel-header">
            <h2>🛡️ Fault Tolerance</h2>
          </div>
          <div className="panel-content">
            <div className="metrics-grid">
              <MetricCard 
                label="Healthy Nodes" 
                value={metrics.fault.healthy_nodes}
                color="success"
              />
              <MetricCard 
                label="Suspected Nodes" 
                value={metrics.fault.suspected_nodes}
                color="warning"
              />
              <MetricCard 
                label="Failed Nodes" 
                value={metrics.fault.failed_nodes}
                color="danger"
              />
              {metrics.fault.node_states && metrics.fault.node_states.length > 0 && (
                <div className="metric-card full-width">
                  <div className="metric-label">Node Status Details</div>
                  <div className="node-status-list">
                    {metrics.fault.node_states.map((node, idx) => (
                      <div key={idx} className={`node-status-item ${node.status}`}>
                        <span className="node-name">{node.node_id}</span>
                        <span className="node-status-badge">{node.status.toUpperCase()}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </section>

        {/* Time Sync Panel */}
        <section className="monitor-panel timesync-panel">
          <div className="panel-header">
            <h2>🕐 Time Synchronization (Berkeley)</h2>
          </div>
          <div className="panel-content">
            <div className="metrics-grid">
              <MetricCard 
                label="Protocol" 
                value={metrics.time_sync.protocol || 'Berkeley'}
              />
              <MetricCard 
                label="Role"
                value={metrics.time_sync?.is_coordinator ? '👑 Coordinator' : '📡 Follower'}
                color={metrics.time_sync?.is_coordinator ? 'success' : ''}
              />
              <MetricCard 
                label="Clock Offset (µs)" 
                value={(() => {
                  if (metrics.time_sync?.peers_synced === 0) return 'No peers';
                  const offset = metrics.time_sync?.clock_offset ?? 0;
                  const sign = offset > 0 ? '+' : '';
                  return `${sign}${offset.toFixed(1)}µs`;
                })()}
                color={
                  metrics.time_sync?.peers_synced === 0 ? '' :
                  Math.abs(metrics.time_sync?.clock_offset ?? 0) < 1000 ? 'success' :
                  Math.abs(metrics.time_sync?.clock_offset ?? 0) < 50000 ? 'warning' : 'danger'
                }
              />
              <MetricCard 
                label="Sync Interval" 
                value={`${metrics.time_sync?.sync_interval ?? 10}s`}
              />
              <MetricCard 
                label="Peers Synced" 
                value={metrics.time_sync?.peers_synced ?? 0}
                color={metrics.time_sync?.peers_synced > 0 ? 'success' : 'warning'}
              />
              <MetricCard 
                label="Sync Rounds"
                value={metrics.time_sync?.sync_round ?? 0}
                color={metrics.time_sync?.sync_round > 0 ? 'success' : ''}
              />
              <MetricCard 
                label="Last Sync"
                value={(() => {
                  const ns = metrics.time_sync?.last_sync;
                  if (!ns || ns === 0) return 'Never';
                  // ns is a large integer - use string math to avoid float precision loss
                  const ms = Math.floor(ns / 1e6);
                  const d = new Date(ms);
                  return d.toLocaleTimeString();
                })()}
              />
              <MetricCard 
                label="Cristian Offset (µs)" 
                value={(() => {
                  const offset = metrics.time_sync?.cristian_offset ?? 0;
                  return offset === 0 ? '0.0µs' : `${offset > 0 ? '+' : ''}${offset.toFixed(1)}µs`;
                })()} 
                color="success"
              />
              <MetricCard 
                label="Cristian RTT (µs)" 
                value={(() => {
                  const rtt = metrics.time_sync?.cristian_rtt ?? 0;
                  return `${rtt.toFixed(1)}µs`;
                })()}
              />
            </div>
          </div>
        </section>

        {/* Logical Clocks Panel */}
        <section className="monitor-panel logical-panel">
          <div className="panel-header">
            <h2>⏳ Logical Clocks (Lamport & Vector)</h2>
          </div>
          <div className="panel-content">
            <div className="metrics-grid">
              <MetricCard 
                label="Lamport Counter" 
                value={metrics.time_sync?.lamport_counter ?? 0} 
                color="warning" 
              />
              {metrics.time_sync?.vector_clock && Object.keys(metrics.time_sync.vector_clock).length > 0 && (
                <div className="metric-card full-width">
                  <div className="metric-label">Vector Clock State</div>
                  <div className="peer-list">
                    {Object.entries(metrics.time_sync.vector_clock).map(([node, tick]) => (
                      <div key={node} className="peer-item">
                        <span className="peer-name">{node}</span>
                        <span className="peer-address">Tick: {tick}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

// Reusable Metric Card Component
function MetricCard({ label, value, color, children }) {
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className={`metric-value ${color || ''}`}>{value}</div>
      {children}
    </div>
  );
}

export default Monitor;
