import React, { useState, useEffect } from 'react';
import { FileIcons, getFileIcon } from './FileIcons';
import './FileManager.css';

const API_BASE = 'http://localhost:8080';

function FileManager() {
  const [files, setFiles] = useState([]);
  const [status, setStatus] = useState({ node_id: '-', role: 'UNKNOWN', files_count: 0 });
  const [leaderURL, setLeaderURL] = useState(API_BASE); // Track current leader
  const [leaderName, setLeaderName] = useState('node1'); // Track leader name for display
  const [searchQuery, setSearchQuery] = useState('');
  const [viewMode, setViewMode] = useState('grid'); // 'grid', 'list', 'tree'
  const [sortBy, setSortBy] = useState('name'); // 'name', 'size', 'date', 'type'
  const [sortOrder, setSortOrder] = useState('asc'); // 'asc' or 'desc'
  const [filterType, setFilterType] = useState('all'); // 'all', 'file', 'folder'
  const [notification, setNotification] = useState({ show: false, message: '', type: 'info' });

  useEffect(() => {
    const init = async () => {
      await loadStatus();
      loadFiles(); // Load files from any available node
    };
    init();
    
    const interval = setInterval(async () => {
      await loadStatus();
      loadFiles(); // Refresh files periodically
    }, 5000); // Auto-refresh every 5s to detect leader changes
    
    return () => clearInterval(interval);
  }, []);

  const loadStatus = async () => {
    const nodes = ['http://localhost:8080', 'http://localhost:8081', 'http://localhost:8082'];
    let statuses = [];
    for (const nodeUrl of nodes) {
      try {
        const response = await fetch(`${nodeUrl}/api/status`);
        if (response.ok) {
          const data = await response.json();
          console.log(`Status from ${nodeUrl}:`, data);
          statuses.push({ nodeUrl, data });
        }
      } catch (error) {
        // Skip offline nodes
      }
    }
    
    if (statuses.length === 0) {
      console.error('Failed to load status - all nodes might be offline');
      return null;
    }
    
    // Find the leader
    let leaderStatus = statuses.find(s => s.data.role === 'Leader');
    if (leaderStatus) {
      setStatus(leaderStatus.data);
      setLeaderURL(leaderStatus.nodeUrl);
      setLeaderName(leaderStatus.data.node_id || 'node1');
      return { data: leaderStatus.data, leaderURL: leaderStatus.nodeUrl };
    }
    
    // No node claims to be leader, check if any node knows who the leader is
    const firstStatus = statuses[0];
    setStatus(firstStatus.data);
    
    // Check all statuses for raft_leader information
    let knownLeader = null;
    for (const status of statuses) {
      if (status.data.raft_leader && status.data.raft_leader !== 'unknown') {
        knownLeader = status.data.raft_leader;
        break;
      }
    }
    
    let newLeaderURL = firstStatus.nodeUrl;
    if (knownLeader) {
      const leaderMap = {
        'node1': 'http://localhost:8080',
        'node2': 'http://localhost:8081',
        'node3': 'http://localhost:8082',
        '1': 'http://localhost:8080',
        '2': 'http://localhost:8081',
        '3': 'http://localhost:8082'
      };
      if (leaderMap[knownLeader]) {
        newLeaderURL = leaderMap[knownLeader];
        setLeaderName(knownLeader);
      }
    }
    
    setLeaderURL(newLeaderURL);
    return { data: firstStatus.data, leaderURL: newLeaderURL };
  };

  const loadFiles = async () => {
    const nodes = ['http://localhost:8080', 'http://localhost:8081', 'http://localhost:8082'];
    for (const nodeUrl of nodes) {
      try {
        const response = await fetch(`${nodeUrl}/api/files`);
        if (response.ok) {
          const data = await response.json();
          setFiles(data || []);
          return; // Successfully loaded from this node
        }
      } catch (error) {
        // Try next node
      }
    }
    console.error('Failed to load files from any node');
    setFiles([]);
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    // Refresh status to get latest leader before upload
    const statusResult = await loadStatus();
    
    // Use the leader URL that loadStatus() already determined
    let currentLeaderURL = statusResult ? statusResult.leaderURL : API_BASE;

    const formData = new FormData();
    formData.append('file', file);

    try {
      // Upload to current leader (not necessarily this node)
      console.log(`Uploading to leader at ${currentLeaderURL}`);
      
      const response = await fetch(`${currentLeaderURL}/api/upload`, {
        method: 'POST',
        body: formData
      });

      if (response.ok) {
        showNotification(`Uploaded ${file.name} successfully`, 'success');
        loadFiles(); // Refresh file list from any available node
      } else {
        const errorText = await response.text();
        showNotification(`Failed to upload: ${errorText}`, 'error');
      }
    } catch (error) {
      showNotification(`Upload error: ${error.message}`, 'error');
    }
  };

  const downloadFile = async (filename) => {
    window.open(`${API_BASE}/api/files/${encodeURIComponent(filename)}`, '_blank');
  };

  const deleteFile = async (filename) => {
    // eslint-disable-next-line no-restricted-globals
    if (!confirm(`Delete "${filename}"?`)) return;

    // Refresh status to get latest leader before delete
    const statusResult = await loadStatus();
    
    // Use the leader URL that loadStatus() already determined
    let currentLeaderURL = statusResult ? statusResult.leaderURL : API_BASE;

    try {
      // Delete from leader (only Raft leader can delete)
      console.log(`Deleting from leader at ${currentLeaderURL}`);
      
      const response = await fetch(`${currentLeaderURL}/api/delete/${encodeURIComponent(filename)}`, {
        method: 'DELETE'
      });

      if (response.ok) {
        showNotification(`Deleted "${filename}" successfully`, 'success');
        loadFiles(); // Refresh file list from any available node
      } else {
        const errorText = await response.text();
        showNotification(`Failed to delete: ${errorText}`, 'error');
      }
    } catch (error) {
      showNotification(`Delete error: ${error.message}`, 'error');
    }
  };

  const showNotification = (message, type) => {
    setNotification({ show: true, message, type });
    setTimeout(() => setNotification({ ...notification, show: false }), 3000);
  };

  // Advanced sorting function
  const sortFiles = (filesToSort) => {
    return [...filesToSort].sort((a, b) => {
      let comparison = 0;
      
      switch (sortBy) {
        case 'name':
          comparison = a.Filename.localeCompare(b.Filename);
          break;
        case 'size':
          comparison = a.Size - b.Size;
          break;
        case 'date':
          comparison = new Date(a.CreatedAt) - new Date(b.CreatedAt);
          break;
        case 'type':
          const extA = a.Filename.split('.').pop().toLowerCase();
          const extB = b.Filename.split('.').pop().toLowerCase();
          comparison = extA.localeCompare(extB);
          break;
        default:
          comparison = 0;
      }
      
      return sortOrder === 'asc' ? comparison : -comparison;
    });
  };

  // Advanced filtering and search
  const filteredFiles = () => {
    let result = files;
    
    // Search by name
    if (searchQuery) {
      result = result.filter(file =>
        file.Filename.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }
    
    // Filter by type
    if (filterType !== 'all') {
      if (filterType === 'file') {
        result = result.filter(file => file.Filename.includes('.'));
      } else if (filterType === 'folder') {
        result = result.filter(file => !file.Filename.includes('/'));
      }
    }
    
    // Apply sorting
    return sortFiles(result);
  };

  const buildTree = (files) => {
    const root = {};
    files.forEach(file => {
      const parts = file.Filename.split('/');
      let current = root;
      parts.forEach((part, index) => {
        if (!current[part]) {
          current[part] = index === parts.length - 1 ? { _isFile: true, _data: file } : {};
        }
        current = current[part];
      });
    });
    return root;
  };

  const renderTree = (tree, path = '') => {
    return Object.entries(tree).map(([name, children]) => {
      if (children._isFile) {
        return (
          <li key={`${path}/${name}`} className="file-item">
            <div className="item-content" onClick={() => downloadFile(children._data.Filename)}>
              {React.createElement(getFileIcon(name))}
              <span className="file-name">{name}</span>
              <span className="file-size">({formatSize(children._data.Size)})</span>
            </div>
          </li>
        );
      } else {
        return (
          <li key={`${path}/${name}`} className="folder-item">
            <details>
              <summary>
                <FileIcons.folder />
                <span>{name}</span>
              </summary>
              <ul>{renderTree(children, `${path}/${name}`)}</ul>
            </details>
          </li>
        );
      }
    });
  };

  const formatSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  return (
    <div className="file-manager">
      <header className="fm-header">
        <h1><FileIcons.upload /> Distributed File Storage</h1>
        <div className="status-bar">
          <span className="current-node-badge">🟢 Current Leader: <strong>{leaderName || status.node_id}</strong></span>
          <span className="file-count">{status.files_count} files</span>
          {leaderURL && (
            <span className="leader-indicator" style={{marginLeft: '10px', color: '#4caf50'}}>
              📍 Active at: {leaderURL.replace('http://', '')}
            </span>
          )}
        </div>
      </header>

      <div className="control-panel">
        <div className="upload-section">
          <input
            type="file"
            id="fileInput"
            onChange={handleFileUpload}
            style={{ display: 'none' }}
          />
          <button className="btn btn-primary" onClick={() => document.getElementById('fileInput').click()}>
            <FileIcons.upload /> Upload
          </button>
          <button className="btn btn-success" onClick={loadFiles}>
            <FileIcons.refresh /> Refresh
          </button>
        </div>
        <div className="filters-section">
          <select 
            className="filter-select" 
            value={filterType}
            onChange={(e) => setFilterType(e.target.value)}
          >
            <option value="all">All Files</option>
            <option value="file">Files Only</option>
            <option value="folder">Folders Only</option>
          </select>
          <select 
            className="sort-select" 
            value={`${sortBy}-${sortOrder}`}
            onChange={(e) => {
              const [newSortBy, newSortOrder] = e.target.value.split('-');
              setSortBy(newSortBy);
              setSortOrder(newSortOrder);
            }}
          >
            <option value="name-asc">Name (A-Z)</option>
            <option value="name-desc">Name (Z-A)</option>
            <option value="size-asc">Size (Smallest)</option>
            <option value="size-desc">Size (Largest)</option>
            <option value="date-asc">Date (Oldest)</option>
            <option value="date-desc">Date (Newest)</option>
            <option value="type-asc">Type (A-Z)</option>
            <option value="type-desc">Type (Z-A)</option>
          </select>
        </div>
        <div className="search-section">
          <input
            type="text"
            placeholder="Search files..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
          <FileIcons.search />
        </div>
      </div>

      <div className="file-browser">
        <div className="view-toggle">
          <button
            className={`btn ${viewMode === 'grid' ? 'active' : ''}`}
            onClick={() => setViewMode('grid')}
          >
            <FileIcons.listView /> Grid
          </button>
          <button
            className={`btn ${viewMode === 'list' ? 'active' : ''}`}
            onClick={() => setViewMode('list')}
          >
            <FileIcons.listView /> List
          </button>
          <button
            className={`btn ${viewMode === 'tree' ? 'active' : ''}`}
            onClick={() => setViewMode('tree')}
          >
            <FileIcons.treeView /> Tree
          </button>
        </div>

        {viewMode === 'tree' ? (
          <div className="tree-view">
            <ul className="file-tree">{renderTree(buildTree(filteredFiles()))}</ul>
          </div>
        ) : viewMode === 'grid' ? (
          <div className="grid-view">
            <div className="file-grid">
              {filteredFiles().map((file, idx) => (
                <div key={idx} className="grid-item">
                  <div className="grid-item-content" onClick={() => downloadFile(file.Filename)}>
                    <div className="grid-icon">{React.createElement(getFileIcon(file.Filename))}</div>
                    <div className="grid-file-name">{file.Filename}</div>
                    <div className="grid-file-size">{formatSize(file.Size)}</div>
                  </div>
                  <div className="grid-actions">
                    <button className="btn-icon" onClick={(e) => { e.stopPropagation(); downloadFile(file.Filename); }}>
                      <FileIcons.download />
                    </button>
                    <button className="btn-icon btn-danger" onClick={(e) => { e.stopPropagation(); deleteFile(file.Filename); }}>
                      <FileIcons.delete />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="list-view">
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Name</th>
                  <th>Size</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredFiles().map((file, idx) => (
                  <tr key={idx}>
                    <td>{React.createElement(getFileIcon(file.Filename))}</td>
                    <td>{file.Filename}</td>
                    <td>{formatSize(file.Size)}</td>
                    <td>{file.CreatedAt ? new Date(Math.floor(file.CreatedAt / 1000000)).toLocaleString() : 'N/A'}</td>
                    <td>
                      <button className="btn-icon" onClick={() => downloadFile(file.Filename)}>
                        <FileIcons.download />
                      </button>
                      <button className="btn-icon btn-danger" onClick={() => deleteFile(file.Filename)}>
                        <FileIcons.delete />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {notification.show && (
        <div className={`notification ${notification.type} show`}>
          {notification.message}
        </div>
      )}
    </div>
  );
}

export default FileManager;
