import React from 'react';
import FileManager from './components/FileManager';
import Monitor from './components/Monitor';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <nav className="top-nav">
          <Link to="/" className="nav-link">File Manager</Link>
          <Link to="/monitor" className="nav-link">Algorithm Monitor</Link>
        </nav>
        <Routes>
          <Route path="/" element={<FileManager />} />
          <Route path="/monitor" element={<Monitor />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
