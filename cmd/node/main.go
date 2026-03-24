package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/node"
)

func main() {
	// Parse command line flags
	nodeID := flag.String("id", "node1", "Unique node identifier")
	addr := flag.String("addr", "localhost", "Node address (IP or hostname)")
	port := flag.String("port", "8080", "HTTP server port")
	dataDir := flag.String("data", "./data", "Directory for storing files")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer nodes (format: id1=addr1:port1,id2=addr2:port2)")
	flag.Parse()

	// Parse peers
	peers := parsePeers(*peersFlag)

	// Ensure data directory exists
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create absolute path for data directory
	absDataDir, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("Failed to get absolute path for data directory: %v", err)
	}

	// Create and start node
	n := node.NewNode(*nodeID, *addr, *port, peers, absDataDir)
	if err := n.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	log.Printf("Node %s started successfully on %s:%s\n", *nodeID, *addr, *port)
	log.Printf("Data directory: %s\n", absDataDir)
	if len(peers) > 0 {
		log.Printf("Connected to %d peer(s)\n", len(peers))
	} else {
		log.Println("Running in standalone mode")
	}

	// Keep running until interrupted
	select {}
}

// parsePeers converts comma-separated peer string to map
func parsePeers(peersFlag string) map[string]string {
	peers := make(map[string]string)
	if peersFlag == "" {
		return peers
	}

	peerList := splitPeers(peersFlag)
	for _, peer := range peerList {
		parts := splitPeerString(peer)
		if len(parts) == 2 {
			peers[parts[0]] = parts[1]
		}
	}
	return peers
}

// splitPeers splits peer string by comma
func splitPeers(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

// splitPeerString splits id=addr:port format
func splitPeerString(s string) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return []string{s[:i], s[i+1:]}
		}
	}
	return nil
}
