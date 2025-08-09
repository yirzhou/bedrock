package main

import (
	"log"
	"net/http"
	"os"
	
)

func main() {
	// Start a single-node cluster as a HTTP server.
	// Remember to delete the temp dir after the program exits.
	// This is a simple example for demonstrating the basic usage of bedrock.
	tempDir, err := os.MkdirTemp("/tmp", "bedrock-")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	config := DefaultApplicationConfig(tempDir)
	peers := []string{config.RaftAddr}

	kv, err := Open(NewDefaultConfiguration().WithBaseDir(config.GetBaseDir()))
	if err != nil {
		log.Fatalf("Failed to open KVStore: %v", err)
	}

	raftNode, err := StartRaft(kv, config, peers)
	if err != nil {
		log.Fatalf("Failed to start Raft node: %v", err)
	}

	server := NewServer(kv, raftNode)
	http.HandleFunc("/get", server.HandleGet)
	http.HandleFunc("/put", server.HandlePut)

	clientAddr := "127.0.0.1:8080"
	log.Printf("Starting client API server at %s", clientAddr)

	if err := http.ListenAndServe(clientAddr, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}

}
