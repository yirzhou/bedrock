package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// Server is the main application server that wraps the Raft node and KVStore.
// It exposes an HTTP API for clients to interact with the database.
type Server struct {
	kv       *KVStore
	raftNode *raft.Raft
}

// NewServer creates a new server instance.
func NewServer(kv *KVStore, raftNode *raft.Raft) *Server {
	return &Server{
		kv:       kv,
		raftNode: raftNode,
	}
}

// handleGet is the HTTP handler for reading a key.
// It performs a strongly consistent read through the Raft leader.
func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	log.Printf("HandleGet: %s", r.URL.String())
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// Use a Raft Barrier to ensure our FSM is up-to-date.
	// This guarantees a strongly consistent read.
	future := s.raftNode.Barrier(5 * time.Second)
	if err := future.Error(); err != nil {
		log.Printf("failed to reach consensus: %s", err)
		http.Error(w, fmt.Sprintf("failed to reach consensus: %s", err), http.StatusInternalServerError)
		return
	}

	// Now we can safely read from our local KVStore.
	value, ok := s.kv.Get([]byte(key))
	if !ok {
		log.Printf("key not found: %s", key)
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(value)
}

// handlePut is the HTTP handler for writing a key.
// It proposes the change to the Raft cluster.
func (s *Server) HandlePut(w http.ResponseWriter, r *http.Request) {
	log.Printf("HandlePut: %s", r.URL.String())
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("HandlePut: failed to read request body: %v", err)
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	// Check if this node is the leader. Writes must go to the leader.
	if s.raftNode.State() != raft.Leader {
		// Redirect the client to the current leader.
		leaderAddr := s.raftNode.Leader()
		http.Redirect(w, r, "http://"+string(leaderAddr)+r.URL.String(), http.StatusTemporaryRedirect)
		return
	}

	// Serialize the command.
	command := encodePutCommand([]byte(key), value)

	// Submit the command to the Raft log. This will block until committed.
	future := s.raftNode.Apply(command, 5*time.Second)
	if err := future.Error(); err != nil {
		log.Printf("HandlePut: failed to apply command: %v", err)
		http.Error(w, fmt.Sprintf("failed to apply command: %s", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
