package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

// ApplicationConfig is the configuration for the entire raft-based application.
//
// See also: StartRaft()
type ApplicationConfig struct {
	NodeID            string
	RaftAddr          string
	BaseDir           string
	Bootstrap         bool
	SnapshotInterval  time.Duration
	SnapshotRetention int
	SnapshotThreshold int
}

// GetBaseDir returns the base directory for the application.
//
// It is used to store Raft snapshots.
func (c *ApplicationConfig) GetBaseDir() string {
	return c.BaseDir
}

// DefaultApplicationConfig creates a default configuration for a single-node,
// localhost setup. This is useful for testing.
func DefaultApplicationConfig(baseDir string) *ApplicationConfig {
	// Get a temporary directory for the base path.
	// This makes cleanup easy.
	tempDir, err := os.MkdirTemp(baseDir, "bedrock-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %s", err))
	} else {
		log.Printf("created temp dir: %s", tempDir)
	}

	return &ApplicationConfig{
		NodeID:            "server-1",
		RaftAddr:          "127.0.0.1:9000",
		BaseDir:           tempDir,
		Bootstrap:         true, // Bootstrap is true for a single-node test cluster.
		SnapshotInterval:  30 * time.Second,
		SnapshotRetention: 2,
		SnapshotThreshold: 8192,
	}
}
