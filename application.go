package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// StartRaft initializes and starts the Raft node, turning the KVStore into a
// distributed, fault-tolerant state machine.
func StartRaft(kv *KVStore, config *ApplicationConfig, peers []string) (*raft.Raft, error) {
	// 1. Set up the core Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	// (Add logging, etc. to the config as needed)

	// 2. Set up the network transport layer.
	// This is how Raft nodes communicate with each other.
	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %w", err)
	}
	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create tcp transport: %w", err)
	}

	// 3. Set up the snapshot store.
	// This is where Raft will store snapshots of the FSM.
	snapshotStore, err := raft.NewFileSnapshotStore(config.GetBaseDir(), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// 4. Set up the durable log and stable stores.
	// We'll use BoltDB, a simple and reliable file-based store.
	boltDBPath := filepath.Join(config.GetBaseDir(), "raft.db")
	logStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	// The stable store is also provided by the BoltDB implementation.
	stableStore := logStore

	// 5. Instantiate the Raft system.
	// The FSM (your KVStore) is the most important component.
	raftNode, err := raft.NewRaft(raftConfig, kv, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to construct raft node: %w", err)
	}

	// 6. Bootstrap the cluster (if necessary).
	// Bootstrapping is the process of creating the initial cluster configuration.
	// This should only be done once, on one node, when the cluster is first created.
	bootstrapConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(config.NodeID),
				Address: transport.LocalAddr(),
			},
		},
	}
	// In a real application, you would have a flag or a check to ensure
	// this only runs on the first node of a brand new cluster.
	// For other nodes joining an existing cluster, you would use raft.AddVoter().
	if config.Bootstrap {
		bootstrapFuture := raftNode.BootstrapCluster(bootstrapConfig)
		if err := bootstrapFuture.Error(); err != nil {
			return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
	}

	return raftNode, nil
}
