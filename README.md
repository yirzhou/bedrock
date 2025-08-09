# Bedrock: A Distributed, Transactional Key-Value Store

[![codecov](https://codecov.io/gh/yirzhou/bedrock/branch/main/graph/badge.svg)](https://codecov.io/gh/yirzhou/bedrock)

> Disclaimer: Bedrock is a hobby project built for educational purposes. While it is designed to be robust and has a growing test suite, it has not been audited or tested for production environments. Please use it in production at your own risk.

Bedrock is a persistent key-value store written in Go, designed to be both a high-performance embedded engine and a fault-tolerant distributed database. It uses a Log-Structured Merge-Tree (LSM-Tree) for its storage engine and the **Raft consensus algorithm** for replication.

The primary goal of this project is to explore and implement the core concepts of modern distributed database systems.

## Features

### Version 0.3 (Current)

The current version of Bedrock is a complete, distributed, and transactional key-value store.

- **Distributed Consensus with Raft:** Bedrock uses the HashiCorp Raft implementation to replicate data across a cluster of nodes. This provides high availability and fault tolerance, ensuring that the database remains operational and data is safe even if some servers in the cluster fail.

- **Multi-Key Transactions:** Bedrock supports atomic multi-key transactions, providing ACID guarantees. A fine-grained, per-key lock manager provides isolation between concurrent transactions.

- **Durable Writes:** All write operations are replicated via the Raft log, which is implemented on top of a durable **Write-Ahead Log (WAL)** on each node.

- **Crash Recovery & Snapshotting:** Each node can recover its state quickly from its local WAL. The Raft implementation uses efficient snapshots to allow new or slow followers to catch up to the leader's state without replaying the entire history of the log.

- **Leveled Compaction:** A background process on each node constantly performs **Leveled Compaction** on its local data, reclaiming disk space and keeping read performance high.

- **Fast Reads & Range Scans:** On-disk data is indexed with in-memory **sparse indexes**, and the system supports efficient, sorted range scans via a merging iterator.

## Usage Modes

Bedrock can be used in two primary modes: as an embedded library or as a distributed server.

### 1. Embedded (Single-Node) Mode

You can use Bedrock directly within your Go application as a durable, transactional, and concurrent key-value store library. All data will be stored in a local directory.

```go
// --- Using Bedrock as an Embedded Library ---

kvConfig := bedrock.NewDefaultConfiguration() // Or create your own
db, err := bedrock.Open(kvConfig)
if err != nil {
    log.Fatalf("Failed to open Bedrock store: %s", err)
}
defer db.Close()

// 2. Perform operations using transactions.
txn := db.BeginTransaction()
txn.Put([]byte("hello"), []byte("embedded world"))
txn.Put([]byte("status"), []byte("ok"))
if err := txn.Commit(); err != nil {
    log.Fatalf("Commit failed: %s", err)
}

// 3. Read the data back.
readTxn := db.BeginTransaction()
val, ok := readTxn.Get([]byte("hello"))
if ok {
    fmt.Printf("Found value: %s\n", val) // "embedded world"
}
readTxn.Rollback() // Read-only transactions can be rolled back.
```

### 2. Distributed (Cluster) Mode

For high availability, Bedrock can be run as a server process that communicates with other nodes in a cluster. This mode exposes an HTTP API for client interaction.

#### Running the Server

You can start a single-node Bedrock server using the provided `main` function.

```bash
# From your project's root directory:
# Assuming that the `main` function lives in that directory
go run ./cmd/server/
```

By default, this will start the Raft communication on port `9000` and the client API on port `8080`.

#### Writing Data (`PUT`)

To write or update a key, send a `PUT` request to the `/put` endpoint.

```bash
# Set the key "name" to the value "raft"
curl -X PUT -d 'raft' 'http://127.0.0.1:8080/put?key=name'
```

#### Reading Data (`GET`)

To write or update a key, send a `GET` request to the `/get` endpoint.

```bash
# Get the value for the key "name"
# Expected output: raft
curl 'http://127.0.0.1:8080/get?key=name'
```
