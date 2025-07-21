# Bedrock: A Durable, Transactional Key-Value Store

Bedrock is a persistent, single-node key-value store written in Go. It is built from the ground up to be durable, crash-safe, and to support ACID-like transactions, using principles from foundational database systems like LevelDB/RocksDB and Google's Bigtable.

The primary goal of this project is to explore and implement the core concepts of modern storage engines.

## Features

### Version 0.2 (Current)

The current version of Bedrock is a complete, transactional key-value store with a full data lifecycle management system.

- **Multi-Key Transactions:** Bedrock supports atomic multi-key transactions, providing ACID guarantees. Operations are buffered and committed as a single atomic unit, ensuring that a transaction either succeeds completely or has no effect at all. A fine-grained, per-key lock manager provides isolation between concurrent transactions.

- **Durable Writes:** All write operations (`Put`, `Delete`), whether in an explicit transaction or as a single "autocommit" operation, are first recorded in a **Write-Ahead Log (WAL)**. This ensures that no acknowledged write is ever lost.

- **Crash Recovery:** On startup, the system automatically recovers its state. It validates log entries using checksums, detects and truncates corrupted data from unclean shutdowns, and rebuilds its in-memory state to be perfectly consistent with the last successful operation.

- **Checkpointing & Log Segmentation:** To ensure fast recovery times, the system periodically performs checkpoints. The in-memory store (`memtable`) is flushed to sorted, immutable data files on disk (**Segments/SSTables**). This allows old WAL segments to be safely purged.

- **Leveled Compaction:** A background process constantly monitors the on-disk data segments. It uses a **Leveled Compaction** strategy to merge segments together, purging old, overwritten, or deleted (tombstoned) data. This reclaims disk space and keeps read performance high over the long term.

- **Fast Reads:** On-disk data segments are indexed with in-memory **sparse indexes**, allowing for fast key lookups without needing to scan entire files.

## Roadmap

### Version 0.3 (Upcoming)

The next major milestone is to evolve Bedrock from a single-node database into a fault-tolerant, distributed system.

- **Distributed Consensus with Raft:** Integrate the transactional key-value store as a state machine with the **Raft consensus algorithm**. This will replicate the WAL across multiple nodes, providing high availability and ensuring data safety even if some servers in the cluster fail.
