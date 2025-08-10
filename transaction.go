package main

import (
	"bytes"
	"errors"
	"log"

	"github.com/yirzhou/bedrock/lib"
)

// Transaction allows multiple operations to be performed atomically.
type Transaction struct {
	writes     map[string][]byte
	store      *KVStore
	readLocks  map[string]bool
	writeLocks map[string]bool
}

// NewTransaction creates a new transaction.
func NewTransaction(store *KVStore) *Transaction {
	return &Transaction{
		writes:     make(map[string][]byte),
		store:      store,
		readLocks:  make(map[string]bool),
		writeLocks: make(map[string]bool),
	}
}

// Delete deletes a key from the KVStore.
func (txn *Transaction) Delete(key []byte) error {
	if err := txn.validateKey(key); err != nil {
		return err
	}
	return txn.putInternal(key, lib.TOMBSTONE)
}

// validateKey validates the key.
func (txn *Transaction) validateKey(key []byte) error {
	if key == nil || bytes.Equal(key, lib.CHECKPOINT) || bytes.Equal(key, lib.TOMBSTONE) {
		return errors.New("invalid key")
	}
	return nil
}

// Put writes a key-value pair to the KVStore.
func (txn *Transaction) Put(key, value []byte) error {
	if err := txn.validateKey(key); err != nil {
		return err
	}
	return txn.putInternal(key, value)
}

// putInternal writes a key-value pair to the private buffer.
func (txn *Transaction) putInternal(key, value []byte) error {
	stringKey := string(key)
	// 1. Write to the private buffer.
	txn.writes[stringKey] = value

	// 2. Acquire a write lock on the key if not already acquired.
	if _, ok := txn.writeLocks[stringKey]; !ok {
		txn.store.lockManager.AcquireWriteLock(key)
		txn.writeLocks[stringKey] = true
	}
	return nil
}

// Get gets a value from the KVStore.
func (txn *Transaction) Get(key []byte) ([]byte, bool) {
	stringKey := string(key)
	// 1. Check its private buffer first.
	if value, ok := txn.writes[stringKey]; ok {
		if bytes.Equal(value, lib.TOMBSTONE) {
			return nil, false
		}
		return value, true
	}

	// 2. If not found, check the store.
	// First, acquire a read lock on the key.
	// 2. Check if we already hold a lock for this key.
	// A write lock also grants read permission.
	if !txn.readLocks[stringKey] && !txn.writeLocks[stringKey] {
		// 3. If not, acquire a read lock from the manager.
		txn.store.lockManager.AcquireReadLock(key)
		txn.readLocks[stringKey] = true // Remember we hold this lock.
	}
	return txn.store.get(key)
}

// Rollback rolls back the transaction.
func (txn *Transaction) Rollback() error {
	// Clear the buffer.
	txn.writes = make(map[string][]byte)
	// Release the locks.
	txn.releaseLocks()
	return nil
}

// Commit commits the transaction.
// Fast Path for Read-Only: First, check if the transaction's private writes map is empty. If it is, this was a read-only transaction. There's nothing to commit. The method can simply release its read locks and return success immediately.

// Acquire Global Store Lock: If there are writes, the transaction must now acquire the main, coarse-grained KVStore write lock (s.mu.Lock()). This is critical. It ensures that no other transaction can commit and no compaction can start while this one is in the process of becoming durable.

// Create Atomic Commit Record: The function now iterates through the transaction's private writes map and serializes all the key-value pairs into a single, large TXN_COMMIT record, as we designed.

// Write to WAL & fsync: It appends this single record to the WAL and, most importantly, calls fsync() to guarantee it is safely on disk. This fsync is the "commit point." Once it returns successfully, the transaction is officially committed and will survive a crash.

// Apply to Memtable: Now that the changes are durable, the function iterates through the transaction's writes map again and applies each change to the main KVStore's memtable. Because we are still holding the global lock, this update is safe.

// Release Global Store Lock: The durable write and the in-memory update are complete. The function can now release the main KVStore write lock (s.mu.Unlock()).

// Release Per-Key Locks: The final step is to tell the LockManager to release all the individual read and write locks that this transaction was holding. This allows other waiting transactions to proceed.
func (txn *Transaction) Commit() error {
	// 1. Fast path for read-only transactions.
	if len(txn.writes) == 0 {
		txn.store.lockManager.ReleaseReadLocks(txn.getReadLocks())
		return nil
	}

	// 2. Acquire the global store lock.
	txn.store.lock.Lock()
	defer txn.store.lock.Unlock()

	// 3. Create the commit record (for all writes)
	commitRecord := txn.createCommitRecord()

	// 4. Write to WAL and fsync.
	walErr := txn.store.wal.AppendTransaction(0, commitRecord)
	if walErr != nil && walErr != lib.ErrCheckpointNeeded {
		// If the durable write to the WAL fails for any reason other than a
		// log roll, we cannot proceed. The transaction must be aborted.
		log.Println("Fatal: Error writing commit record to WAL:", walErr)
		// We don't release per-key locks here because the transaction is
		// effectively aborted and the state is uncertain. A real system
		// might need more complex recovery, but for now, this is the safest path.
		// The application's logic should be:

		// Assume the transaction has failed. The changes are definitely not saved.

		// Do not retry the transaction. Since the underlying problem is a system-level I/O error, retrying will just fail again.

		// Log a critical alert. This is an issue that requires a human operator to investigate (e.g., "Cannot write to database disk!").

		// Stop trying to perform writes. The application should probably switch into a degraded, read-only mode or shut down gracefully, as it can no longer trust its storage layer.
		return walErr
	}

	// 5. Apply to Memtable.
	for key, value := range txn.writes {
		txn.store.memState.Put([]byte(key), value)
	}

	// Perform the checkpointing if needed.
	if walErr == lib.ErrCheckpointNeeded || txn.store.memState.Size() >= txn.store.config.GetMemtableSizeThreshold() {
		// TODO: the checkpointing can also be done in the background.
		txn.store.checkpointNeeded = true
		if txn.store.config.EnableSyncCheckpoint && txn.store.config.EnableCheckpoint {
			err := txn.store.doCheckpoint(true)
			if err != nil {
				// If the checkpoint fails, it's a serious issue, but the transaction
				// itself is already durably committed to the WAL. We log the error
				// and allow the commit to succeed. The next background run will retry.
				log.Println("Error performing checkpoint:", err)
			}
		}
	}

	// 7. Release the per-key locks.
	txn.releaseLocks()
	return nil
}

// releaseLocks releases the read and write locks.
func (txn *Transaction) releaseLocks() {
	txn.store.lockManager.ReleaseReadLocks(txn.getReadLocks())
	txn.store.lockManager.ReleaseWriteLocks(txn.getWriteLocks())
	// Clear the read and write locks.
	txn.readLocks = make(map[string]bool)
	txn.writeLocks = make(map[string]bool)
}

// createCommitRecord creates the commit record for all writes.
func (txn *Transaction) createCommitRecord() []byte {
	ops := make([][]byte, 0, len(txn.writes))
	for key, value := range txn.writes {
		ops = append(ops, GetPayloadForPut([]byte(key), value))
	}
	return GetPayloadForCommit(ops)
}

// getReadLocks returns the read locks.
func (txn *Transaction) getReadLocks() []string {
	keys := make([]string, 0, len(txn.readLocks))
	for key := range txn.readLocks {
		keys = append(keys, key)
	}
	return keys
}

// getWriteLocks returns the write locks.
func (txn *Transaction) getWriteLocks() []string {
	keys := make([]string, 0, len(txn.writeLocks))
	for key := range txn.writeLocks {
		keys = append(keys, key)
	}
	return keys
}
