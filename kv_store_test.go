package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	keyVal := make(map[string]string)
	count := 100

	// Put 100 key-value pairs.
	for range count {
		key := GenerateRandomString(10)
		value := GenerateRandomString(10)
		kv.Put([]byte(key), []byte(value))
		keyVal[key] = value
	}

	// Confirm the key-value pairs are stored.
	for key, val := range keyVal {
		value, found := kv.Get([]byte(key))
		assert.True(t, found)
		assert.Equal(t, []byte(val), value)
	}
}

func TestRecoveryNormalWithBatchTransactions(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	count := 1024
	// Put 1024 key-value pairs.
	txn := kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	txn.Commit()

	// Record the last sequence number and current segment ID.
	lastSequenceNum := kv.GetLastSequenceNum()
	segmentID := kv.GetCurrentSegmentIDSafe()

	// Close the KVStore.
	kv.Close()
	// Recover the KVStore.
	kv, err = Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()
	// Check if the last sequence number and current segment ID are the same.
	assert.Equal(t, lastSequenceNum, kv.GetLastSequenceNum())
	assert.Equal(t, segmentID, kv.GetCurrentSegmentIDSafe())

	// Check if the key-value pairs are recovered.
	for i := range count {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryNormalWithIndividualTransactions(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	count := 100
	// Put 100 key-value pairs.
	for i := range count {
		txn := kv.BeginTransaction()
		txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		txn.Commit()
	}

	// Record the last sequence number and current segment ID.
	lastSequenceNum := kv.GetLastSequenceNum()
	segmentID := kv.GetCurrentSegmentIDSafe()

	// Close the KVStore.
	kv.Close()
	// Recover the KVStore.
	kv, err = Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()
	// Check if the last sequence number and current segment ID are the same.
	assert.Equal(t, lastSequenceNum, kv.GetLastSequenceNum())
	assert.Equal(t, segmentID, kv.GetCurrentSegmentIDSafe())

	// Check if the key-value pairs are recovered.
	for i := range count {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryNormalBlindWrites(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	count := 1024
	// Put key-value pairs.
	txn := kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	txn.Commit()

	// Record the last sequence number and current segment ID.
	lastSequenceNum := kv.GetLastSequenceNum()
	segmentID := kv.GetCurrentSegmentIDSafe()

	// Close the KVStore.
	kv.Close()
	// Recover the KVStore.
	kv, err = Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()
	// Check if the last sequence number and current segment ID are the same.
	assert.Equal(t, lastSequenceNum, kv.GetLastSequenceNum())
	assert.Equal(t, segmentID, kv.GetCurrentSegmentIDSafe())

	// Check if the key-value pairs are recovered.
	for i := range count {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryWithCorruptedWALFile(t *testing.T) {
	dir := t.TempDir()
	count := 7
	config := NewConfigurationNoMaintenance().WithBaseDir(dir).WithMemtableSizeThreshold(int64(count + 1)).WithCheckpointSize(1024 * 1024)
	kv, err := Open(config)
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	// Put key-value pairs.
	for i := range count {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	segmentID := kv.GetCurrentSegmentIDSafe()

	// Close the DB
	kv.Close()

	// Corrupt the last bit of the latest WAL file.
	walFilePath := filepath.Join(dir, "/logs/"+getWalFileNameFromSegmentID(segmentID))
	stat, err := os.Stat(walFilePath)
	assert.NoError(t, err)
	assert.NoError(t, os.Truncate(walFilePath, stat.Size()-1))

	// The database should be recovered from all segments and WALs.
	// The last bit will only corrupt the last record in the WAL file, which is the "key-99" record.
	// So the key-value pairs from 0 to 98 should be recovered, but the key-value pair for "key-99" should be lost.
	kv, err = Open(config)
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}

	// Check if the key-value pairs are recovered.
	for i := range count - 1 {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
	// Confirm the key-value pair for "key-127" is not found.
	value, found := kv.Get([]byte(fmt.Sprintf("key-%d", count-1)))
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestRecoveryWithCorruptedSparseIndexFile(t *testing.T) {
	dir := t.TempDir()
	config := NewConfigurationNoMaintenance().WithBaseDir(dir).WithCheckpointSize(16)
	kv, _ := Open(config)

	count := 1024
	// Put key-value pairs.
	txn := kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	txn.Commit()

	segmentID := kv.GetCurrentSegmentIDUnsafe() - 1

	// Close the DB
	kv.Close()

	// Corrupt the sparse index file.
	sparseIndexFilePath := filepath.Join(dir, "/checkpoints/"+getSparseIndexFileNameFromSegmentId(segmentID))
	stat, _ := os.Stat(sparseIndexFilePath)
	os.Truncate(sparseIndexFilePath, stat.Size()-1)

	// The database should be recovered from all segments and WALs.
	// Record the time it takes to recover
	kv, _ = Open(config)
	defer kv.CloseAndCleanUp()

	// Check if the key-value pairs are recovered.
	for i := range count {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryWithCorruptedCheckpoint(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	// Put 100 key-value pairs.
	for i := range 100 {
		kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Close the DB
	kv.Close()

	// Corrupt the checkpoint file.
	checkpointFilePath := filepath.Join(dir, "/checkpoints/CHECKPOINT")
	os.Truncate(checkpointFilePath, 0)

	// The database should be recovered from all segments and WALs.
	kv, err = Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}

	// Check if the key-value pairs are stored.
	for i := range 100 {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}

func TestRecoveryNormalWithVariousCheckpointSizes(t *testing.T) {
	checkpointSize := 1024
	for checkpointSize <= 1024*1024 {
		dir := t.TempDir()
		kv, _ := Open(NewDefaultConfiguration().WithNoLog().WithBaseDir(dir).WithCheckpointSize(int64(checkpointSize)))
		count := 100

		// Put key-value pairs.
		txn := kv.BeginTransaction()
		for i := range count {
			txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
		}
		txn.Commit()
		// Close the DB
		kv.Close()

		// Recover the DB
		kv, _ = Open(NewDefaultConfiguration().WithNoLog().WithBaseDir(dir).WithCheckpointSize(int64(checkpointSize)))
		defer kv.CloseAndCleanUp()

		// Check if the key-value pairs are recovered.
		txn = kv.BeginTransaction()
		for i := range count {
			value, found := txn.Get([]byte(fmt.Sprintf("key-%d", i)))
			assert.True(t, found)
			assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value, "checkpointSize: %d, key: %s, value: %s", checkpointSize, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		}
		checkpointSize *= 2
		txn.Commit()
	}
}

func TestDeleteBatch(t *testing.T) {
	dir := t.TempDir()
	kv, _ := Open(NewConfigurationNoMaintenance().WithNoLog().WithBaseDir(dir))
	defer kv.CloseAndCleanUp()

	count := 1024

	// Put key-value pairs.
	txn := kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	txn.Commit()

	// Delete key-value pairs.
	txn = kv.BeginTransaction()
	for i := range count {
		err := txn.Delete([]byte(fmt.Sprintf("key-%d", i)))
		assert.NoError(t, err)
	}
	txn.Commit()

	// Check if the key-value pairs are deleted.
	for i := range count {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.False(t, found)
		assert.Nil(t, value)
	}

	// Put key-value pairs again.
	txn = kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	txn.Commit()

	// Check if the key-value pairs are recovered.
	for i := range count {
		value, found := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
}
