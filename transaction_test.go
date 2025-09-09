package bedrock

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSuccessfulCommit(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()

	txn := kv.BeginTransaction()
	for i := range 100 {
		txn.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	txn.Commit()
	for i := range 100 {
		value, ok := kv.Get([]byte(fmt.Sprintf("key%d", i)))
		assert.True(t, ok)
		assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
	}
}

func TestSuccessfulRollback(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()
	txn := kv.BeginTransaction()
	txn.Put([]byte("key1"), []byte("value1"))
	txn.Rollback()
	_, ok := kv.Get([]byte("key1"))
	assert.False(t, ok)

	// Open a new transaction and read the key.
	// The key should not be found.
	txn = kv.BeginTransaction()
	_, ok = txn.Get([]byte("key1"))
	assert.False(t, ok)
	txn.Rollback()
}

func TestGetAfterPut(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()

	txn := kv.BeginTransaction()
	txn.Put([]byte("key1"), []byte("value1"))
	value, ok := txn.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value1"), value)
	txn.Rollback()
}

func TestIsolationTwoGoroutines(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		txn := kv.BeginTransaction()
		txn.Get([]byte("key1"))
		// Sleep for 100 ms
		time.Sleep(100 * time.Millisecond)
		txn.Commit()
	}()

	go func() {
		defer wg.Done()
		// Sleep for 20 ms
		time.Sleep(20 * time.Millisecond)
		txn := kv.BeginTransaction()
		txn.Put([]byte("key1"), []byte("new_value"))
		txn.Commit()
	}()

	wg.Wait()
	value, ok := kv.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("new_value"), value)
}

func TestWriteWriteConflict(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		txn := kv.BeginTransaction()
		txn.Put([]byte("key1"), []byte("value1"))
		// Sleep for 100 ms
		time.Sleep(100 * time.Millisecond)
		txn.Commit()
	}()

	go func() {
		defer wg.Done()
		// Sleep for 20 ms
		time.Sleep(20 * time.Millisecond)
		txn := kv.BeginTransaction()
		txn.Put([]byte("key1"), []byte("value2"))
		txn.Commit()
	}()

	wg.Wait()
	value, ok := kv.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value2"), value)
}

func TestNonConflictingWrites(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		txn := kv.BeginTransaction()
		txn.Put([]byte("key1"), []byte("value1"))
		txn.Commit()
	}()

	go func() {
		defer wg.Done()
		txn := kv.BeginTransaction()
		txn.Put([]byte("key2"), []byte("value2"))
		txn.Commit()
	}()

	wg.Wait()
	value, ok := kv.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value1"), value)
	value, ok = kv.Get([]byte("key2"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value2"), value)
}

func TestCommitAndCrash(t *testing.T) {
	dir := t.TempDir()
	config := NewConfigurationNoMaintenance().WithBaseDir(dir)
	kv, err := Open(config)
	if err != nil {
		t.Fatalf("Error creating KVStore: %v", err)
	}

	count := 100
	txn := kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	txn.Commit()

	// Crash the process (simulated by closing the database).
	kv.Close()

	// Recover the database.
	kv, err = Open(config)
	if err != nil {
		t.Fatalf("Error recovering KVStore: %v", err)
	}
	defer kv.CloseAndCleanUp()

	// Check if the key-value pairs are recovered.
	for i := range count {
		value, ok := kv.Get([]byte(fmt.Sprintf("key%d", i)))
		assert.True(t, ok)
		assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
	}
}
