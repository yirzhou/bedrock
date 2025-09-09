package bedrock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Snapshot Sink for Testing ---

// mockSnapshotSink captures the data written during a snapshot for verification.
type mockSnapshotSink struct {
	*bytes.Buffer
}

func (m *mockSnapshotSink) ID() string {
	return "mock-sink"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

// --- FSM Unit Tests ---

func TestFSMApply(t *testing.T) {
	// Setup a minimal KVStore for testing.
	// In a real test suite, this would be a reusable helper.
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	require.NoError(t, err)
	defer kv.CloseAndCleanUp()

	t.Run("should apply PUT command", func(t *testing.T) {
		// Create a Put command log entry
		putCmd := encodePutCommand([]byte("key1"), []byte("value1"))
		logEntry := &raft.Log{Data: putCmd}

		// Apply the command
		resp := kv.Apply(logEntry)
		assert.Nil(t, resp)

		// Verify the state of the memtable
		val, ok := kv.memState.Get([]byte("key1"))
		assert.True(t, ok)
		assert.Equal(t, []byte("value1"), val)
	})

	t.Run("should apply DELETE command", func(t *testing.T) {
		// First, put a value so we can delete it.
		kv.memState.Put([]byte("key2"), []byte("value2"))

		// Create a Delete command log entry
		deleteCmd := encodeDeleteCommand([]byte("key2"))
		logEntry := &raft.Log{Data: deleteCmd}

		// Apply the command
		resp := kv.Apply(logEntry)
		assert.Nil(t, resp)

		// Verify the state of the memtable (it should now be a tombstone)
		_, ok := kv.memState.Get([]byte("key2"))
		assert.False(t, ok)
	})
}

func TestFSMSnapshot_MemtableOnly(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	require.NoError(t, err)
	defer kv.CloseAndCleanUp()

	// Add data to the memtable
	kv.memState.Put([]byte("key1"), []byte("value1"))
	kv.memState.Put([]byte("key2"), []byte("value2"))

	// Create the snapshot
	fsmSnapshot, err := kv.Snapshot()
	require.NoError(t, err)

	// Persist the snapshot to our mock sink
	sink := &mockSnapshotSink{Buffer: new(bytes.Buffer)}
	err = fsmSnapshot.Persist(sink)
	require.NoError(t, err)

	// Verify the contents of the snapshot sink
	snapshotBytes := sink.Bytes()

	// Check the block type
	blockType := snapshotBytes[0]
	assert.Equal(t, byte(SNAPSHOT_MEMSTATE_FILE_TYPE), blockType)

	// Check the length
	dataLen := binary.LittleEndian.Uint32(snapshotBytes[1:5])
	assert.Equal(t, uint32(len(snapshotBytes)-5), dataLen)

	// (A more robust test would deserialize the memtable data and verify its contents)
}

func TestFSMRestore(t *testing.T) {
	dir := t.TempDir()
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir))
	require.NoError(t, err)
	defer kv.CloseAndCleanUp()

	// 0. Add some data to the memtable.
	kv.Put([]byte("key1"), []byte("value1"))
	kv.Put([]byte("key2"), []byte("value2"))

	// 1. Create a mock snapshot of a memtable.
	memtableData, err := kv.memState.Serialize() // Assuming a simple serialize method
	require.NoError(t, err)

	snapshotBuf := new(bytes.Buffer)
	snapshotBuf.WriteByte(byte(SNAPSHOT_MEMSTATE_FILE_TYPE))
	binary.Write(snapshotBuf, binary.LittleEndian, uint32(len(memtableData)))
	snapshotBuf.Write(memtableData)

	// 2. Create a ReadCloser from the buffer.
	snapshotReader := io.NopCloser(snapshotBuf)

	// 3. Restore the FSM from the snapshot.
	err = kv.Restore(snapshotReader)
	require.NoError(t, err)

	// 4. Verify the state.
	val, ok := kv.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value1"), val)

	val, ok = kv.Get([]byte("key2"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value2"), val)
}

func TestFSM_SnapshotRestore_FileBased(t *testing.T) {
	// --- Setup Phase ---
	dir1 := t.TempDir()
	kv1, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir1).WithEnableCheckpoint(false).WithEnableSyncCheckpoint(false))
	require.NoError(t, err)
	count := 100
	for i := range count {
		kv1.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	// Put data and trigger a checkpoint to create segment files.
	err = kv1.doCheckpoint(false) // Run a full checkpoint
	require.NoError(t, err)
	kv1.Close() // Close the original DB

	// Re-open to ensure state is correct before snapshotting
	kv1, err = Open(NewConfigurationNoMaintenance().WithBaseDir(dir1))
	require.NoError(t, err)
	for i := range count {
		val, ok := kv1.Get([]byte(fmt.Sprintf("key%d", i)))
		assert.True(t, ok)
		assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), val)
	}
	defer kv1.CloseAndCleanUp()

	// --- Snapshot Phase ---
	fsmSnapshot, err := kv1.Snapshot()
	require.NoError(t, err)

	sink := &mockSnapshotSink{Buffer: new(bytes.Buffer)}
	err = fsmSnapshot.Persist(sink)
	require.NoError(t, err)

	// --- Restore Phase ---
	dir2 := t.TempDir()
	kv2, err := Open(NewConfigurationNoMaintenance().WithBaseDir(dir2))
	require.NoError(t, err)
	defer kv2.CloseAndCleanUp()

	// Restore the new, empty KV store from the snapshot
	snapshotReader := io.NopCloser(sink.Buffer)
	err = kv2.Restore(snapshotReader)
	require.NoError(t, err)

	// --- Verification Phase ---
	// The new store should have loaded the levels and indexes from the snapshot.
	assert.NotEmpty(t, kv2.levels)
	assert.NotEmpty(t, kv2.levels[0])

	// A Get request for a key that was in the original segment file should now work.
	for i := range count {
		val, ok := kv2.Get([]byte(fmt.Sprintf("key%d", i)))
		assert.True(t, ok)
		assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), val)
	}
}
