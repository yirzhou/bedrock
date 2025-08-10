package main

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemStateGetSparseIndex(t *testing.T) {
	memState := NewMemState()
	segmentID := uint64(1)
	sparseIndexEntries := sparseIndex{
		{key: []byte("key1"), offset: 0},
		{key: []byte("key2"), offset: 10},
	}
	memState.AddSparseIndexEntriesBulk(segmentID, sparseIndexEntries)

	// Test case 1: Segment ID found
	retrievedIndex := memState.GetSparseIndex(segmentID)
	assert.Equal(t, sparseIndexEntries, retrievedIndex)

	// Test case 2: Segment ID not found
	retrievedIndex = memState.GetSparseIndex(uint64(2))
	assert.Empty(t, retrievedIndex)
}

func TestMemStatePrint(t *testing.T) {
	memState := NewMemState()
	memState.Put([]byte("key1"), []byte("value1"))
	memState.Put([]byte("key2"), []byte("value2"))

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	memState.Print()

	w.Close()
	out, _ := io.ReadAll(r)
	os.Stdout = oldStdout // Restore stdout

	expected := "========== MemState starts ==========\nkey1: value1\nkey2: value2\n========== MemState ends ==========\n"
	// The order of key-value pairs in the map is not guaranteed, so we need to check both orders.
	expected2 := "========== MemState starts ==========\nkey2: value2\nkey1: value1\n========== MemState ends ==========\n"

	assert.True(t, string(out) == expected || string(out) == expected2)
}

func TestMemStateGetAllSegmentIDsDescendingOrder(t *testing.T) {
	memState := NewMemState()

	// Test case 1: No segment IDs
	segmentIDs, err := memState.GetAllSegmentIDsDescendingOrder()
	assert.NoError(t, err)
	assert.Empty(t, segmentIDs)

	// Test case 2: With segment IDs
	memState.AddSparseIndexEntriesBulk(uint64(1), sparseIndex{})
	memState.AddSparseIndexEntriesBulk(uint64(3), sparseIndex{})
	memState.AddSparseIndexEntriesBulk(uint64(2), sparseIndex{})

	segmentIDs, err = memState.GetAllSegmentIDsDescendingOrder()
	assert.NoError(t, err)
	assert.Equal(t, []uint64{3, 2, 1}, segmentIDs)
}
