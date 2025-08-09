package main

import (
	"testing"
	"bedrock/lib"

	"github.com/stretchr/testify/assert"
)

// --- Mock Iterator for Testing ---

// mockIterator is a simple, in-memory iterator implementation used for testing.
type mockIterator struct {
	records []KVRecord
	pos     int
}

// newMockIterator creates a new iterator from a slice of KVRecords.
func newMockIterator(records []KVRecord) *mockIterator {
	return &mockIterator{
		records: records,
		pos:     -1, // Start before the first element
	}
}

func (it *mockIterator) Seek(key []byte) { /* Not needed for these tests */ }
func (it *mockIterator) Next() {
	it.pos++
}
func (it *mockIterator) Key() []byte {
	if it.Valid() {
		return it.records[it.pos].Key
	}
	return nil
}
func (it *mockIterator) Value() []byte {
	if it.Valid() {
		return it.records[it.pos].Value
	}
	return nil
}
func (it *mockIterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.records)
}

// --- Helper function to collect all results from an iterator ---

func collectAll(iter Iterator) []KVRecord {
	var results []KVRecord
	for iter.Valid() {
		results = append(results, KVRecord{Key: iter.Key(), Value: iter.Value()})
		iter.Next()
	}
	return results
}

// --- Unit Tests for mergingIterator ---

func TestMergingIterator_SimpleMerge(t *testing.T) {
	memtableIter := newMockIterator([]KVRecord{
		{Key: []byte("b"), Value: []byte("memtable_b")},
	})
	segmentIter1 := newMockIterator([]KVRecord{
		{Key: []byte("a"), Value: []byte("segment_a")},
		{Key: []byte("c"), Value: []byte("segment_c")},
	})

	mergingIter := NewMergingIterator(memtableIter, []Iterator{segmentIter1})
	results := collectAll(mergingIter)

	expected := []KVRecord{
		{Key: []byte("a"), Value: []byte("segment_a")},
		{Key: []byte("b"), Value: []byte("memtable_b")},
		{Key: []byte("c"), Value: []byte("segment_c")},
	}

	assert.Equal(t, expected, results)
}

func TestMergingIterator_Deduplication(t *testing.T) {
	// Memtable is newest (priority 0)
	memtableIter := newMockIterator([]KVRecord{
		{Key: []byte("apple"), Value: []byte("newest_apple")},
		{Key: []byte("cat"), Value: []byte("memtable_cat")},
	})
	// Segment 1 is next newest (priority 1)
	segmentIter1 := newMockIterator([]KVRecord{
		{Key: []byte("apple"), Value: []byte("old_apple")},
		{Key: []byte("banana"), Value: []byte("segment1_banana")},
	})
	// Segment 2 is oldest (priority 2)
	segmentIter2 := newMockIterator([]KVRecord{
		{Key: []byte("banana"), Value: []byte("older_banana")},
		{Key: []byte("dog"), Value: []byte("segment2_dog")},
	})

	mergingIter := NewMergingIterator(memtableIter, []Iterator{segmentIter1, segmentIter2})
	results := collectAll(mergingIter)

	expected := []KVRecord{
		{Key: []byte("apple"), Value: []byte("newest_apple")},     // Should pick the memtable version
		{Key: []byte("banana"), Value: []byte("segment1_banana")}, // Should pick the segment 1 version
		{Key: []byte("cat"), Value: []byte("memtable_cat")},
		{Key: []byte("dog"), Value: []byte("segment2_dog")},
	}

	assert.Equal(t, expected, results)
}

func TestMergingIterator_Tombstones(t *testing.T) {
	// Memtable has a tombstone for "apple"
	memtableIter := newMockIterator([]KVRecord{
		{Key: []byte("apple"), Value: lib.TOMBSTONE},
	})
	// Segment has an old value for "apple" and a valid value for "banana"
	segmentIter1 := newMockIterator([]KVRecord{
		{Key: []byte("apple"), Value: []byte("old_apple")},
		{Key: []byte("banana"), Value: []byte("value_banana")},
	})

	mergingIter := NewMergingIterator(memtableIter, []Iterator{segmentIter1})
	results := collectAll(mergingIter)

	// The iterator should completely skip "apple" because the newest version is a tombstone.
	expected := []KVRecord{
		{Key: []byte("banana"), Value: []byte("value_banana")},
	}

	assert.Equal(t, expected, results)
}

func TestMergingIterator_EmptySources(t *testing.T) {
	memtableIter := newMockIterator([]KVRecord{}) // Empty memtable
	segmentIter1 := newMockIterator([]KVRecord{
		{Key: []byte("a"), Value: []byte("segment_a")},
		{Key: []byte("b"), Value: []byte("segment_b")},
	})
	segmentIter2 := newMockIterator([]KVRecord{}) // Empty segment

	mergingIter := NewMergingIterator(memtableIter, []Iterator{segmentIter1, segmentIter2})
	results := collectAll(mergingIter)

	expected := []KVRecord{
		{Key: []byte("a"), Value: []byte("segment_a")},
		{Key: []byte("b"), Value: []byte("segment_b")},
	}

	assert.Equal(t, expected, results)
}

func TestMergingIterator_ScanRange(t *testing.T) {
	kv, err := Open(NewConfigurationNoMaintenance().WithBaseDir(t.TempDir()).WithEnableCheckpoint(true).WithEnableSyncCheckpoint(true).WithEnableCompaction(false))
	assert.NoError(t, err)
	defer kv.CloseAndCleanUp()

	count := 26
	txn := kv.BeginTransaction()
	for i := range count {
		txn.Put([]byte(string(rune('a'+i))), []byte(string(rune('a'+i))))
	}
	txn.Commit()

	iter := kv.NewIterator()
	results := []KVRecord{}
	ScanRange(iter, []byte("a"), []byte("z"), func(key, value []byte) {
		results = append(results, KVRecord{Key: key, Value: value})
	})

	assert.Equal(t, count-1, len(results))
	for i := range count - 1 {
		assert.Equal(t, []byte(string(rune('a'+i))), results[i].Key)
		assert.Equal(t, []byte(string(rune('a'+i))), results[i].Value)
	}

	// Test CollectRange
	records, err := kv.CollectRange([]byte("a"), []byte("z"))
	assert.NoError(t, err)
	assert.Equal(t, count-1, len(records))
	for i := range count - 1 {
		assert.Equal(t, []byte(string(rune('a'+i))), records[i].Key)
		assert.Equal(t, []byte(string(rune('a'+i))), records[i].Value)
	}
}
