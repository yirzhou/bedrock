package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegmentIterator(t *testing.T) {
	// Create a temporary directory for the test
	dir := t.TempDir()

	// Create a segment file with some records
	segmentPath := filepath.Join(dir, "segment-1")
	file, err := os.Create(segmentPath)
	assert.NoError(t, err)

	records := []KVRecord{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	var index sparseIndex
	var offset int64

	for _, record := range records {
		bytes := getKVRecordBytes(record.Key, record.Value)
		n, err := file.Write(bytes)
		assert.NoError(t, err)
		index = append(index, sparseIndexEntry{key: record.Key, offset: offset})
		offset += int64(n)
	}
	file.Close()

	// Create a new segment iterator
	iterator, err := NewSegmentIterator(segmentPath, index)
	assert.NoError(t, err)

	// Test Next()
	var result []KVRecord
	for iterator.Next(); iterator.Valid(); iterator.Next() {
		result = append(result, KVRecord{Key: iterator.Key(), Value: iterator.Value()})
	}
	assert.Equal(t, len(records), len(result))
	for i, record := range records {
		assert.Equal(t, record.Key, result[i].Key)
		assert.Equal(t, record.Value, result[i].Value)
	}

	// Test Seek()
		iterator.Seek([]byte("key3"))
	assert.True(t, iterator.Valid())
	assert.True(t, bytes.Compare(iterator.Key(), []byte("key3")) >= 0)

	iterator.Seek([]byte("key1"))
	assert.True(t, iterator.Valid())
	assert.True(t, bytes.Compare(iterator.Key(), []byte("key1")) >= 0)

	iterator.Seek([]byte("key5"))
	assert.True(t, iterator.Valid())
	assert.True(t, bytes.Compare(iterator.Key(), []byte("key5")) >= 0)

	iterator.Seek([]byte("key6"))
	assert.False(t, iterator.Valid())

	// Test NewSegmentIterator with non-existent file
	_, err = NewSegmentIterator("non-existent-file", nil)
	assert.Error(t, err)

	// Test Seek() with empty index
	iteratorEmpty, err := NewSegmentIterator(segmentPath, sparseIndex{})
	assert.NoError(t, err)
	iteratorEmpty.Seek([]byte("anykey"))
	assert.False(t, iteratorEmpty.Valid())
	assert.Equal(t, io.EOF, iteratorEmpty.CurrentErr)
}
