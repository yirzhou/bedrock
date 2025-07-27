package bedrock

import (
	"bytes"
	"io"
	"log"
	"os"
	"slices"
)

type segmentIterator struct {
	file         *os.File    // The file handle for the segment file
	index        sparseIndex // The in-memory sparse index for this segment
	currentKey   []byte
	currentValue []byte
	currentErr   error // To store any errors encountered during iteration

}

func NewSegmentIterator(segmentPath string, index sparseIndex) (*segmentIterator, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		log.Printf("Error opening segment file %s: %v", segmentPath, err)
		return nil, err
	}
	// Read the file content into the sparse index.

	return &segmentIterator{
		file:  file,
		index: index,
		// currentKey starts as nil, making Valid() initially false.
	}, nil
}

// Perform a binary search on it.index to find the correct starting offset.

// Use it.file.Seek() to jump the file cursor to that disk offset.

// Reset the bufio.Reader to start reading from this new position.

// Call it.Next() one or more times to scan forward from that offset until you find the first key that is >= the seek key.
func (s *segmentIterator) Seek(key []byte) {
	// Use a helper struct for the search to work with BinarySearchFunc
	target := sparseIndexEntry{key: key}
	cmp := func(a, b sparseIndexEntry) int {
		return bytes.Compare(a.key, b.key)
	}

	// idx is the index of the first entry >= key, or len(s.index) if not found.
	idx, _ := slices.BinarySearchFunc(s.index, target, cmp)

	// We need to seek to the block that *could* contain our key.
	// This is the block that starts with the largest key that is <= our target key.
	// If idx > 0, this is the block at idx - 1.
	if idx > 0 {
		idx--
	}

	// If the index is empty or the seek key is smaller than all indexed keys,
	// idx will be 0, which is correct.
	if idx >= len(s.index) {
		s.currentErr = io.EOF
		s.currentKey = nil
		return
	}

	// Get the offset from the sparse index.
	offset := s.index[idx].offset

	// 1. Seek the underlying file handle to the correct starting block.
	_, err := s.file.Seek(offset, io.SeekStart)
	if err != nil {
		s.currentErr = err
		s.currentKey = nil
		return
	}

	// 2. Now, scan forward from that position until we find a key that is
	//    greater than or equal to the key we're looking for.
	for {
		s.Next() // Read the next record

		// If we've run out of records or the current key is what we want (or past it), stop.
		if !s.Valid() || bytes.Compare(s.Key(), key) >= 0 {
			break
		}
	}
}

func (s *segmentIterator) Next() {
	// Decode the next key-value pair from the segment file.
	record, err := getNextKVRecord(s.file)
	if err != nil {
		log.Println("Next: Error decoding next key-value pair:", err)
		// Invalidate the iterator.
		s.currentErr = err
		s.currentKey = nil
		return
	}
	s.currentKey = record.Key
	s.currentValue = record.Value
}

func (s *segmentIterator) Key() []byte {
	return s.currentKey
}

func (s *segmentIterator) Value() []byte {
	return s.currentValue
}

func (s *segmentIterator) Valid() bool {
	return s.currentKey != nil && s.currentErr == nil
}
