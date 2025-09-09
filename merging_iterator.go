package bedrock

import (
	"bytes"
	"container/heap"
	"log"

	"github.com/yirzhou/bedrock/lib"
)

type mergingIterator struct {
	heap         *iteratorHeap // A min-heap of our other iterators
	currentKey   []byte
	currentValue []byte
	err          error
}

func NewMergingIterator(memtableIter Iterator, segmentIters []Iterator) *mergingIterator {
	// 1. Create a slice of all potential heap items.
	// The capacity is set to the total number of iterators.
	items := make([]*heapItem, 0)

	// Add the memtable iterator with the highest priority (0).
	items = append(items, &heapItem{iterator: memtableIter, priority: 0})

	// Add all segment iterators with decreasing priority.
	// Assumes segmentIters is already sorted from newest to oldest.
	for i, segIter := range segmentIters {
		items = append(items, &heapItem{iterator: segIter, priority: i + 1})
	}

	// 2. Prepare the initial heap slice. We only add iterators that are
	//    actually valid to begin with.
	pq := iteratorHeap{}
	heap.Init(&pq)
	for _, item := range items {
		// Position each iterator at its first element.
		item.iterator.Next()
		if item.iterator.Valid() {
			heap.Push(&pq, item)
		}
	}

	// 3. Now, initialize the heap property on the pre-populated slice.
	// This is more efficient than pushing one by one.
	// heap.Init(&pq)

	// 4. Create the merging iterator.
	iter := &mergingIterator{
		heap: &pq,
	}

	// 5. Prime the iterator by calling Next() to position it at the first key.
	iter.Next()

	return iter
}

func (m *mergingIterator) Next() {
	// This outer loop handles skipping over deleted keys (tombstones).
	for {
		// If the heap is empty, there are no more items. Invalidate the iterator.
		if m.heap.Len() == 0 {
			m.currentKey = nil
			m.currentValue = nil
			return
		}

		// 1. Find the next key to process.
		// The item at the top of the heap has the next key in the global sort order.
		nextKey := m.heap.Peek().(*heapItem).iterator.Key()

		// 2. Pop the winner and store its value immediately, BEFORE advancing.
		winnerItem := heap.Pop(m.heap).(*heapItem)
		winningValue := winnerItem.iterator.Value()

		// 3. Advance the winner's iterator and push it back if it's still valid.
		winnerItem.iterator.Next()
		if winnerItem.iterator.Valid() {
			heap.Push(m.heap, winnerItem)
		}

		// 4. Pop and discard all other older duplicates for this same key.
		for m.heap.Len() > 0 && bytes.Equal(m.heap.Peek().(*heapItem).iterator.Key(), nextKey) {
			itemToDiscard := heap.Pop(m.heap).(*heapItem)
			// Advance the iterator for the item we just processed.
			itemToDiscard.iterator.Next()
			// If it's still valid, push it back onto the heap for the next round.
			if itemToDiscard.iterator.Valid() {
				heap.Push(m.heap, itemToDiscard)
			}
		}

		// 5. Check if the winning record is a tombstone.
		if bytes.Equal(winningValue, lib.TOMBSTONE) {
			// If it's a tombstone, this key is considered deleted. We don't
			// set the iterator's state and instead continue the loop to find
			// the next valid key.
			continue
		}

		// 6. We found a valid, non-deleted key. Set the iterator's state.
		m.currentKey = nextKey
		m.currentValue = winningValue
		return // Exit the loop.
	}
}

// Key returns the key at the current iterator position.
func (m *mergingIterator) Key() []byte {
	return m.currentKey
}

// Value returns the value at the current iterator position.
func (m *mergingIterator) Value() []byte {
	return m.currentValue
}

// Valid returns true if the iterator is positioned at a valid key-value pair.
func (m *mergingIterator) Valid() bool {
	return m.currentKey != nil && m.err == nil
}

// Seek positions the iterator at the first key >= the given key.
// This is an expensive operation.
func (m *mergingIterator) Seek(key []byte) {
	// 1. Seek all underlying iterators to the target key.
	// We need to rebuild the heap from scratch.
	newHeap := make(iteratorHeap, 0, m.heap.Len())
	for _, item := range *m.heap {
		item.iterator.Seek(key)
		if item.iterator.Valid() {
			newHeap = append(newHeap, item)
		}
	}

	// 2. Re-initialize the heap with the newly positioned iterators.
	heap.Init(&newHeap)
	m.heap = &newHeap

	// 3. Prime the iterator by calling Next() to find the first valid key
	//    at or after the seek position.
	m.Next()
}

// ScanRange scans the range of keys between startKey and endKey.
// It calls the provided function for each key-value pair in the range.
//
// Note that the interval is [startKey, endKey), i.e. the endKey is not included.
func ScanRange(iter Iterator, startKey, endKey []byte, fn func(key, value []byte)) {

	// Throws an error if startKey is greater than endKey.
	if bytes.Compare(startKey, endKey) > 0 {
		log.Println("ScanRange: startKey is greater than endKey")
		return
	}

	// 1. Position the iterator at the beginning of the range.
	iter.Seek(startKey)

	// 2. Loop through all subsequent keys.
	for iter.Valid() {
		// 3. Check if we have gone past the end of the range.
		if endKey != nil && bytes.Compare(iter.Key(), endKey) >= 0 {
			break // We've passed the end key.
		}

		// 4. Process the current key-value pair.
		fn(iter.Key(), iter.Value())

		// 5. Move to the next key.
		iter.Next()
	}
}

// CollectRange collects the key-value pairs in the range between startKey and endKey.
//
// It returns a slice of KVRecord.
func (kv *KVStore) CollectRange(startKey, endKey []byte) ([]KVRecord, error) {
	records := []KVRecord{}

	ScanRange(kv.NewIterator(), startKey, endKey, func(key, value []byte) {
		records = append(records, KVRecord{Key: key, Value: value})
	})

	return records, nil
}
