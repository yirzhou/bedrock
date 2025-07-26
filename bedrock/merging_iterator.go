package bedrock

import (
	"bytes"
	"container/heap"
	"log"
	"wal/lib"
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

// Seek is not implemented for the merging iterator in this simple version.
// A full implementation would require re-initializing the heap.
func (m *mergingIterator) Seek(key []byte) {
	// For simplicity, we can just say this is not supported for now.
	// A real implementation would need to Seek() all underlying iterators
	// and rebuild the heap, which is a complex operation.
	log.Println("Seek is not implemented for the merging iterator in this simple version.")
}

// Recursive version of Next() that handles tombstones.
func (m *mergingIterator) NextRecursive() (KVRecord, error) {
	// Find the next global key
	pq := m.heap
	iter := pq.Pop().(*heapItem)

	nextKey := iter.iterator.Key()
	nextValue := iter.iterator.Value()

	// Advance the iterator.
	iter.iterator.Next()
	if iter.iterator.Valid() {
		pq.Push(iter)
	}

	// Handle duplicates.
	for pq.Len() > 0 && bytes.Equal(pq.Peek().(*heapItem).iterator.Key(), nextKey) {
		iter = pq.Pop().(*heapItem)
		iter.iterator.Next()
		if iter.iterator.Valid() {
			pq.Push(iter)
		}
	}
	// Handle tombstones.
	if bytes.Equal(nextValue, lib.TOMBSTONE) {
		// Recursively call Next() to handle tombstones.
		return m.NextRecursive()
	}

	m.currentKey = nextKey
	m.currentValue = nextValue

	return KVRecord{Key: nextKey, Value: nextValue}, nil
}
