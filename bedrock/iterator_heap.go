package bedrock

import "bytes"

// heapItem is a small wrapper used internally by our min-heap.
// It holds an iterator and its priority for correct sorting.
type heapItem struct {
	iterator Iterator
	priority int // Lower number means higher priority (newer data)
}

// iteratorHeap is a helper type to implement Go's heap interface.
type iteratorHeap []*heapItem

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	itemA := h[i]
	itemB := h[j]

	// First, compare by key.
	keyCmp := bytes.Compare(itemA.iterator.Key(), itemB.iterator.Key())
	if keyCmp != 0 {
		return keyCmp < 0 // A smaller key has higher priority.
	}

	// If keys are the same, compare by priority.
	// A lower priority number means it's newer data, so it has higher priority.
	return itemA.priority < itemB.priority
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *iteratorHeap) Peek() interface{} {
	return (*h)[0]
}
