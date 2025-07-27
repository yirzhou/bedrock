package bedrock

import "sort"

// memtableIterator implements the Iterator interface for a memtable.
type memtableIterator struct {
	memtable *MemState // A reference to the memtable
	keys     []string  // A sorted slice of the memtable's keys
	position int       // The current index in the 'keys' slice
	kv       *KVStore  // A reference to the KVStore
}

func (m *MemState) NewIterator() *memtableIterator {
	// Extract all keys from the map.
	keys := make([]string, 0, len(m.state))
	for k := range m.state {
		keys = append(keys, k)
	}
	// Sort the keys lexicographically.
	sort.Strings(keys)

	return &memtableIterator{
		memtable: m,
		keys:     keys,
		position: -1, // Start before the first element
	}
}

func (m *memtableIterator) Seek(key []byte) {
	// Binary search to find the first key greater than or equal to the given key.
	i := sort.SearchStrings(m.keys, string(key))
	m.position = i
}

func (m *memtableIterator) Next() {
	m.position++
}

func (m *memtableIterator) Key() []byte {
	return []byte(m.keys[m.position])
}

func (m *memtableIterator) Value() []byte {
	return m.memtable.state[m.keys[m.position]]
}

func (m *memtableIterator) Valid() bool {
	return m.position >= 0 && m.position < len(m.keys)
}
