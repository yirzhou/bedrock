package main

// Iterator provides a way to seek and scan over key-value pairs.
type Iterator interface {
	// Seek positions the iterator at the first key that is greater
	// than or equal to the given key.
	Seek(key []byte)

	// Next advances the iterator to the next key in the sequence.
	Next()

	// Key returns the key at the current iterator position.
	// The returned slice is only valid until the next call to Next().
	Key() []byte

	// Value returns the value at the current iterator position.
	// The returned slice is only valid until the next call to Next().
	Value() []byte

	// Valid returns true if the iterator is positioned at a valid key-value pair.
	Valid() bool
}
