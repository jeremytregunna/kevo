package iterator

// Iterator defines the interface for iterating over key-value pairs
// This is used across the storage engine components to provide a consistent
// way to traverse data regardless of where it's stored.
type Iterator interface {
	// SeekToFirst positions the iterator at the first key
	SeekToFirst()

	// SeekToLast positions the iterator at the last key
	SeekToLast()

	// Seek positions the iterator at the first key >= target
	Seek(target []byte) bool

	// Next advances the iterator to the next key
	Next() bool

	// Key returns the current key
	Key() []byte

	// Value returns the current value
	Value() []byte

	// Valid returns true if the iterator is positioned at a valid entry
	Valid() bool

	// IsTombstone returns true if the current entry is a deletion marker
	// This is used during compaction to distinguish between a regular nil value and a tombstone
	IsTombstone() bool
}
