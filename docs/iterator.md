# Iterator Package Documentation

The `iterator` package provides a unified interface and implementations for traversing key-value data across the Kevo engine. Iterators are a fundamental abstraction used throughout the system for ordered access to data, regardless of where it's stored.

## Overview

Iterators in the Kevo engine follow a consistent interface pattern that allows components to access data in a uniform way. This enables combining and composing iterators to provide complex data access patterns while maintaining a simple, consistent API.

Key responsibilities of the iterator package include:
- Defining a standard iterator interface
- Providing adapter patterns for implementing iterators
- Implementing specialized iterators for different use cases
- Supporting bounded, composite, and hierarchical iteration

## Iterator Interface

### Core Interface

The core `Iterator` interface defines the contract that all iterators must follow:

```go
type Iterator interface {
    // Positioning methods
    SeekToFirst()                // Position at the first key
    SeekToLast()                 // Position at the last key
    Seek(target []byte) bool     // Position at the first key >= target
    Next() bool                  // Advance to the next key
    
    // Access methods
    Key() []byte                 // Return the current key
    Value() []byte               // Return the current value
    Valid() bool                 // Check if the iterator is valid
    
    // Special methods
    IsTombstone() bool           // Check if current entry is a deletion marker
}
```

This interface is used across all storage layers (MemTable, SSTables, transactions) to provide consistent access to key-value data.

## Iterator Types and Patterns

### Adapter Pattern

The package provides adapter patterns to simplify implementing the full interface:

1. **Base Iterators**:
   - Implement the core interface directly for specific data structures
   - Examples: SkipList iterators, Block iterators

2. **Adapter Wrappers**:
   - Transform existing iterators to provide additional functionality
   - Examples: Bounded iterators, filtering iterators

### Bounded Iterators

Bounded iterators limit the range of keys an iterator will traverse:

1. **Key Range Limiting**:
   - Apply start and end bounds to constrain iteration
   - Skip keys outside the specified range

2. **Implementation Approach**:
   - Wrap an existing iterator
   - Filter out keys outside the desired range
   - Maintain the underlying iterator's properties otherwise

### Composite Iterators

Composite iterators combine multiple source iterators into a single view:

1. **MergingIterator**:
   - Merges multiple iterators into a single sorted stream
   - Handles duplicate keys according to specified policy

2. **Implementation Details**:
   - Maintains a priority queue or similar structure
   - Selects the next appropriate key from all sources
   - Handles edge cases like exhausted sources

### Hierarchical Iterators

Hierarchical iterators implement the LSM tree's multi-level view:

1. **LSM Hierarchy Semantics**:
   - Newer sources (e.g., MemTable) take precedence over older sources (e.g., SSTables)
   - Combines multiple levels into a single, consistent view
   - Respects the "newest version wins" rule for duplicate keys

2. **Source Precedence**:
   - Iterators are provided in order from newest to oldest
   - When multiple sources contain the same key, the newer source's value is used
   - Tombstones (deletion markers) hide older values

## Implementation Details

### Hierarchical Iterator

The `HierarchicalIterator` is a cornerstone of the storage engine:

1. **Source Management**:
   - Maintains an ordered array of source iterators
   - Sources must be provided in newest-to-oldest order
   - Typically includes MemTable, immutable MemTables, and SSTable iterators

2. **Key Selection Algorithm**:
   - During `Seek`, `Next`, etc., examines all valid sources
   - Tracks seen keys to handle duplicates
   - Selects the smallest key that satisfies the operation's constraints
   - For duplicate keys, uses the value from the newest source

3. **Thread Safety**:
   - Mutex protection for concurrent access
   - Safe for concurrent reads, though typically used from one thread

4. **Memory Efficiency**:
   - Lazily fetches values only when needed
   - Doesn't materialize full result set in memory

### Key Selection Process

The key selection process is a critical algorithm in hierarchical iterators:

1. **For `SeekToFirst`**:
   - Position all source iterators at their first key
   - Select the smallest key across all sources, considering duplicates

2. **For `Seek(target)`**:
   - Position all source iterators at the smallest key >= target
   - Select the smallest valid key >= target, considering duplicates

3. **For `Next`**:
   - Remember the current key
   - Advance source iterators past this key
   - Select the smallest key that is > current key

### Tombstone Handling

Tombstones (deletion markers) are handled specially:

1. **Detection**:
   - Identified by `nil` values in most iterators
   - Allows distinguishing between deleted keys and non-existent keys

2. **Impact on Iteration**:
   - Tombstones are visible during direct iteration
   - During merging, tombstones from newer sources hide older values
   - This mechanism enables proper deletion semantics in the LSM tree

## Common Usage Patterns

### Basic Iterator Usage

```go
// Use any Iterator implementation
iter := someSource.NewIterator()

// Iterate through all entries
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("Key: %s, Value: %s\n", iter.Key(), iter.Value())
}

// Or seek to a specific key
if iter.Seek([]byte("target")) {
    fmt.Printf("Found: %s\n", iter.Value())
}
```

### Bounded Range Iterator

```go
// Create a bounded iterator
startKey := []byte("user:1000")
endKey := []byte("user:2000")
rangeIter := bounded.NewBoundedIterator(sourceIter, startKey, endKey)

// Iterate through the bounded range
for rangeIter.SeekToFirst(); rangeIter.Valid(); rangeIter.Next() {
    fmt.Printf("Key: %s\n", rangeIter.Key())
}
```

### Hierarchical Multi-Source Iterator

```go
// Create iterators for each source (newest to oldest)
memTableIter := memTable.NewIterator()
sstableIter1 := sstable1.NewIterator()
sstableIter2 := sstable2.NewIterator()

// Combine them into a hierarchical view
sources := []iterator.Iterator{memTableIter, sstableIter1, sstableIter2}
hierarchicalIter := composite.NewHierarchicalIterator(sources)

// Use the combined view
for hierarchicalIter.SeekToFirst(); hierarchicalIter.Valid(); hierarchicalIter.Next() {
    if !hierarchicalIter.IsTombstone() {
        fmt.Printf("%s: %s\n", hierarchicalIter.Key(), hierarchicalIter.Value())
    }
}
```

## Performance Considerations

### Time Complexity

Iterator operations have the following complexity characteristics:

1. **SeekToFirst/SeekToLast**:
   - O(S) where S is the number of sources
   - Each source may have its own seek complexity

2. **Seek(target)**:
   - O(S * log N) where N is the typical size of each source
   - Binary search within each source, then selection across sources

3. **Next()**:
   - Amortized O(S) for typical cases
   - May require advancing multiple sources past duplicates

4. **Key()/Value()/Valid()**:
   - O(1) - constant time for accessing current state

### Memory Management

Iterator implementations focus on memory efficiency:

1. **Lazy Evaluation**:
   - Values are fetched only when needed
   - No materialization of full result sets

2. **Buffer Reuse**:
   - Key/value buffers are reused where possible
   - Careful copying when needed for correctness

3. **Source Independence**:
   - Each source manages its own memory
   - Composite iterators add minimal overhead

### Optimizations

Several optimizations improve iterator performance:

1. **Key Skipping**:
   - Skip sources that can't contain the target key
   - Early termination when possible

2. **Caching**:
   - Cache recently accessed values
   - Avoid redundant lookups

3. **Batched Advancement**:
   - Advance multiple levels at once when possible
   - Reduces overall iteration cost

## Design Principles

### Interface Consistency

The iterator design follows several key principles:

1. **Uniform Interface**:
   - All iterators share the same interface
   - Allows seamless substitution and composition

2. **Explicit State**:
   - Iterator state is always explicit
   - `Valid()` must be checked before accessing data

3. **Unidirectional Design**:
   - Forward-only iteration for simplicity
   - Backward iteration would add complexity with little benefit

### Composability

The iterators are designed for composition:

1. **Adapter Pattern**:
   - Wrap existing iterators to add functionality
   - Build complex behaviors from simple components

2. **Delegation**:
   - Delegate operations to underlying iterators
   - Apply transformations or filtering as needed

3. **Transparency**:
   - Composite iterators behave like simple iterators
   - Internal complexity is hidden from users

## Integration with Storage Layers

The iterator system integrates with all storage layers:

1. **MemTable Integration**:
   - SkipList-based iterators for in-memory data
   - Priority for recent changes

2. **SSTable Integration**:
   - Block-based iterators for persistent data
   - Efficient seeking through index blocks

3. **Transaction Integration**:
   - Combines buffer and engine state
   - Preserves transaction isolation

4. **Engine Integration**:
   - Provides unified view across all components
   - Handles version selection and visibility