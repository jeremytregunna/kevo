# MemTable Package Documentation

The `memtable` package implements an in-memory data structure for the Kevo engine. MemTables are a key component of the LSM tree architecture, providing fast, sorted, in-memory storage for recently written data before it's flushed to disk as SSTables.

## Overview

MemTables serve as the primary write buffer for the storage engine, allowing efficient processing of write operations before they are persisted to disk. The implementation uses a skiplist data structure to provide fast insertions, retrievals, and ordered iteration.

Key responsibilities of the MemTable include:
- Providing fast in-memory writes
- Supporting efficient key lookups
- Offering ordered iteration for range scans
- Tracking tombstones for deleted keys
- Supporting atomic transitions between mutable and immutable states

## Architecture

### Core Components

The MemTable package consists of several interrelated components:

1. **SkipList**: The core data structure providing O(log n) operations.
2. **MemTable**: A wrapper around SkipList with additional functionality.
3. **MemTablePool**: A manager for active and immutable MemTables.
4. **Recovery**: Mechanisms for rebuilding MemTables from WAL entries.

```
┌─────────────────┐
│  MemTablePool   │
└───────┬─────────┘
        │
┌───────┴─────────┐      ┌─────────────────┐
│ Active MemTable │      │   Immutable     │
└───────┬─────────┘      │   MemTables     │
        │                └─────────────────┘
┌───────┴─────────┐
│    SkipList     │
└─────────────────┘
```

## Implementation Details

### SkipList Data Structure

The SkipList is a probabilistic data structure that allows fast operations by maintaining multiple layers of linked lists:

1. **Nodes**: Each node contains:
   - Entry data (key, value, sequence number, value type)
   - Height information
   - Next pointers at each level

2. **Probabilistic Height**: New nodes get a random height following a probabilistic distribution:
   - Height 1: 100% of nodes
   - Height 2: 25% of nodes
   - Height 3: 6.25% of nodes, etc.

3. **Search Algorithm**:
   - Starts at the highest level of the head node
   - Moves forward until finding a node greater than the target
   - Drops down a level and continues
   - This gives O(log n) expected time for operations

4. **Concurrency Considerations**:
   - Uses atomic operations for pointer manipulation
   - Cache-aligned node structure

### Memory Management

The MemTable implementation includes careful memory management:

1. **Size Tracking**:
   - Each entry's size is estimated (key length + value length + overhead)
   - Running total maintained using atomic operations

2. **Resource Limits**:
   - Configurable maximum size (default 32MB)
   - Age-based limits (configurable maximum age)
   - When limits are reached, the MemTable becomes immutable

3. **Memory Overhead**:
   - Skip list nodes add overhead (pointers at each level)
   - Overhead is controlled by limiting maximum height (12 by default)
   - Bracing factor of 4 provides good balance between height and width

### Entry Types and Tombstones

The MemTable supports two types of entries:

1. **Value Entries** (`TypeValue`):
   - Normal key-value pairs
   - Stored with their sequence number

2. **Deletion Tombstones** (`TypeDeletion`):
   - Markers indicating a key has been deleted
   - Value is nil, but the key and sequence number are preserved
   - Essential for proper deletion semantics in the LSM tree architecture

### MemTablePool

The MemTablePool manages multiple MemTables:

1. **Active MemTable**:
   - Single mutable MemTable for current writes
   - Becomes immutable when size/age thresholds are reached

2. **Immutable MemTables**:
   - Former active MemTables waiting to be flushed to disk
   - Read-only, no modifications allowed
   - Still available for reads while awaiting flush

3. **Lifecycle Management**:
   - Monitors size and age of active MemTable
   - Triggers transitions from active to immutable
   - Creates new active MemTable when needed

### Iterator Functionality

MemTables provide iterator interfaces for sequential access:

1. **Forward Iteration**:
   - `SeekToFirst()`: Position at the first entry
   - `Seek(key)`: Position at or after the given key  
   - `Next()`: Move to the next entry
   - `Valid()`: Check if the current position is valid

2. **Entry Access**:
   - `Key()`: Get the current entry's key
   - `Value()`: Get the current entry's value
   - `IsTombstone()`: Check if the current entry is a deletion marker

3. **Iterator Adapters**:
   - Adapters to the common iterator interface for the engine

## Concurrency and Isolation

MemTables employ a concurrency model suited for the storage engine's architecture:

1. **Read Concurrency**:
   - Multiple readers can access MemTables concurrently
   - Read locks are used for concurrent Get operations

2. **Write Isolation**:
   - The single-writer architecture ensures only one writer at a time
   - Writes to the active MemTable use write locks

3. **Immutable State**:
   - Once a MemTable becomes immutable, no further modifications occur
   - This provides a simple isolation model

4. **Atomic Transitions**:
   - The transition from mutable to immutable is atomic
   - Uses atomic boolean for immutable state flag

## Recovery Process

The recovery functionality rebuilds MemTables from WAL data:

1. **WAL Entries**:
   - Each WAL entry contains an operation type, key, value and sequence number
   - Entries are processed in order to rebuild the MemTable state

2. **Sequence Number Handling**:
   - Maximum sequence number is tracked during recovery
   - Ensures future operations have larger sequence numbers

3. **Batch Operations**:
   - Support for atomic batch operations from WAL
   - Batch entries contain multiple operations with sequential sequence numbers

## Performance Considerations

### Time Complexity

The SkipList data structure offers favorable complexity for MemTable operations:

| Operation | Average Case | Worst Case |
|-----------|--------------|------------|
| Insert    | O(log n)     | O(n)       |
| Lookup    | O(log n)     | O(n)       |
| Delete    | O(log n)     | O(n)       |
| Iteration | O(1) per step| O(1) per step |

### Memory Usage Optimization

Several optimizations are employed to improve memory efficiency:

1. **Shared Memory Allocations**:
   - Node arrays allocated in contiguous blocks
   - Reduces allocation overhead

2. **Cache Awareness**:
   - Nodes aligned to cache lines (64 bytes) 
   - Improves CPU cache utilization

3. **Appropriate Sizing**:
   - Default sizing (32MB) provides good balance
   - Configurable based on workload needs

### Write Amplification

MemTables help reduce write amplification in the LSM architecture:

1. **Buffering Writes**:
   - Multiple key updates are consolidated in memory
   - Only the latest value gets written to disk

2. **Batching**:
   - Many small writes batched into larger disk operations
   - Improves overall I/O efficiency

## Common Usage Patterns

### Basic Usage

```go
// Create a new MemTable
memTable := memtable.NewMemTable()

// Add entries with incrementing sequence numbers
memTable.Put([]byte("key1"), []byte("value1"), 1)
memTable.Put([]byte("key2"), []byte("value2"), 2)
memTable.Delete([]byte("key3"), 3)

// Retrieve a value
value, found := memTable.Get([]byte("key1"))
if found {
    fmt.Printf("Value: %s\n", value)
}

// Check if the MemTable is too large
if memTable.ApproximateSize() > 32*1024*1024 {
    memTable.SetImmutable()
    // Write to disk...
}
```

### Using MemTablePool

```go
// Create a pool with configuration
config := config.NewDefaultConfig("/path/to/data")
pool := memtable.NewMemTablePool(config)

// Add entries
pool.Put([]byte("key1"), []byte("value1"), 1)
pool.Delete([]byte("key2"), 2)

// Check if flushing is needed
if pool.IsFlushNeeded() {
    // Switch to a new active MemTable and get the old one for flushing
    immutable := pool.SwitchToNewMemTable()
    
    // Flush the immutable table to disk as an SSTable
    // ...
}
```

### Iterating Over Entries

```go
// Create an iterator
iter := memTable.NewIterator()

// Iterate through all entries
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("%s: ", iter.Key())
    
    if iter.IsTombstone() {
        fmt.Println("<deleted>")
    } else {
        fmt.Printf("%s\n", iter.Value())
    }
}

// Or seek to a specific point
iter.Seek([]byte("key5"))
if iter.Valid() {
    fmt.Printf("Found: %s\n", iter.Key())
}
```

## Configuration Options

The MemTable behavior can be tuned through several configuration parameters:

1. **MemTableSize** (default: 32MB):
   - Maximum size before triggering a flush
   - Larger sizes improve write throughput but increase memory usage

2. **MaxMemTables** (default: 4):
   - Maximum number of MemTables in memory (active + immutable)
   - Higher values allow more in-flight flushes

3. **MaxMemTableAge** (default: 600 seconds):
   - Maximum age before forcing a flush
   - Ensures data isn't held in memory too long

## Trade-offs and Limitations

### Write Bursts and Flush Stalls

High write bursts can lead to multiple MemTables becoming immutable before the background flush process completes. The system handles this by:

1. Maintaining multiple immutable MemTables in memory
2. Tracking the number of immutable MemTables
3. Potentially slowing down writes if too many immutable MemTables accumulate

### Memory Usage vs. Performance

The MemTable configuration involves balancing memory usage against performance:

1. **Larger MemTables**:
   - Pro: Better write performance, fewer disk flushes
   - Con: Higher memory usage, potentially longer recovery time

2. **Smaller MemTables**:
   - Pro: Lower memory usage, faster recovery
   - Con: More frequent flushes, potentially lower write throughput

### Ordering and Consistency

The MemTable maintains ordering via:

1. **Key Comparison**: Primary ordering by key
2. **Sequence Numbers**: Secondary ordering to handle updates to the same key
3. **Value Types**: Distinguishing between values and deletion markers

This ensures consistent state even with concurrent reads while a background flush is occurring.