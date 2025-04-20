# Engine Package Documentation

The `engine` package provides the core storage engine functionality for the Kevo project. It integrates all components (WAL, MemTable, SSTables, Compaction) into a unified storage system with a simple interface.

## Overview

The Engine is the main entry point for interacting with the storage system. It implements a Log-Structured Merge (LSM) tree architecture, which provides efficient writes and reasonable read performance for key-value storage.

Key responsibilities of the Engine include:
- Managing the write path (WAL, MemTable, flush to SSTable)
- Coordinating the read path across multiple storage layers
- Handling concurrency with a single-writer design
- Providing transaction support
- Coordinating background operations like compaction

## Architecture

### Components and Data Flow

The engine orchestrates a multi-layered storage hierarchy:

```
┌───────────────────┐
│  Client Request   │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌───────────────────┐
│      Engine       │◄────┤   Transactions    │
└─────────┬─────────┘     └───────────────────┘
          │
          ▼
┌───────────────────┐     ┌───────────────────┐
│  Write-Ahead Log  │     │    Statistics     │
└─────────┬─────────┘     └───────────────────┘
          │
          ▼
┌───────────────────┐
│     MemTable      │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌───────────────────┐
│  Immutable MTs    │◄────┤    Background     │
└─────────┬─────────┘     │      Flush        │
          │               └───────────────────┘
          ▼
┌───────────────────┐     ┌───────────────────┐
│     SSTables      │◄────┤     Compaction    │
└───────────────────┘     └───────────────────┘
```

### Key Sequence

1. **Write Path**:
   - Client calls `Put()` or `Delete()`
   - Operation is logged in WAL for durability
   - Data is added to the active MemTable
   - When the MemTable reaches its size threshold, it becomes immutable
   - A background process flushes immutable MemTables to SSTables
   - Periodically, compaction merges SSTables for better read performance

2. **Read Path**:
   - Client calls `Get()`
   - Engine searches for the key in this order:
     a. Active MemTable
     b. Immutable MemTables (if any)
     c. SSTables (from newest to oldest)
   - First occurrence of the key determines the result
   - Tombstones (deletion markers) cause key not found results

## Implementation Details

### Engine Structure

The Engine struct contains several important fields:

- **Configuration**: The engine's configuration and paths
- **Storage Components**: WAL, MemTable pool, and SSTable readers
- **Concurrency Control**: Locks for coordination
- **State Management**: Tracking variables for file numbers, sequence numbers, etc.
- **Background Processes**: Channels and goroutines for background tasks

### Key Operations

#### Initialization

The `NewEngine()` function initializes a storage engine by:
1. Creating required directories
2. Loading or creating configuration
3. Initializing the WAL
4. Creating a MemTable pool
5. Loading existing SSTables
6. Recovering data from WAL if necessary
7. Starting background tasks for flushing and compaction

#### Write Operations

The `Put()` and `Delete()` methods follow a similar pattern:
1. Acquire a write lock
2. Append the operation to the WAL
3. Update the active MemTable
4. Check if the MemTable needs to be flushed
5. Release the lock

#### Read Operations

The `Get()` method:
1. Acquires a read lock
2. Checks the MemTable for the key
3. If not found, checks SSTables in order from newest to oldest
4. Handles tombstones (deletion markers) appropriately
5. Returns the value or a "key not found" error

#### MemTable Flushing

When a MemTable becomes full:
1. The `scheduleFlush()` method switches to a new active MemTable
2. The filled MemTable becomes immutable
3. A background process flushes the immutable MemTable to an SSTable

#### SSTable Management

SSTables are organized by level for compaction:
- Level 0 contains SSTables directly flushed from MemTables
- Higher levels are created through compaction
- Keys may overlap between SSTables in Level 0
- Keys are non-overlapping between SSTables in higher levels

## Transaction Support

The engine provides ACID-compliant transactions through:

1. **Atomicity**: WAL logging and atomic batch operations
2. **Consistency**: Single-writer architecture
3. **Isolation**: Reader-writer concurrency control (similar to SQLite)
4. **Durability**: WAL ensures operations are persisted before being considered committed

Transactions are created using the `BeginTransaction()` method, which returns a `Transaction` interface with these key methods:
- `Get()`, `Put()`, `Delete()`: For data operations
- `NewIterator()`, `NewRangeIterator()`: For scanning data
- `Commit()`, `Rollback()`: For transaction control

## Error Handling

The engine handles various error conditions:
- File system errors during WAL and SSTable operations
- Memory limitations
- Concurrency issues
- Recovery from crashes

Key errors that may be returned include:
- `ErrEngineClosed`: When operations are attempted on a closed engine
- `ErrKeyNotFound`: When a key is not found during retrieval

## Performance Considerations

### Statistics

The engine maintains detailed statistics for monitoring:
- Operation counters (puts, gets, deletes)
- Hit and miss rates
- Bytes read and written
- Flush counts and MemTable sizes
- Error tracking

These statistics can be accessed via the `GetStats()` method.

### Tuning Parameters

Performance can be tuned through the configuration parameters:
- MemTable size
- WAL sync mode
- SSTable block size
- Compaction settings

### Resource Management

The engine manages resources to prevent excessive memory usage:
- MemTables are flushed when they reach a size threshold
- Background processing prevents memory buildup
- File descriptors for SSTables are managed carefully

## Common Usage Patterns

### Basic Usage

```go
// Create an engine
eng, err := engine.NewEngine("/path/to/data")
if err != nil {
    log.Fatal(err)
}
defer eng.Close()

// Store and retrieve data
err = eng.Put([]byte("key"), []byte("value"))
if err != nil {
    log.Fatal(err)
}

value, err := eng.Get([]byte("key"))
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Value: %s\n", value)
```

### Using Transactions

```go
// Begin a transaction
tx, err := eng.BeginTransaction(false) // false = read-write transaction
if err != nil {
    log.Fatal(err)
}

// Perform operations in the transaction
err = tx.Put([]byte("key1"), []byte("value1"))
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

### Iterating Over Keys

```go
// Get an iterator for all keys
iter, err := eng.GetIterator()
if err != nil {
    log.Fatal(err)
}

// Iterate from the first key
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
}

// Get an iterator for a specific range
rangeIter, err := eng.GetRangeIterator([]byte("start"), []byte("end"))
if err != nil {
    log.Fatal(err)
}

// Iterate through the range
for rangeIter.SeekToFirst(); rangeIter.Valid(); rangeIter.Next() {
    fmt.Printf("%s: %s\n", rangeIter.Key(), rangeIter.Value())
}
```

## Comparison with Other Storage Engines

Unlike many production storage engines like RocksDB or LevelDB, the Kevo engine prioritizes:

1. **Simplicity**: Clear Go implementation with minimal dependencies
2. **Educational Value**: Code readability over absolute performance
3. **Composability**: Clean interfaces for higher-level abstractions
4. **Single-Node Focus**: No distributed features to complicate the design

Features missing compared to production engines:
- Bloom filters (optional enhancement)
- Advanced caching systems
- Complex compression schemes
- Multi-node distribution capabilities

## Limitations and Trade-offs

- **Write Amplification**: LSM-trees involve multiple writes of the same data
- **Read Amplification**: May need to check multiple layers for a single key
- **Space Amplification**: Some space overhead for tombstones and overlapping keys
- **Background Compaction**: Performance may be affected by background compaction

However, the design mitigates these issues:
- Efficient in-memory structures minimize disk accesses
- Hierarchical iterators optimize range scans
- Compaction strategies reduce read amplification over time