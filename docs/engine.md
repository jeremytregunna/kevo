# Engine Package Documentation

The `engine` package provides the core storage engine functionality for the Kevo project. It implements a facade-based architecture that integrates all components (WAL, MemTable, SSTables, Compaction) into a unified storage system with a clean, modular interface.

## Overview

The Engine is the main entry point for interacting with the storage system. It implements a Log-Structured Merge (LSM) tree architecture through a facade pattern that delegates operations to specialized managers for storage, transactions, and compaction.

Key responsibilities of the Engine include:
- Managing the write path (WAL, MemTable, flush to SSTable)
- Coordinating the read path across multiple storage layers
- Handling concurrency with a single-writer design
- Providing transaction support
- Coordinating background operations like compaction
- Collecting and reporting statistics

## Architecture

### Facade-Based Design

The engine implements a facade pattern that provides a simplified interface to the complex subsystems:

```
┌───────────────────────┐
│     Client Request    │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│     EngineFacade      │
└───────────┬───────────┘
            │
            ▼
┌─────────┬─────────┬─────────┐
│ Storage │   Tx    │ Compact │
│ Manager │ Manager │ Manager │
└─────────┴─────────┴─────────┘
```

1. **EngineFacade**: The main entry point that coordinates all operations
2. **StorageManager**: Handles data storage and retrieval operations
3. **TransactionManager**: Manages transaction lifecycle and isolation
4. **CompactionManager**: Coordinates background compaction processes
5. **Statistics Collector**: Centralized statistics collection

### Components and Data Flow

The engine orchestrates a multi-layered storage hierarchy through its component managers:

```
┌───────────────────┐
│  Client Request   │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐     ┌───────────────────┐
│   EngineFacade    │◄────┤ Statistics Collector  │
└─────────┬─────────┘     └───────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌─────────┐ ┌─────────┐   ┌───────────────────┐
│ Storage │ │   Tx    │◄──┤   Transaction     │
│ Manager │ │ Manager │   │     Buffer        │
└────┬────┘ └─────────┘   └───────────────────┘
     │
┌────┴────┐
▼         ▼
┌─────────┐ ┌─────────┐
│   WAL   │ │MemTable │
└─────────┘ └────┬────┘
                 │
                 ▼
          ┌─────────────┐  ┌───────────────────┐
          │  SSTables   │◄─┤   Compaction      │
          └─────────────┘  │     Manager       │
                          └───────────────────┘
```

### Key Sequence

1. **Write Path**:
   - Client calls `Put()` or `Delete()`
   - EngineFacade delegates to StorageManager
   - Operation is logged in WAL for durability
   - Data is added to the active MemTable
   - When the MemTable reaches its size threshold, it becomes immutable
   - A background process flushes immutable MemTables to SSTables
   - The CompactionManager periodically merges SSTables for better read performance

2. **Read Path**:
   - Client calls `Get()`
   - EngineFacade delegates to StorageManager
   - Storage manager searches for the key in this order:
     a. Active MemTable
     b. Immutable MemTables (if any)
     c. SSTables (from newest to oldest)
   - First occurrence of the key determines the result
   - Tombstones (deletion markers) cause key not found results

3. **Transaction Path**:
   - Client calls `BeginTransaction()`
   - EngineFacade delegates to TransactionManager
   - A new transaction is created (read-only or read-write)
   - Transaction operations are buffered until commit
   - On commit, changes are applied atomically

## Implementation Details

### EngineFacade Structure

The `EngineFacade` struct contains several important fields:

- **Configuration**: The engine's configuration and paths
- **Component Managers**: 
  - `storage`: StorageManager interface for data operations
  - `txManager`: TransactionManager interface for transaction handling
  - `compaction`: CompactionManager interface for compaction operations
- **Statistics**: Centralized stats collector for metrics
- **State**: Flag for engine closed status

### Manager Interfaces

The engine defines clear interfaces for each manager component:

1. **StorageManager Interface**:
   - Data operations: `Get`, `Put`, `Delete`, `IsDeleted`
   - Iterator operations: `GetIterator`, `GetRangeIterator`
   - Management operations: `FlushMemTables`, `ApplyBatch`, `Close`
   - Statistics retrieval: `GetStorageStats`

2. **TransactionManager Interface**:
   - Transaction operations: `BeginTransaction`
   - Statistics retrieval: `GetTransactionStats`

3. **CompactionManager Interface**:
   - Compaction operations: `TriggerCompaction`, `CompactRange`
   - Lifecycle management: `Start`, `Stop`
   - Tombstone tracking: `TrackTombstone`
   - Statistics retrieval: `GetCompactionStats`

### Key Operations

#### Initialization

The `NewEngineFacade()` function initializes a storage engine by:
1. Creating required directories
2. Loading or creating configuration
3. Creating a statistics collector
4. Initializing the storage manager
5. Initializing the transaction manager
6. Setting up the compaction manager
7. Starting background compaction processes

#### Write Operations

The `Put()` and `Delete()` methods follow a similar pattern:
1. Check if engine is closed
2. Track the operation start in statistics
3. Delegate to the storage manager
4. Track operation latency and bytes
5. Handle any errors

#### Read Operations

The `Get()` method:
1. Check if engine is closed
2. Track the operation start in statistics
3. Delegate to the storage manager
4. Track operation latency and bytes read
5. Handle errors appropriately (distinguishing between "not found" and other errors)

#### Transaction Support

The `BeginTransaction()` method:
1. Check if engine is closed
2. Track the operation start in statistics
3. Handle legacy transaction creation for backward compatibility
4. Delegate to the transaction manager
5. Track operation latency
6. Return the created transaction

## Statistics Collection

The engine implements a comprehensive statistics collection system:

1. **Atomic Collector**:
   - Thread-safe statistics collection
   - Minimal contention using atomic operations
   - Tracks operations, latencies, bytes, and errors

2. **Component-Specific Stats**:
   - Each manager contributes its own statistics
   - Storage stats (sstable count, memtable size, etc.)
   - Transaction stats (started, committed, aborted)
   - Compaction stats (compaction count, time spent, etc.)

3. **Metrics Categories**:
   - Operation counts (puts, gets, deletes)
   - Latency measurements (min, max, average)
   - Resource usage (bytes read/written)
   - Error tracking

## Transaction Support

The engine provides ACID-compliant transactions through the TransactionManager:

1. **Atomicity**: WAL logging and atomic batch operations
2. **Consistency**: Single-writer architecture
3. **Isolation**: Reader-writer concurrency control
4. **Durability**: WAL ensures operations are persisted before being considered committed

Transactions are created using the `BeginTransaction()` method, which returns a `Transaction` interface with these key methods:
- `Get()`, `Put()`, `Delete()`: For data operations
- `NewIterator()`, `NewRangeIterator()`: For scanning data
- `Commit()`, `Rollback()`: For transaction control
- `IsReadOnly()`: For checking transaction type

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
- Latency measurements

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
eng, err := engine.NewEngineFacade("/path/to/data")
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

## Extensibility and Modularity

The facade-based architecture provides several advantages:

1. **Clean Separation of Concerns**:
   - Storage logic is isolated from transaction handling
   - Compaction runs independently from core data operations
   - Statistics collection has minimal impact on performance

2. **Interface-Based Design**:
   - All components interact through well-defined interfaces
   - Makes testing and mocking much easier
   - Allows for alternative implementations

3. **Dependency Injection**:
   - Managers receive their dependencies explicitly
   - Simplifies unit testing and component replacement
   - Improves code clarity and maintainability

## Comparison with Other Storage Engines

Unlike many production storage engines like RocksDB or LevelDB, the Kevo engine emphasizes:

1. **Simplicity**: Clear Go implementation with minimal dependencies
2. **Educational Value**: Code readability over absolute performance
3. **Composability**: Clean interfaces for higher-level abstractions
4. **Modularity**: Facade pattern for clear component separation

Features present in the Kevo engine:
- Atomic operations and transactions
- Hierarchical storage with LSM tree architecture
- Background compaction for performance optimization
- Comprehensive statistics collection
- Bloom filters for improved performance (in the SSTable layer)

Features missing compared to production engines:
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
- Modular design allows targeted optimizations