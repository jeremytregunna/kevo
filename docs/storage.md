# Storage Package Documentation

The `storage` package implements the storage management layer for the Kevo engine. It provides a unified interface to the underlying storage components (WAL, MemTable, SSTable) and handles the data persistence and retrieval operations.

## Overview

The Storage Manager is a core component of the Kevo engine's facade-based architecture. It encapsulates the details of how data is stored, retrieved, and maintained across multiple storage layers, providing a clean interface for the rest of the engine to use.

Key responsibilities of the storage package include:
- Managing the write path (WAL and MemTable updates)
- Coordinating the read path across storage layers
- Handling MemTable flushing to SSTables
- Providing iterators for sequential data access
- Managing the lifecycle of storage components
- Collecting and reporting storage-specific statistics

## Architecture

### Component Structure

The storage package consists of several interrelated components:

```
┌───────────────────────┐
│    Storage Manager    │◄─────┐
└───────────┬───────────┘      │
            │                  │
            ▼                  │
┌───────────────────────┐      │
│    MemTable Pool      │      │
└───────────┬───────────┘      │
            │                  │
            ▼                  │
┌─────────┬─────────┬─────────┐      ┌───────────────────────┐
│ Active  │ Immut.  │  SST    │      │    Statistics         │
│MemTable │MemTables│ Readers │      │    Collector          │
└─────────┴─────────┴─────────┘      └───────────────────────┘
            │                                    ▲
            ▼                                    │
┌───────────────────────┐                       │
│   Write-Ahead Log     │───────────────────────┘
└───────────────────────┘
```

1. **StorageManager**: Implements the `StorageManager` interface
2. **MemTablePool**: Manages active and immutable MemTables
3. **Storage Components**: Active MemTable, Immutable MemTables, and SSTable readers
4. **Write-Ahead Log**: Ensures durability for write operations
5. **Statistics Collector**: Records storage metrics and performance data

## Implementation Details

### Manager Implementation

The `Manager` struct implements the `StorageManager` interface:

```go
type Manager struct {
    // Configuration and paths
    cfg        *config.Config
    dataDir    string
    sstableDir string
    walDir     string

    // Core components
    wal          *wal.WAL
    memTablePool *memtable.MemTablePool
    sstables     []*sstable.Reader

    // State management
    nextFileNum uint64
    lastSeqNum  uint64
    bgFlushCh   chan struct{}
    closed      atomic.Bool

    // Statistics
    stats stats.Collector

    // Concurrency control
    mu      sync.RWMutex
    flushMu sync.Mutex
}
```

This structure centralizes all storage components and provides thread-safe access to them.

### Key Operations

#### Data Operations

The manager implements the core data operations defined in the `StorageManager` interface:

1. **Put Operation**:
   ```go
   func (m *Manager) Put(key, value []byte) error {
       m.mu.Lock()
       defer m.mu.Unlock()
       
       // Append to WAL
       seqNum, err := m.wal.Append(wal.OpTypePut, key, value)
       if err != nil {
           return err
       }
       
       // Add to MemTable
       m.memTablePool.Put(key, value, seqNum)
       m.lastSeqNum = seqNum
       
       // Check if MemTable needs to be flushed
       if m.memTablePool.IsFlushNeeded() {
           if err := m.scheduleFlush(); err != nil {
               return err
           }
       }
       
       return nil
   }
   ```

2. **Get Operation**:
   ```go
   func (m *Manager) Get(key []byte) ([]byte, error) {
       m.mu.RLock()
       defer m.mu.RUnlock()
       
       // Check the MemTablePool (active + immutables)
       if val, found := m.memTablePool.Get(key); found {
           // Check if it's a deletion marker
           if val == nil {
               return nil, engine.ErrKeyNotFound
           }
           return val, nil
       }
       
       // Check the SSTables (from newest to oldest)
       for i := len(m.sstables) - 1; i >= 0; i-- {
           val, err := m.sstables[i].Get(key)
           if err == nil {
               return val, nil
           }
           if err != sstable.ErrKeyNotFound {
               return nil, err
           }
       }
       
       return nil, engine.ErrKeyNotFound
   }
   ```

3. **Delete Operation**:
   ```go
   func (m *Manager) Delete(key []byte) error {
       m.mu.Lock()
       defer m.mu.Unlock()
       
       // Append to WAL
       seqNum, err := m.wal.Append(wal.OpTypeDelete, key, nil)
       if err != nil {
           return err
       }
       
       // Add deletion marker to MemTable
       m.memTablePool.Delete(key, seqNum)
       m.lastSeqNum = seqNum
       
       // Check if MemTable needs to be flushed
       if m.memTablePool.IsFlushNeeded() {
           if err := m.scheduleFlush(); err != nil {
               return err
           }
       }
       
       return nil
   }
   ```

#### MemTable Management

The storage manager is responsible for MemTable lifecycle management:

1. **MemTable Flushing**:
   ```go
   func (m *Manager) FlushMemTables() error {
       m.flushMu.Lock()
       defer m.flushMu.Unlock()
       
       // Get immutable MemTables
       tables := m.memTablePool.GetImmutableMemTables()
       if len(tables) == 0 {
           return nil
       }
       
       // Create a new WAL file for future writes
       if err := m.rotateWAL(); err != nil {
           return err
       }
       
       // Flush each immutable MemTable
       for _, memTable := range tables {
           if err := m.flushMemTable(memTable); err != nil {
               return err
           }
       }
       
       return nil
   }
   ```

2. **Scheduling Flush**:
   ```go
   func (m *Manager) scheduleFlush() error {
       // Get the MemTable that needs to be flushed
       immutable := m.memTablePool.SwitchToNewMemTable()
       
       // Schedule background flush
       select {
       case m.bgFlushCh <- struct{}{}:
           // Signal sent successfully
       default:
           // A flush is already scheduled
       }
       
       return nil
   }
   ```

#### Iterator Support

The manager provides iterator functionality for sequential access:

1. **Full Iterator**:
   ```go
   func (m *Manager) GetIterator() (iterator.Iterator, error) {
       m.mu.RLock()
       defer m.mu.RUnlock()
       
       // Create a hierarchical iterator that combines all sources
       return m.newHierarchicalIterator(), nil
   }
   ```

2. **Range Iterator**:
   ```go
   func (m *Manager) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
       m.mu.RLock()
       defer m.mu.RUnlock()
       
       // Create a hierarchical iterator with range bounds
       iter := m.newHierarchicalIterator()
       iter.SetBounds(startKey, endKey)
       return iter, nil
   }
   ```

### Statistics Tracking

The manager integrates with the statistics collection system:

```go
func (m *Manager) GetStorageStats() map[string]interface{} {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    stats := make(map[string]interface{})
    
    // Add MemTable statistics
    stats["memtable_size"] = m.memTablePool.GetActiveMemTableSize()
    stats["immutable_memtable_count"] = len(m.memTablePool.GetImmutableMemTables())
    
    // Add SSTable statistics
    stats["sstable_count"] = len(m.sstables)
    
    // Add sequence number information
    stats["last_sequence"] = m.lastSeqNum
    
    return stats
}
```

## Integration with Engine Facade

The Storage Manager is a critical component in the engine's facade pattern:

1. **Initialization**:
   ```go
   func NewEngineFacade(dataDir string) (*EngineFacade, error) {
       // ...
       
       // Create the statistics collector
       statsCollector := stats.NewAtomicCollector()
       
       // Create the storage manager
       storageManager, err := storage.NewManager(cfg, statsCollector)
       if err != nil {
           return nil, fmt.Errorf("failed to create storage manager: %w", err)
       }
       
       // ...
   }
   ```

2. **Operation Delegation**:
   ```go
   func (e *EngineFacade) Put(key, value []byte) error {
       // Track the operation
       e.stats.TrackOperation(stats.OpPut)
       
       // Delegate to storage manager
       err := e.storage.Put(key, value)
       
       // Track operation result
       // ...
       
       return err
   }
   ```

## Performance Considerations

### Concurrency Model

The storage manager uses a careful concurrency approach:

1. **Read-Write Lock**:
   - Main lock (`mu`) is a reader-writer lock
   - Allows concurrent reads but exclusive writes
   - Core to the single-writer architecture

2. **Flush Lock**:
   - Separate lock (`flushMu`) for flush operations
   - Prevents concurrent flushes while allowing reads

3. **Lock Granularity**:
   - Fine-grained locking for better concurrency
   - Critical sections are kept as small as possible

### Memory Usage

Memory management is a key concern:

1. **MemTable Sizing**:
   - Configurable MemTable size (default 32MB)
   - Automatic flushing when threshold is reached
   - Prevents unbounded memory growth

2. **Resource Release**:
   - Prompt release of immutable MemTables after flush
   - Careful handling of file descriptors for SSTables

### I/O Optimization

Several I/O optimizations are implemented:

1. **Sequential Writes**:
   - Append-only WAL writes are sequential for high performance
   - SSTable creation uses sequential writes

2. **Memory-Mapped Reading**:
   - SSTables use memory mapping for efficient reading
   - Leverages OS-level caching for frequently accessed data

3. **Batched Operations**:
   - Support for batched writes through `ApplyBatch`
   - Reduces WAL overhead for multiple operations

## Common Usage Patterns

### Direct Usage

While typically used through the EngineFacade, the storage manager can be used directly:

```go
// Create a storage manager
cfg := config.NewDefaultConfig("/path/to/data")
stats := stats.NewAtomicCollector()
manager, err := storage.NewManager(cfg, stats)
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// Perform operations
err = manager.Put([]byte("key"), []byte("value"))
if err != nil {
    log.Fatal(err)
}

value, err := manager.Get([]byte("key"))
if err != nil {
    log.Fatal(err)
}
```

### Batch Operations

For multiple operations, batch processing is more efficient:

```go
// Create a batch of operations
entries := []*wal.Entry{
    {Type: wal.OpTypePut, Key: []byte("key1"), Value: []byte("value1")},
    {Type: wal.OpTypePut, Key: []byte("key2"), Value: []byte("value2")},
    {Type: wal.OpTypeDelete, Key: []byte("key3")},
}

// Apply the batch atomically
err = manager.ApplyBatch(entries)
if err != nil {
    log.Fatal(err)
}
```

### Iterator Usage

The manager provides iterators for sequential access:

```go
// Get an iterator
iter, err := manager.GetIterator()
if err != nil {
    log.Fatal(err)
}

// Iterate through all entries
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
}

// Get a range iterator
rangeIter, err := manager.GetRangeIterator([]byte("a"), []byte("m"))
if err != nil {
    log.Fatal(err)
}

// Iterate through the bounded range
for rangeIter.SeekToFirst(); rangeIter.Valid(); rangeIter.Next() {
    fmt.Printf("%s: %s\n", rangeIter.Key(), rangeIter.Value())
}
```

## Design Principles

### Single-Writer Architecture

The storage manager follows a single-writer architecture:

1. **Write Exclusivity**:
   - Only one write operation can proceed at a time
   - Simplifies concurrency model and prevents race conditions

2. **Concurrent Reads**:
   - Multiple reads can proceed concurrently
   - No blocking between readers

3. **Sequential Consistency**:
   - Operations appear to execute in a sequential order
   - No anomalies from concurrent modifications

### Error Handling

The storage manager uses a comprehensive error handling approach:

1. **Clear Error Types**:
   - Distinct error types for different failure scenarios
   - Proper error wrapping for context preservation

2. **Recovery Mechanisms**:
   - WAL recovery after crashes
   - Corruption detection and handling

3. **Resource Cleanup**:
   - Proper cleanup on error paths
   - Prevents resource leaks

### Separation of Concerns

The manager separates different responsibilities:

1. **Component Independence**:
   - WAL handles durability
   - MemTable handles in-memory storage
   - SSTables handle persistent storage

2. **Clear Boundaries**:
   - Well-defined interfaces between components
   - Each component has a specific role

3. **Lifecycle Management**:
   - Proper initialization and cleanup
   - Resource acquisition and release