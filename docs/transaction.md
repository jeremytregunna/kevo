# Transaction Package Documentation

The `transaction` package implements ACID-compliant transactions for the Kevo engine. It provides a way to group multiple read and write operations into atomic units, ensuring data consistency and isolation.

## Overview

Transactions in the Kevo engine follow a SQLite-inspired concurrency model using reader-writer locks. This approach provides a simple yet effective solution for concurrent access, allowing multiple simultaneous readers while ensuring exclusive write access.

Key responsibilities of the transaction package include:
- Implementing atomic operations (all-or-nothing semantics)
- Managing isolation between concurrent transactions
- Providing a consistent view of data during transactions
- Supporting both read-only and read-write transactions
- Handling transaction commit and rollback

## Architecture

### Key Components

The transaction system consists of several interrelated components:

```
┌───────────────────────┐
│   Transaction (API)   │
└───────────┬───────────┘
            │
┌───────────▼───────────┐      ┌───────────────────────┐
│  TransactionManager   │◄─────┤     EngineFacade      │
└───────────┬───────────┘      └───────────────────────┘
            │
            ▼
┌───────────▼───────────┐      ┌───────────────────────┐
│  EngineTransaction    │◄─────┤   StorageManager      │
└───────────┬───────────┘      └───────────────────────┘
            │
            ▼
┌───────────────────────┐      ┌───────────────────────┐
│     TxBuffer          │◄─────┤   Transaction          │
└───────────────────────┘      │      Iterators         │
                               └───────────────────────┘
```

1. **Transaction Interface**: The public API for transaction operations
2. **TransactionManager**: Handles transaction creation and tracking
3. **EngineTransaction**: Implementation of the Transaction interface
4. **StorageManager**: Provides the underlying storage operations
5. **TxBuffer**: In-memory storage for uncommitted changes
6. **Transaction Iterators**: Special iterators that merge buffer and database state

## ACID Properties Implementation

### Atomicity

Transactions ensure all-or-nothing semantics through several mechanisms:

1. **Write Buffering**:
   - All writes are stored in an in-memory buffer during the transaction
   - No changes are applied to the database until commit

2. **Batch Commit**:
   - At commit time, all changes are submitted as a single batch
   - The WAL (Write-Ahead Log) ensures the batch is atomic

3. **Rollback Support**:
   - Discarding the buffer effectively rolls back all changes
   - No cleanup needed since changes weren't applied to the database

### Consistency

The engine maintains data consistency through:

1. **Single-Writer Architecture**:
   - Only one write transaction can be active at a time
   - Prevents inconsistent states from concurrent modifications

2. **Write-Ahead Logging**:
   - All changes are logged before being applied
   - System can recover to a consistent state after crashes

3. **Key Ordering**:
   - Keys are maintained in sorted order throughout the system
   - Ensures consistent iteration and range scan behavior

### Isolation

The transaction system provides isolation using a simple but effective approach:

1. **Reader-Writer Locks**:
   - Read-only transactions acquire shared (read) locks
   - Read-write transactions acquire exclusive (write) locks
   - Multiple readers can execute concurrently
   - Writers have exclusive access

2. **Read Snapshot Semantics**:
   - Readers see a consistent snapshot of the database
   - New writes by other transactions aren't visible

3. **Isolation Level**:
   - Effectively provides "serializable" isolation
   - Transactions execute as if they were run one after another

### Durability

Durability is ensured through the WAL (Write-Ahead Log):

1. **WAL Integration**:
   - Transaction commits are written to the WAL first
   - Only after WAL sync are changes considered committed

2. **Sync Options**:
   - Transactions can use different WAL sync modes
   - Configurable trade-off between performance and durability

## Implementation Details

### Transaction Lifecycle

A transaction follows this lifecycle:

1. **Creation**:
   - Read-only: Acquires a read lock
   - Read-write: Acquires a write lock (exclusive)

2. **Operation Phase**:
   - Read operations check the buffer first, then the engine
   - Write operations are stored in the buffer only

3. **Commit**:
   - Read-only: Simply releases the read lock
   - Read-write: Applies buffered changes via a WAL batch, then releases write lock

4. **Rollback**:
   - Discards the buffer
   - Releases locks
   - Marks transaction as closed

### Transaction Buffer

The transaction buffer is an in-memory staging area for changes:

1. **Buffering Mechanism**:
   - Stores key-value pairs and deletion markers
   - Maintains sorted order for efficient iteration
   - Deduplicates repeated operations on the same key

2. **Precedence Rules**:
   - Buffer operations take precedence over engine values
   - Latest operation on a key within the buffer wins

3. **Tombstone Handling**:
   - Deletions are stored as tombstones in the buffer
   - Applied to the engine only on commit

### Transaction Iterators

Specialized iterators provide a merged view of buffer and engine data:

1. **Merged View**:
   - Combines data from both the transaction buffer and the underlying engine
   - Buffer entries take precedence over engine entries for the same key

2. **Range Iterators**:
   - Support bounded iterations within a key range
   - Enforce bounds checking on both buffer and engine data

3. **Deletion Handling**:
   - Skip tombstones during iteration
   - Hide engine keys that are deleted in the buffer

## Concurrency Control

### Reader-Writer Lock Model

The transaction system uses a simple reader-writer lock approach:

1. **Lock Acquisition**:
   - Read-only transactions acquire shared (read) locks
   - Read-write transactions acquire exclusive (write) locks

2. **Concurrency Patterns**:
   - Multiple read-only transactions can run concurrently
   - Read-write transactions run exclusively (no other transactions)
   - Writers block new readers, but don't interrupt existing ones

3. **Lock Management**:
   - Locks are acquired at transaction start
   - Released at commit or rollback
   - Safety mechanisms prevent multiple releases

### Isolation Level

The system provides serializable isolation:

1. **Serializable Semantics**:
   - Transactions behave as if executed one after another
   - No anomalies like dirty reads, non-repeatable reads, or phantoms

2. **Implementation Strategy**:
   - Simple locking approach
   - Write exclusivity ensures no write conflicts
   - Read snapshots provide consistent views

3. **Optimistic vs. Pessimistic**:
   - Uses a pessimistic approach with up-front locking
   - Avoids need for validation or aborts due to conflicts

## Common Usage Patterns

### Basic Transaction Usage

```go
// Start a read-write transaction
tx, err := engine.BeginTransaction(false) // false = read-write
if err != nil {
    log.Fatal(err)
}

// Perform operations
err = tx.Put([]byte("key1"), []byte("value1"))
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

value, err := tx.Get([]byte("key2"))
if err != nil && err != engine.ErrKeyNotFound {
    tx.Rollback()
    log.Fatal(err)
}

// Delete a key
err = tx.Delete([]byte("key3"))
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

### Read-Only Transactions

```go
// Start a read-only transaction
tx, err := engine.BeginTransaction(true) // true = read-only
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback() // Safe to call even after commit

// Perform read operations
value, err := tx.Get([]byte("key1"))
if err != nil && err != engine.ErrKeyNotFound {
    log.Fatal(err)
}

// Iterate over a range of keys
iter := tx.NewRangeIterator([]byte("start"), []byte("end"))
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
}

// Commit (for read-only, this just releases resources)
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

### Batch Operations

```go
// Start a read-write transaction
tx, err := engine.BeginTransaction(false)
if err != nil {
    log.Fatal(err)
}

// Perform multiple operations
for i := 0; i < 100; i++ {
    key := []byte(fmt.Sprintf("key%d", i))
    value := []byte(fmt.Sprintf("value%d", i))
    
    if err := tx.Put(key, value); err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
}

// Commit as a single atomic batch
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}
```

## Performance Considerations

### Transaction Overhead

Transactions introduce some overhead compared to direct engine operations:

1. **Locking Overhead**:
   - Acquiring and releasing locks has some cost
   - Write transactions block other transactions

2. **Memory Usage**:
   - Transaction buffers consume memory
   - Large transactions with many changes need more memory

3. **Commit Cost**:
   - WAL batch writes and syncs add latency at commit time
   - More changes in a transaction means higher commit cost

### Optimization Strategies

Several strategies can improve transaction performance:

1. **Transaction Sizing**:
   - Very large transactions increase memory pressure
   - Very small transactions have higher per-operation overhead
   - Find a balance based on your workload

2. **Read-Only Preference**:
   - Use read-only transactions when possible
   - They allow concurrency and have lower overhead

3. **Batch Similar Operations**:
   - Group similar operations in a transaction
   - Reduces overall transaction count

4. **Key Locality**:
   - Group operations on related keys
   - Improves cache locality and iterator efficiency

## Limitations and Trade-offs

### Concurrency Model Limitations

The simple locking approach has some trade-offs:

1. **Writer Blocking**:
   - Only one writer at a time limits write throughput
   - Long-running write transactions block other writers

2. **No Write Concurrency**:
   - Unlike some databases, no support for row/key-level locking
   - Entire database is locked for writes

3. **No Deadlock Detection**:
   - Simple model doesn't need deadlock detection
   - But also can't handle complex lock acquisition patterns

### Error Handling

Transaction error handling requires some care:

1. **Commit Errors**:
   - If commit fails, data is not persisted
   - Application must decide whether to retry or report error

2. **Rollback After Errors**:
   - Always rollback after encountering errors
   - Prevents leaving locks held

3. **Resource Leaks**:
   - Unclosed transactions can lead to lock leaks
   - Use defer for Rollback() to ensure cleanup

## Advanced Concepts

### Potential Future Enhancements

Several enhancements could improve the transaction system:

1. **Optimistic Concurrency**:
   - Allow concurrent write transactions with validation at commit time
   - Could improve throughput for workloads with few conflicts

2. **Finer-Grained Locking**:
   - Key-range locks or partitioned locks
   - Would allow more concurrency for non-overlapping operations

3. **Savepoints**:
   - Partial rollback capability within transactions
   - Useful for complex operations with recovery points

4. **Nested Transactions**:
   - Support for transactions within transactions
   - Would enable more complex application logic