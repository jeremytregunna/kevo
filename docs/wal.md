# Write-Ahead Log (WAL) Package Documentation

The `wal` package implements a durable, crash-resistant Write-Ahead Log for the Kevo engine. It serves as the primary mechanism for ensuring data durability and atomicity, especially during system crashes or power failures.

## Overview

The Write-Ahead Log records all database modifications before they are applied to the main database structures. This follows the "write-ahead logging" principle: all changes must be logged before being applied to the database, ensuring that if a system crash occurs, the database can be recovered to a consistent state by replaying the log.

Key responsibilities of the WAL include:
- Recording database operations in a durable manner
- Supporting atomic batch operations
- Providing crash recovery mechanisms
- Managing log file rotation and cleanup

## File Format and Record Structure

### WAL File Format

WAL files use a `.wal` extension and are named with a timestamp:
```
<timestamp>.wal  (e.g., 01745172985771529746.wal)
```

The timestamp-based naming allows for chronological ordering during recovery.

### Record Format

Records in the WAL have a consistent structure:

```
┌──────────────┬──────────────┬──────────────┬──────────────────────┐
│    CRC-32    │    Length    │    Type      │       Payload        │
│   (4 bytes)  │   (2 bytes)  │   (1 byte)   │    (Length bytes)    │
└──────────────┴──────────────┴──────────────┴──────────────────────┘
     Header (7 bytes)                          Data
```

- **CRC-32**: A checksum of the payload for data integrity verification
- **Length**: The payload length (up to 32KB)
- **Type**: The record type:
  - `RecordTypeFull (1)`: A complete record
  - `RecordTypeFirst (2)`: First fragment of a large record
  - `RecordTypeMiddle (3)`: Middle fragment of a large record
  - `RecordTypeLast (4)`: Last fragment of a large record

Records larger than the maximum size (32KB) are automatically split into multiple fragments.

### Operation Payload Format

For standard operations (Put/Delete), the payload format is:

```
┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│   Op Type    │  Sequence    │   Key Len    │     Key      │  Value Len   │    Value     │
│   (1 byte)   │  (8 bytes)   │  (4 bytes)   │ (Key Len)    │  (4 bytes)   │ (Value Len)  │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
```

- **Op Type**: The operation type:
  - `OpTypePut (1)`: Key-value insertion
  - `OpTypeDelete (2)`: Key deletion
  - `OpTypeMerge (3)`: Value merging (reserved for future use)
  - `OpTypeBatch (4)`: Batch of operations
- **Sequence**: A monotonically increasing sequence number
- **Key Len / Key**: The length and bytes of the key
- **Value Len / Value**: The length and bytes of the value (omitted for delete operations)

## Implementation Details

### Core Components

#### WAL Writer

The `WAL` struct manages writing to the log file and includes:
- Buffered writing for efficiency
- CRC32 checksums for data integrity
- Sequence number management
- Synchronization control based on configuration

#### WAL Reader

The `Reader` struct handles reading and validating records:
- Verifies CRC32 checksums
- Reconstructs fragmented records
- Presents a logical view of entries to consumers

#### Batch Processing

The `Batch` struct handles atomic multi-operation groups:
- Collect multiple operations (Put/Delete)
- Write them as a single atomic unit
- Track operation counts and sizes

### Key Operations

#### Writing Operations

The `Append` method writes a single operation to the log:
1. Assigns a sequence number
2. Computes the required size
3. Determines if fragmentation is needed
4. Writes the record(s) with appropriate headers
5. Syncs to disk based on configuration

#### Batch Operations

The `AppendBatch` method handles writing multiple operations atomically:
1. Writes a batch header with operation count
2. Assigns sequential sequence numbers to operations
3. Writes all operations with the same basic format
4. Syncs to disk based on configuration

#### Record Fragmentation

For records larger than 32KB:
1. The record is split into fragments
2. First fragment (`RecordTypeFirst`) contains metadata and part of the key
3. Middle fragments (`RecordTypeMiddle`) contain continuing data
4. Last fragment (`RecordTypeLast`) contains the final portion

#### Reading and Recovery

The `ReadEntry` method reads entries from the log:
1. Reads a physical record
2. Validates the checksum
3. If it's a fragmented record, collects all fragments
4. Parses the entry data into an `Entry` struct

## Durability Guarantees

The WAL provides configurable durability through three sync modes:

1. **Immediate Sync Mode (`SyncImmediate`)**:
   - Every write is immediately synced to disk
   - Highest durability, lowest performance
   - Data safe even in case of system crash or power failure
   - Suitable for critical data where durability is paramount

2. **Batch Sync Mode (`SyncBatch`)**:
   - Syncs after a configurable amount of data is written
   - Balances durability and performance
   - May lose very recent transactions in case of crash
   - Default setting for most workloads

3. **No Sync Mode (`SyncNone`)**:
   - Relies on OS caching and background flushing
   - Highest performance, lowest durability
   - Data may be lost in case of crash
   - Suitable for non-critical or easily reproducible data

The application can choose the appropriate sync mode based on its durability requirements.

## Recovery Process

WAL recovery happens during engine startup:

1. **WAL File Discovery**:
   - Scan for all `.wal` files in the WAL directory
   - Sort files by timestamp (filename)

2. **Sequential Replay**:
   - Process each file in chronological order
   - For each file, read and validate all records
   - Apply valid operations to rebuild the MemTable

3. **Error Handling**:
   - Skip corrupted records when possible
   - If a file is heavily corrupted, move to the next file
   - As long as one file is processed successfully, recovery continues

4. **Sequence Number Recovery**:
   - Track the highest sequence number seen
   - Update the next sequence number for future operations

5. **WAL Reset**:
   - After recovery, either reuse the last WAL file (if not full)
   - Or create a new WAL file for future operations

The recovery process is designed to be robust against partial corruption and to recover as much data as possible.

## Corruption Handling

The WAL implements several mechanisms to handle and recover from corruption:

1. **CRC32 Checksums**:
   - Every record includes a CRC32 checksum
   - Corrupted records are detected and skipped

2. **Scanning Recovery**:
   - When corruption is detected, the reader can scan ahead
   - Tries to find the next valid record header

3. **Progressive Recovery**:
   - Even if some records are lost, subsequent valid records are processed
   - Files with too many errors are skipped, but recovery continues with later files

4. **Backup Mechanism**:
   - Problematic WAL files can be moved to a backup directory
   - This allows recovery to proceed with a clean slate if needed

## Performance Considerations

### Buffered Writing

The WAL uses buffered I/O to reduce the number of system calls:
- Writes go through a 64KB buffer
- The buffer is flushed when sync is called
- This significantly improves write throughput

### Sync Frequency Trade-offs

The sync frequency directly impacts performance:
- `SyncImmediate`: 1 sync per write operation (slowest, safest)
- `SyncBatch`: 1 sync per N bytes written (configurable balance)
- `SyncNone`: No explicit syncs (fastest, least safe)

### File Size Management

WAL files have a configurable maximum size (default 64MB):
- Full files are closed and new ones created
- This prevents individual files from growing too large
- Facilitates easier backup and cleanup

## Common Usage Patterns

### Basic Usage

```go
// Create a new WAL
cfg := config.NewDefaultConfig("/path/to/data")
myWAL, err := wal.NewWAL(cfg, "/path/to/data/wal")
if err != nil {
    log.Fatal(err)
}

// Append operations
seqNum, err := myWAL.Append(wal.OpTypePut, []byte("key"), []byte("value"))
if err != nil {
    log.Fatal(err)
}

// Ensure durability
if err := myWAL.Sync(); err != nil {
    log.Fatal(err)
}

// Close the WAL when done
if err := myWAL.Close(); err != nil {
    log.Fatal(err)
}
```

### Using Batches for Atomicity

```go
// Create a batch
batch := wal.NewBatch()
batch.Put([]byte("key1"), []byte("value1"))
batch.Put([]byte("key2"), []byte("value2"))
batch.Delete([]byte("key3"))

// Write the batch atomically
startSeq, err := myWAL.AppendBatch(batch.ToEntries())
if err != nil {
    log.Fatal(err)
}
```

### WAL Recovery

```go
// Handler function for each recovered entry
handler := func(entry *wal.Entry) error {
    switch entry.Type {
    case wal.OpTypePut:
        // Apply Put operation
        memTable.Put(entry.Key, entry.Value, entry.SequenceNumber)
    case wal.OpTypeDelete:
        // Apply Delete operation
        memTable.Delete(entry.Key, entry.SequenceNumber)
    }
    return nil
}

// Replay all WAL files in a directory
if err := wal.ReplayWALDir("/path/to/data/wal", handler); err != nil {
    log.Fatal(err)
}
```

## Trade-offs and Limitations

### Write Amplification

The WAL doubles write operations (once to WAL, once to final storage):
- This is a necessary trade-off for durability
- Can be mitigated through batching and appropriate sync modes

### Recovery Time

Recovery time is proportional to the size of the WAL:
- Large WAL files or many operations increase startup time
- Mitigated by regular compaction that makes old WAL files obsolete

### Corruption Resilience

While the WAL can recover from some corruption:
- Severe corruption at the start of a file may render it unreadable
- Header corruption can cause loss of subsequent records
- Partial sync before crash can lead to truncated records

These limitations are managed through:
- Regular WAL rotation
- Multiple independent WAL files
- Robust error handling during recovery