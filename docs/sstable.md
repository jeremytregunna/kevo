# SSTable Package Documentation

The `sstable` package implements the Sorted String Table (SSTable) persistent storage format for the Kevo engine. SSTables are immutable, ordered files that store key-value pairs and are optimized for efficient reading, particularly for range scans.

## Overview

SSTables form the persistent storage layer of the LSM tree architecture in the Kevo engine. They store key-value pairs in sorted order, with a hierarchical structure that allows efficient retrieval with minimal disk I/O.

Key responsibilities of the SSTable package include:
- Writing sorted key-value pairs to immutable files
- Reading and searching data efficiently
- Providing iterators for sequential access
- Ensuring data integrity with checksums
- Supporting efficient binary search through block indexing

## File Format Specification

The SSTable file format is designed for efficient storage and retrieval of sorted key-value pairs. It follows a structured layout with multiple layers of organization:

```
┌─────────────────────────────────────────────────────────────────┐
│                          Data Blocks                            │
├─────────────────────────────────────────────────────────────────┤
│                          Index Block                            │
├─────────────────────────────────────────────────────────────────┤
│                            Footer                               │
└─────────────────────────────────────────────────────────────────┘
```

### 1. Data Blocks

The bulk of an SSTable consists of data blocks, each containing a series of key-value entries:

- Keys are sorted lexicographically within and across blocks
- Keys are compressed using a prefix compression technique
- Each block has restart points where full keys are stored
- Data blocks have a default target size of 16KB
- Each block includes:
  - Entry data (compressed keys and values)
  - Restart point offsets
  - Restart point count
  - Checksum for data integrity

### 2. Index Block

The index block is a special block that allows efficient location of data blocks:

- Contains one entry per data block
- Each entry includes:
  - First key in the data block
  - Offset of the data block in the file
  - Size of the data block
- Allows binary search to locate the appropriate data block for a key

### 3. Footer

The footer is a fixed-size section at the end of the file containing metadata:

- Index block offset
- Index block size
- Total entry count
- Min/max key offsets (for future use)
- Magic number for file format verification
- Footer checksum

### Block Format

Each block (both data and index) has the following internal format:

```
┌──────────────────────┬─────────────────┬──────────┬──────────┐
│     Entry Data       │ Restart Points  │  Count   │ Checksum │
└──────────────────────┴─────────────────┴──────────┴──────────┘
```

Entry data consists of a series of entries, each with:
1. For restart points: full key length, full key
2. For other entries: shared prefix length, unshared length, unshared key bytes
3. Value length, value data

## Implementation Details

### Core Components

#### Writer

The `Writer` handles creating new SSTable files:

1. **FileManager**: Handles file I/O and atomic file creation
2. **BlockManager**: Manages building and serializing data blocks
3. **IndexBuilder**: Constructs the index block from data block metadata

The write process follows these steps:
1. Collect sorted key-value pairs
2. Build data blocks when they reach target size
3. Track index information as blocks are written
4. Build and write the index block
5. Write the footer
6. Finalize the file with atomic rename

#### Reader

The `Reader` provides access to data in SSTable files:

1. **File handling**: Memory-maps the file for efficient access
2. **Footer parsing**: Reads metadata to locate index and blocks
3. **Block cache**: Optionally caches recently accessed blocks
4. **Search algorithm**: Binary search through the index, then within blocks

The read process follows these steps:
1. Parse the footer to locate the index block
2. Binary search the index to find the appropriate data block
3. Read and parse the data block
4. Binary search within the block for the specific key

#### Block Handling

The block system includes several specialized components:

1. **Block Builder**: Constructs blocks with prefix compression
2. **Block Reader**: Parses serialized blocks
3. **Block Iterator**: Provides sequential access to entries in a block

### Key Features

#### Prefix Compression

To reduce storage space, keys are stored using prefix compression:

1. Blocks have "restart points" at regular intervals (default every 16 keys)
2. At restart points, full keys are stored
3. Between restart points, keys store:
   - Length of shared prefix with previous key
   - Length of unshared suffix
   - Unshared suffix bytes

This provides significant space savings for keys with common prefixes.

#### Memory Mapping

For efficient reading, SSTable files are memory-mapped:

1. File data is mapped into virtual memory
2. OS handles paging and read-ahead
3. Reduces system call overhead
4. Allows direct access to file data without explicit reads

#### Tombstones

SSTables support deletion through tombstone markers:

1. Tombstones are stored as entries with nil values
2. They indicate a key has been deleted
3. Compaction eventually removes tombstones and deleted keys

#### Checksum Verification

Data integrity is ensured through checksums:

1. Each block has a 64-bit xxHash checksum
2. The footer also has a checksum
3. Checksums are verified when blocks are read
4. Corrupted blocks trigger appropriate error handling

## Block Structure and Index Format

### Data Block Structure

Data blocks are the primary storage units in an SSTable:

```
┌────────┬────────┬─────────────┐ ┌────────┬────────┬─────────────┐
│Entry 1 │Entry 2 │    ...      │ │Restart │ Count  │  Checksum   │
│        │        │             │ │ Points │        │             │
└────────┴────────┴─────────────┘ └────────┴────────┴─────────────┘
   Entry Data (Variable Size)          Block Footer (Fixed Size)
```

Each entry in a data block has the following format:

For restart points:
```
┌───────────┬───────────┬───────────┬───────────┐
│ Key Length│    Key    │Value Length│   Value   │
│  (2 bytes)│ (variable)│  (4 bytes) │(variable) │
└───────────┴───────────┴───────────┴───────────┘
```

For non-restart points (using prefix compression):
```
┌───────────┬───────────┬───────────┬───────────┬───────────┐
│  Shared   │ Unshared  │ Unshared  │   Value   │   Value   │
│   Length  │  Length   │    Key    │  Length   │           │
│ (2 bytes) │ (2 bytes) │(variable) │ (4 bytes) │(variable) │
└───────────┴───────────┴───────────┴───────────┴───────────┘
```

### Index Block Structure

The index block has a similar structure to data blocks but contains entries that point to data blocks:

```
┌─────────────────┬─────────────────┬──────────┬──────────┐
│   Index Entries │  Restart Points │  Count   │ Checksum │
└─────────────────┴─────────────────┴──────────┴──────────┘
```

Each index entry contains:
- Key: First key in the corresponding data block
- Value: Block offset (8 bytes) + block size (4 bytes)

### Footer Format

The footer is a fixed-size structure at the end of the file:

```
┌─────────────┬────────────┬────────────┬────────────┬────────────┬─────────┐
│    Index    │   Index    │   Entry    │    Min     │    Max     │ Checksum│
│   Offset    │    Size    │   Count    │Key Offset  │Key Offset  │         │
│  (8 bytes)  │ (4 bytes)  │ (4 bytes)  │ (8 bytes)  │ (8 bytes)  │(8 bytes)│
└─────────────┴────────────┴────────────┴────────────┴────────────┴─────────┘
```

## Performance Considerations

### Read Optimization

SSTables are heavily optimized for read operations:

1. **Block Structure**: The block-based approach minimizes I/O
2. **Block Size Tuning**: Default 16KB balances random vs. sequential access
3. **Memory Mapping**: Efficient OS-level caching
4. **Two-level Search**: Index search followed by block search
5. **Restart Points**: Balance between compression and lookup speed

### Space Efficiency

Several techniques reduce storage requirements:

1. **Prefix Compression**: Reduces space for similar keys
2. **Delta Encoding**: Used in the index for block offsets
3. **Configurable Block Size**: Can be tuned for specific workloads

### I/O Patterns

Understanding I/O patterns helps optimize performance:

1. **Sequential Writes**: SSTables are written sequentially
2. **Random Reads**: Point lookups may access arbitrary blocks
3. **Range Scans**: Sequential reading of multiple blocks
4. **Index Loading**: Always loaded first for any operation

## Iterators and Range Scans

### Iterator Types

The SSTable package provides several iterators:

1. **Block Iterator**: Iterates within a single block
2. **SSTable Iterator**: Iterates across all blocks in an SSTable
3. **Iterator Adapter**: Adapts to the common engine iterator interface

### Range Scan Functionality

Range scans are efficient operations in SSTables:

1. Use the index to find the starting block
2. Iterate through entries in that block
3. Continue to subsequent blocks as needed
4. Respect range boundaries (start/end keys)

### Implementation Notes

The iterator implementation includes:

1. **Lazy Loading**: Blocks are loaded only when needed
2. **Positioning Methods**: Seek, SeekToFirst, Next
3. **Validation**: Bounds checking and state validation
4. **Key/Value Access**: Direct access to current entry data

## Common Usage Patterns

### Writing an SSTable

```go
// Create a new SSTable writer
writer, err := sstable.NewWriter("/path/to/output.sst")
if err != nil {
    log.Fatal(err)
}

// Add key-value pairs in sorted order
writer.Add([]byte("key1"), []byte("value1"))
writer.Add([]byte("key2"), []byte("value2"))
writer.Add([]byte("key3"), []byte("value3"))

// Add a tombstone (deletion marker)
writer.AddTombstone([]byte("key4"))

// Finalize the SSTable
if err := writer.Finish(); err != nil {
    log.Fatal(err)
}
```

### Reading from an SSTable

```go
// Open an SSTable for reading
reader, err := sstable.OpenReader("/path/to/table.sst")
if err != nil {
    log.Fatal(err)
}
defer reader.Close()

// Get a specific value
value, err := reader.Get([]byte("key1"))
if err != nil {
    if err == sstable.ErrNotFound {
        fmt.Println("Key not found")
    } else {
        log.Fatal(err)
    }
} else {
    fmt.Printf("Value: %s\n", value)
}
```

### Iterating Through an SSTable

```go
// Create an iterator
iter := reader.NewIterator()

// Iterate through all entries
for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    fmt.Printf("%s: ", iter.Key())
    
    if iter.IsTombstone() {
        fmt.Println("<deleted>")
    } else {
        fmt.Printf("%s\n", iter.Value())
    }
}

// Or iterate over a specific range
rangeIter := reader.NewIterator()
startKey := []byte("key2")
endKey := []byte("key4")

for rangeIter.Seek(startKey); rangeIter.Valid() && bytes.Compare(rangeIter.Key(), endKey) < 0; rangeIter.Next() {
    fmt.Printf("%s: %s\n", rangeIter.Key(), rangeIter.Value())
}
```

## Configuration Options

The SSTable behavior can be tuned through several configuration parameters:

1. **Block Size** (default: 16KB):
   - Controls the target size for data blocks
   - Larger blocks improve compression and sequential reads
   - Smaller blocks improve random access performance

2. **Restart Interval** (default: 16 entries):
   - Controls how often restart points occur in blocks
   - Affects the balance between compression and lookup speed

3. **Index Key Interval** (default: ~64KB):
   - Controls how frequently keys are indexed
   - Affects the size of the index and lookup performance

## Trade-offs and Limitations

### Immutability

SSTables are immutable, which brings benefits and challenges:

1. **Benefits**:
   - Simplifies concurrent read access
   - No locking required for reads
   - Enables efficient merging during compaction

2. **Challenges**:
   - Updates require rewriting
   - Deletes are implemented as tombstones
   - Space amplification until compaction

### Size vs. Performance Trade-offs

Several design decisions involve balancing size against performance:

1. **Block Size**: Larger blocks improve compression but may result in reading unnecessary data
2. **Restart Points**: More frequent restarts improve random lookup but reduce compression
3. **Index Density**: Denser indices improve lookup speed but increase memory usage

### Specialized Use Cases

The SSTable format is optimized for:

1. **Append-only workloads**: Where data is written once and read many times
2. **Range scans**: Where sequential access to sorted data is common
3. **Batch processing**: Where data can be sorted before writing

It's less optimal for:
1. **Frequent updates**: Due to immutability
2. **Very large keys or values**: Which can cause inefficient storage
3. **Random writes**: Which require external sorting