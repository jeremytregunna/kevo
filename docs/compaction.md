# Compaction Package Documentation

The `compaction` package implements background processes that merge and optimize SSTable files in the Kevo engine. Compaction is a critical component of the LSM tree architecture, responsible for controlling read amplification, managing tombstones, and maintaining overall storage efficiency.

## Overview

Compaction combines multiple SSTable files into fewer, larger, and more optimized files. This process is essential for maintaining good read performance and controlling disk usage in an LSM tree-based storage system.

Key responsibilities of the compaction package include:
- Selecting files for compaction based on configurable strategies
- Merging overlapping key ranges across multiple SSTables
- Managing tombstones and deleted data
- Organizing SSTables into a level-based hierarchy
- Coordinating background compaction operations

## Architecture

### Component Structure

The compaction package consists of several interrelated components that work together:

```
┌───────────────────────┐
│   CompactionManager   │◄─────┐
└───────────┬───────────┘      │
            │                  │
            ▼                  │
┌───────────────────────┐      │
│ CompactionCoordinator │      │
└───────────┬───────────┘      │
            │                  │
            ▼                  │
┌───────────────────────┐      │      ┌───────────────────────┐
│  CompactionStrategy   │─────▶│      │     EngineFacade      │
└───────────┬───────────┘      │      └───────────────────────┘
            │                  │                │
            ▼                  │                │
┌───────────────────────┐      │                ▼
│     FileTracker       │      │      ┌───────────────────────┐
└─────────────────┬─────┘      │      │    Statistics         │
                  │            │      │    Collector          │
                  ▼            │      └───────────────────────┘
┌───────────────────────┐      │
│   CompactionExecutor  │──────┘
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│   TombstoneManager    │
└───────────────────────┘
```

1. **CompactionManager**: Implements the `CompactionManager` interface
2. **CompactionCoordinator**: Orchestrates the compaction process
3. **CompactionStrategy**: Determines which files to compact and when
4. **CompactionExecutor**: Performs the actual merging of files
5. **FileTracker**: Manages the lifecycle of SSTable files
6. **TombstoneManager**: Tracks deleted keys and their lifecycle
7. **Statistics Collector**: Records compaction metrics and performance data

## Compaction Strategies

### Tiered Compaction Strategy

The primary strategy implemented is a tiered (or leveled) compaction strategy, inspired by LevelDB and RocksDB:

1. **Level Organization**:
   - Level 0: Contains files directly flushed from MemTables
   - Level 1+: Contains files with non-overlapping key ranges

2. **Compaction Triggers**:
   - L0→L1: When L0 has too many files (causes read amplification)
   - Ln→Ln+1: When a level exceeds its size threshold

3. **Size Ratio**:
   - Each level (L+1) can hold approximately 10x more data than level L
   - This ratio is configurable (CompactionRatio in configuration)

### File Selection Algorithm

The strategy uses several criteria to select files for compaction:

1. **L0 Compaction**:
   - Select all L0 files that overlap with the oldest L0 file
   - Include overlapping files from L1

2. **Level-N Compaction**:
   - Select a file from level N based on several possible criteria:
     - Oldest file first
     - File with most overlapping files in the next level
     - File containing known tombstones
   - Include all overlapping files from level N+1

3. **Range Compaction**:
   - Select all files in a given key range across multiple levels
   - Useful for manual compactions or hotspot optimization

## Implementation Details

### Compaction Process

The compaction execution follows these steps:

1. **File Selection**:
   - Strategy identifies files to compact
   - Input files are grouped by level

2. **Merge Process**:
   - Create merged iterators across all input files
   - Write merged data to new output files
   - Handle tombstones appropriately

3. **File Management**:
   - Mark input files as obsolete
   - Register new output files
   - Clean up obsolete files

### Tombstone Handling

Tombstones (deletion markers) require special treatment during compaction:

1. **Tombstone Tracking**:
   - Recent deletions are tracked in the TombstoneManager
   - Tracks tombstones with timestamps to determine when they can be discarded

2. **Tombstone Elimination**:
   - Basic rule: A tombstone can be discarded if all older SSTables have been compacted
   - Tombstones in lower levels can be dropped once they've propagated to higher levels
   - Special case: Tombstones indicating overwritten keys can be dropped immediately

3. **Preservation Logic**:
   - Configurable MaxLevelWithTombstones controls how far tombstones propagate
   - Required to ensure deleted data doesn't "resurface" from older files

### Background Processing

Compaction runs as a background process:

1. **Worker Thread**:
   - Runs on a configurable interval (default 30 seconds)
   - Selects and performs one compaction task per cycle

2. **Concurrency Control**:
   - Lock mechanism ensures only one compaction runs at a time
   - Avoids conflicts with other operations like flushing

3. **Graceful Shutdown**:
   - Compaction can be stopped cleanly on engine shutdown
   - Pending changes are completed before shutdown

## File Tracking and Cleanup

The FileTracker component manages file lifecycles:

1. **File States**:
   - Active: Current file in use
   - Pending: Being compacted
   - Obsolete: Ready for deletion

2. **Safe Deletion**:
   - Files are only deleted when not in use
   - Two-phase marking ensures no premature deletions

3. **Cleanup Process**:
   - Runs after each compaction cycle
   - Safely removes obsolete files from disk

## Performance Considerations

### Read Amplification

Compaction is crucial for controlling read amplification:

1. **Level Strategy Impact**:
   - Without compaction, all SSTables would need checking for each read
   - With leveling, reads typically check one file per level

2. **Optimization for Point Queries**:
   - Higher levels have fewer overlaps
   - Binary search within levels reduces lookups

3. **Range Query Optimization**:
   - Reduced file count improves range scan performance
   - Sorted levels allow efficient merge iteration

### Write Amplification

The compaction process does introduce write amplification:

1. **Cascading Rewrites**:
   - Data may be rewritten multiple times as it moves through levels
   - Key factor in overall write amplification of the storage engine

2. **Mitigation Strategies**:
   - Larger level size ratios reduce compaction frequency
   - Careful file selection minimizes unnecessary rewrites

### Space Amplification

Compaction also manages space amplification:

1. **Duplicate Key Elimination**:
   - Compaction removes outdated versions of keys
   - Critical for preventing unbounded growth

2. **Tombstone Purging**:
   - Eventually removes deletion markers
   - Prevents accumulation of "ghost" records

## Tuning Parameters

Several parameters can be adjusted to optimize compaction behavior:

1. **CompactionLevels** (default: 7):
   - Number of levels in the storage hierarchy
   - More levels mean less write amplification but more read amplification

2. **CompactionRatio** (default: 10):
   - Size ratio between adjacent levels
   - Higher ratio means less frequent compaction but larger individual compactions

3. **CompactionThreads** (default: 2):
   - Number of threads for compaction operations
   - More threads can speed up compaction but increase resource usage

4. **CompactionInterval** (default: 30 seconds):
   - Time between compaction checks
   - Lower values make compaction more responsive but may cause more CPU usage

5. **MaxLevelWithTombstones** (default: 1):
   - Highest level that preserves tombstones
   - Controls how long deletion markers persist

## Common Usage Patterns

### Default Configuration

Most users don't need to interact directly with compaction, as it's managed automatically by the storage engine. The default configuration provides a good balance between read and write performance.

### Manual Compaction Trigger

For maintenance or after bulk operations, manual compaction can be triggered:

```go
// Trigger compaction for the entire database
err := engine.GetCompactionManager().TriggerCompaction()
if err != nil {
    log.Fatal(err)
}

// Compact a specific key range
startKey := []byte("user:1000")
endKey := []byte("user:2000")
err = engine.GetCompactionManager().CompactRange(startKey, endKey)
if err != nil {
    log.Fatal(err)
}
```

### Custom Compaction Strategy

For specialized workloads, a custom compaction strategy can be implemented:

```go
// Example: Creating a coordinator with a custom strategy
customStrategy := NewMyCustomStrategy(config, sstableDir)
coordinator := NewCompactionCoordinator(config, sstableDir, CompactionCoordinatorOptions{
    Strategy: customStrategy,
})

// Start background compaction
coordinator.Start()
```

## Trade-offs and Limitations

### Compaction Pauses

Compaction can temporarily impact performance:

1. **Disk I/O Spikes**:
   - Compaction involves significant disk I/O
   - May affect concurrent read/write operations

2. **Resource Sharing**:
   - Compaction competes with regular operations for system resources
   - Tuning needed to balance background work against foreground performance

### Size vs. Level Trade-offs

The level structure involves several trade-offs:

1. **Few Levels**:
   - Less read amplification (fewer levels to check)
   - More write amplification (more frequent compactions)

2. **Many Levels**:
   - More read amplification (more levels to check)
   - Less write amplification (less frequent compactions)

### Full Compaction Limitations

Some limitations exist for full database compactions:

1. **Resource Intensity**:
   - Full compaction requires significant I/O and CPU
   - May need to be scheduled during low-usage periods

2. **Space Requirements**:
   - Temporarily requires space for both old and new files
   - May not be feasible with limited disk space

## Advanced Concepts

### Dynamic Level Sizing

The implementation uses dynamic level sizing:

1. **Target Size Calculation**:
   - Level L target size = Base size × CompactionRatio^L
   - Automatically adjusts as the database grows

2. **Level-0 Special Case**:
   - Level 0 is managed by file count rather than size
   - Controls read amplification from recent writes

### Compaction Priority

Compaction tasks are prioritized based on several factors:

1. **Level-0 Buildup**: Highest priority to prevent read amplification
2. **Size Imbalance**: Levels exceeding target size
3. **Tombstone Presence**: Files with deletions that can be cleaned up
4. **File Age**: Older files get priority for compaction

### Seek-Based Compaction

For future enhancement, seek-based compaction could be implemented:

1. **Tracking Hot Files**:
   - Monitor which files receive the most seek operations
   - Prioritize these files for compaction

2. **Adaptive Strategy**:
   - Adjust compaction based on observed workload patterns
   - Optimize frequently accessed key ranges