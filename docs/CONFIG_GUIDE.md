# Kevo Engine Configuration Guide

This guide provides recommendations for configuring the Kevo Engine for various workloads and environments.

## Configuration Parameters

The Kevo Engine can be configured through the `config.Config` struct. Here are the most important parameters:

### WAL Configuration

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `WALDir` | Directory for Write-Ahead Log files | `<dbPath>/wal` | Any valid directory path |
| `WALSyncMode` | Synchronization mode for WAL writes | `SyncBatch` | `SyncNone`, `SyncBatch`, `SyncImmediate` |
| `WALSyncBytes` | Bytes written before sync in batch mode | 1MB | 64KB-16MB |

### MemTable Configuration

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `MemTableSize` | Maximum size of a MemTable before flush | 32MB | 4MB-128MB |
| `MaxMemTables` | Maximum number of MemTables in memory | 4 | 2-8 |
| `MaxMemTableAge` | Maximum age of a MemTable before flush (seconds) | 600 | 60-3600 |

### SSTable Configuration

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `SSTDir` | Directory for SSTable files | `<dbPath>/sst` | Any valid directory path |
| `SSTableBlockSize` | Size of data blocks in SSTable | 16KB | 4KB-64KB |
| `SSTableIndexSize` | Approximate size between index entries | 64KB | 16KB-256KB |
| `SSTableMaxSize` | Maximum size of an SSTable file | 64MB | 16MB-256MB |
| `SSTableRestartSize` | Number of keys between restart points | 16 | 8-64 |

### Compaction Configuration

| Parameter | Description | Default | Range |
|-----------|-------------|---------|-------|
| `CompactionLevels` | Number of compaction levels | 7 | 3-10 |
| `CompactionRatio` | Size ratio between adjacent levels | 10 | 5-20 |
| `CompactionThreads` | Number of compaction worker threads | 2 | 1-8 |
| `CompactionInterval` | Time between compaction checks (seconds) | 30 | 5-300 |
| `MaxLevelWithTombstones` | Maximum level to keep tombstones | 1 | 0-3 |

## Workload-Based Recommendations

### Balanced Workload (Default)

For a balanced mix of reads and writes:

```go
config := config.NewDefaultConfig(dbPath)
```

The default configuration is optimized for a good balance between read and write performance, with reasonable durability guarantees.

### Write-Intensive Workload

For workloads with many writes (e.g., logging, event streaming):

```go
config := config.NewDefaultConfig(dbPath)
config.MemTableSize = 64 * 1024 * 1024     // 64MB
config.WALSyncMode = config.SyncBatch      // Batch mode for better write throughput
config.WALSyncBytes = 4 * 1024 * 1024      // 4MB between syncs
config.SSTableBlockSize = 32 * 1024        // 32KB
config.CompactionRatio = 5                 // More frequent compactions
```

### Read-Intensive Workload

For workloads with many reads (e.g., content serving, lookups):

```go
config := config.NewDefaultConfig(dbPath)
config.MemTableSize = 16 * 1024 * 1024     // 16MB
config.SSTableBlockSize = 8 * 1024         // 8KB for better read performance
config.SSTableIndexSize = 32 * 1024        // 32KB for more index points
config.CompactionRatio = 20                // Less frequent compactions
```

### Low-Latency Workload

For workloads requiring minimal latency spikes:

```go
config := config.NewDefaultConfig(dbPath)
config.MemTableSize = 8 * 1024 * 1024      // 8MB for quicker flushes
config.CompactionInterval = 5              // More frequent compaction checks
config.CompactionThreads = 1               // Reduce contention
```

### High-Durability Workload

For workloads where data durability is critical:

```go
config := config.NewDefaultConfig(dbPath)
config.WALSyncMode = config.SyncImmediate  // Immediate sync after each write
config.MaxMemTableAge = 60                 // Flush MemTables more frequently
```

### Memory-Constrained Environment

For environments with limited memory:

```go
config := config.NewDefaultConfig(dbPath)
config.MemTableSize = 4 * 1024 * 1024      // 4MB
config.MaxMemTables = 2                    // Only keep 2 MemTables in memory
config.SSTableBlockSize = 4 * 1024         // 4KB blocks
```

## Environmental Considerations

### SSD vs HDD Storage

For SSD storage:
- Consider using larger block sizes (16KB-32KB)
- Batch WAL syncs are generally sufficient

For HDD storage:
- Use larger block sizes (32KB-64KB) to reduce seeks
- Consider more aggressive compaction to reduce fragmentation

### Client-Side vs Server-Side

For client-side applications:
- Reduce memory usage with smaller MemTable sizes
- Consider using SyncNone or SyncBatch modes for better performance

For server-side applications:
- Configure based on workload characteristics
- Allocate more memory for MemTables in high-throughput scenarios

## Performance Impact of Key Parameters

### WALSyncMode

- **SyncNone**: Highest write throughput, but risk of data loss on crash
- **SyncBatch**: Good balance of throughput and durability
- **SyncImmediate**: Highest durability, but lowest write throughput

### MemTableSize

- **Larger**: Better write throughput, higher memory usage, potentially longer pauses
- **Smaller**: Lower memory usage, more frequent compaction, potentially lower throughput

### SSTableBlockSize

- **Larger**: Better scan performance, slightly higher space usage
- **Smaller**: Better point lookup performance, potentially higher index overhead

### CompactionRatio

- **Larger**: Less frequent compaction, higher read amplification
- **Smaller**: More frequent compaction, lower read amplification

## Tuning Process

To find the optimal configuration for your specific workload:

1. Run the benchmarking tool with your expected workload:
   ```
   go run ./cmd/storage-bench/... -tune
   ```

2. The tool will generate a recommendations report based on the benchmark results

3. Adjust the configuration based on the recommendations and your specific requirements

4. Validate with your application workload

## Example Custom Configuration

```go
// Example custom configuration for a write-heavy time-series database
func CustomTimeSeriesConfig(dbPath string) *config.Config {
    cfg := config.NewDefaultConfig(dbPath)
    
    // Optimize for write throughput
    cfg.MemTableSize = 64 * 1024 * 1024
    cfg.WALSyncMode = config.SyncBatch
    cfg.WALSyncBytes = 4 * 1024 * 1024
    
    // Optimize for sequential scans
    cfg.SSTableBlockSize = 32 * 1024
    
    // Optimize for compaction
    cfg.CompactionRatio = 5
    
    return cfg
}
```

## Conclusion

The Kevo Engine provides a flexible configuration system that can be tailored to various workloads and environments. By understanding the impact of each configuration parameter, you can optimize the engine for your specific needs.

For most applications, the default configuration provides a good starting point, but tuning can significantly improve performance for specific workloads.