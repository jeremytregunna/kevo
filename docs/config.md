# Configuration Package Documentation

The `config` package implements the configuration management system for the Kevo engine. It provides a structured way to define, validate, persist, and load configuration parameters, ensuring consistent behavior across storage engine instances and restarts.

## Overview

Configuration in the Kevo engine is handled through a versioned manifest system. This approach allows for tracking configuration changes over time and ensures that all components operate with consistent settings.

Key responsibilities of the config package include:
- Defining and validating configuration parameters
- Persisting configuration to disk in a manifest file
- Loading configuration during engine startup
- Tracking engine state across restarts
- Providing versioning and backward compatibility

## Configuration Parameters

### WAL Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `WALDir` | string | `<dbPath>/wal` | Directory for Write-Ahead Log files |
| `WALSyncMode` | SyncMode | `SyncBatch` | Synchronization mode (None, Batch, Immediate) |
| `WALSyncBytes` | int64 | 1MB | Bytes written before sync in batch mode |
| `WALMaxSize` | int64 | 0 (dynamic) | Maximum size of a WAL file before rotation |

### MemTable Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `MemTableSize` | int64 | 32MB | Maximum size of a MemTable before flush |
| `MaxMemTables` | int | 4 | Maximum number of MemTables in memory |
| `MaxMemTableAge` | int64 | 600 (seconds) | Maximum age of a MemTable before flush |
| `MemTablePoolCap` | int | 4 | Capacity of the MemTable pool |

### SSTable Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `SSTDir` | string | `<dbPath>/sst` | Directory for SSTable files |
| `SSTableBlockSize` | int | 16KB | Size of data blocks in SSTable |
| `SSTableIndexSize` | int | 64KB | Approximate size between index entries |
| `SSTableMaxSize` | int64 | 64MB | Maximum size of an SSTable file |
| `SSTableRestartSize` | int | 16 | Number of keys between restart points |

### Compaction Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `CompactionLevels` | int | 7 | Number of compaction levels |
| `CompactionRatio` | float64 | 10.0 | Size ratio between adjacent levels |
| `CompactionThreads` | int | 2 | Number of compaction worker threads |
| `CompactionInterval` | int64 | 30 (seconds) | Time between compaction checks |
| `MaxLevelWithTombstones` | int | 1 | Maximum level to keep tombstones |

## Manifest Format

The manifest is a JSON file that stores configuration and state information for the engine.

### Structure

The manifest contains an array of entries, each representing a point-in-time snapshot of the engine configuration:

```json
[
  {
    "timestamp": 1619123456,
    "version": 1,
    "config": {
      "version": 1,
      "wal_dir": "/path/to/data/wal",
      "wal_sync_mode": 1,
      "wal_sync_bytes": 1048576,
      ...
    },
    "filesystem": {
      "/path/to/data/sst/0_000001_00000123456789.sst": 1,
      "/path/to/data/sst/1_000002_00000123456790.sst": 2
    }
  },
  {
    "timestamp": 1619123789,
    "version": 1,
    "config": {
      ...updated configuration...
    },
    "filesystem": {
      ...updated file list...
    }
  }
]
```

### Components

1. **Timestamp**: When the entry was created
2. **Version**: The format version of the manifest
3. **Config**: The complete configuration at that point in time
4. **FileSystem**: A map of file paths to sequence numbers

The last entry in the array represents the current state of the engine.

## Implementation Details

### Configuration Structure

The `Config` struct contains all tunable parameters for the storage engine:

1. **Core Fields**:
   - `Version`: The configuration format version
   - Various parameter fields organized by component

2. **Synchronization**:
   - Mutex to protect concurrent access
   - Thread-safe update methods

3. **Validation**:
   - Comprehensive validation of all parameters
   - Prevents invalid configurations from being used

### Manifest Management

The `Manifest` struct manages configuration persistence and tracking:

1. **Entry Tracking**:
   - List of historical configuration entries
   - Current entry pointer for easy access

2. **File System State**:
   - Tracks SSTable files and their sequence numbers
   - Enables recovery after restart

3. **Persistence**:
   - Atomic updates via temporary files
   - Concurrent access protection

### SyncMode Enum

The `SyncMode` enum defines the WAL synchronization behavior:

1. **SyncNone (0)**:
   - No explicit synchronization
   - Fastest performance, lowest durability

2. **SyncBatch (1)**:
   - Synchronize after a certain amount of data
   - Good balance of performance and durability

3. **SyncImmediate (2)**:
   - Synchronize after every write
   - Highest durability, lowest performance

## Versioning and Compatibility

### Current Version

The current manifest format version is 1, defined by `CurrentManifestVersion`.

### Versioning Strategy

The configuration system supports forward and backward compatibility:

1. **Version Field**:
   - Each config and manifest has a version field
   - Used to detect format changes

2. **Backward Compatibility**:
   - New versions can read old formats
   - Default values apply for missing parameters

3. **Forward Compatibility**:
   - Unknown fields are preserved during updates
   - Allows safe rollback to older versions

## Common Usage Patterns

### Creating Default Configuration

```go
// Create a default configuration for a specific database path
config := config.NewDefaultConfig("/path/to/data")

// Validate the configuration
if err := config.Validate(); err != nil {
    log.Fatal(err)
}
```

### Loading Configuration from Manifest

```go
// Load configuration from an existing manifest
config, err := config.LoadConfigFromManifest("/path/to/data")
if err != nil {
    if errors.Is(err, config.ErrManifestNotFound) {
        // Create a new configuration if manifest doesn't exist
        config = config.NewDefaultConfig("/path/to/data")
    } else {
        log.Fatal(err)
    }
}
```

### Modifying Configuration

```go
// Update configuration parameters
config.Update(func(cfg *config.Config) {
    // Modify parameters
    cfg.MemTableSize = 64 * 1024 * 1024  // 64MB
    cfg.WALSyncMode = config.SyncBatch
    cfg.CompactionInterval = 60  // 60 seconds
})

// Save the updated configuration
if err := config.SaveManifest("/path/to/data"); err != nil {
    log.Fatal(err)
}
```

### Working with Full Manifest

```go
// Load or create a manifest
var manifest *config.Manifest
manifest, err := config.LoadManifest("/path/to/data")
if err != nil {
    if errors.Is(err, config.ErrManifestNotFound) {
        // Create a new manifest
        manifest, err = config.NewManifest("/path/to/data", nil)
        if err != nil {
            log.Fatal(err)
        }
    } else {
        log.Fatal(err)
    }
}

// Update configuration
manifest.UpdateConfig(func(cfg *config.Config) {
    cfg.CompactionRatio = 8.0
})

// Track files
manifest.AddFile("/path/to/data/sst/0_000001_00000123456789.sst", 1)

// Save changes
if err := manifest.Save(); err != nil {
    log.Fatal(err)
}
```

## Performance Considerations

### Memory Impact

The configuration system has minimal memory footprint:

1. **Static Structure**:
   - Fixed size in memory
   - No dynamic growth during operation

2. **Sharing**:
   - Single configuration instance shared among components
   - No duplication of configuration data

### I/O Patterns

Configuration I/O is infrequent and optimized:

1. **Read Once**:
   - Configuration is read once at startup
   - Kept in memory during operation

2. **Write Rarely**:
   - Written only when configuration changes
   - No impact on normal operation

3. **Atomic Updates**:
   - Uses atomic file operations
   - Prevents corruption during crashes

## Configuration Recommendations

### Production Environment

For production use:

1. **WAL Settings**:
   - `WALSyncMode`: `SyncBatch` for most workloads
   - `WALSyncBytes`: 1-4MB for good throughput with reasonable durability

2. **Memory Management**:
   - `MemTableSize`: 64-128MB for high-throughput systems
   - `MaxMemTables`: 4-8 based on available memory

3. **Compaction**:
   - `CompactionRatio`: 8-12 (higher means less frequent but larger compactions)
   - `CompactionThreads`: 2-4 for multi-core systems

### Development/Testing

For development and testing:

1. **WAL Settings**:
   - `WALSyncMode`: `SyncNone` for maximum performance
   - Small database directory for easier management

2. **Memory Settings**:
   - Smaller `MemTableSize` (4-8MB) for more frequent flushes
   - Reduced `MaxMemTables` to limit memory usage

3. **Compaction**:
   - More frequent compaction for testing (`CompactionInterval`: 5-10 seconds)
   - Fewer `CompactionLevels` (3-5) for simpler behavior

## Limitations and Future Enhancements

### Current Limitations

1. **Limited Runtime Changes**:
   - Some parameters can't be changed while the engine is running
   - May require restart for some configuration changes

2. **No Hot Reload**:
   - No automatic detection of configuration changes
   - Changes require explicit engine reload

3. **Simple Versioning**:
   - Basic version number without semantic versioning
   - No complex migration paths between versions

### Potential Enhancements

1. **Hot Configuration Updates**:
   - Ability to update more parameters at runtime
   - Notification system for configuration changes

2. **Configuration Profiles**:
   - Predefined configurations for common use cases
   - Easy switching between profiles

3. **Enhanced Validation**:
   - Interdependent parameter validation
   - Workload-specific recommendations