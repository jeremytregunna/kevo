# Kevo

A lightweight, minimalist Log-Structured Merge (LSM) tree storage engine written
in Go.

## Overview

Kevo is a clean, composable storage engine that follows LSM tree
principles, focusing on simplicity while providing the building blocks needed
for higher-level database implementations. It's designed to be both educational
and practically useful for embedded storage needs.

## Features

- **Clean, idiomatic Go implementation** of the LSM tree architecture
- **Single-writer architecture** for simplicity and reduced concurrency complexity
- **Complete storage primitives**: WAL, MemTable, SSTable, Compaction
- **Configurable durability** guarantees (sync vs. batched fsync)
- **Composable interfaces** for fundamental operations (reads, writes, iteration, transactions)
- **ACID-compliant transactions** with SQLite-inspired reader-writer concurrency

## Use Cases

- **Educational Tool**: Learn and teach storage engine internals
- **Embedded Storage**: Applications needing local, durable storage
- **Prototype Foundation**: Base layer for experimenting with novel database designs
- **Go Ecosystem Component**: Reusable storage layer for Go applications

## Getting Started

### Installation

```bash
go get git.canoozie.net/jer/kevo
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"

    "git.canoozie.net/jer/kevo/pkg/engine"
)

func main() {
    // Create or open a storage engine at the specified path
    eng, err := engine.NewEngine("/path/to/data")
    if err != nil {
        log.Fatalf("Failed to open engine: %v", err)
    }
    defer eng.Close()

    // Store a key-value pair
    if err := eng.Put([]byte("hello"), []byte("world")); err != nil {
        log.Fatalf("Failed to put: %v", err)
    }

    // Retrieve a value by key
    value, err := eng.Get([]byte("hello"))
    if err != nil {
        log.Fatalf("Failed to get: %v", err)
    }
    fmt.Printf("Value: %s\n", value)

    // Using transactions
    tx, err := eng.BeginTransaction(false) // false = read-write transaction
    if err != nil {
        log.Fatalf("Failed to start transaction: %v", err)
    }

    // Perform operations within the transaction
    if err := tx.Put([]byte("foo"), []byte("bar")); err != nil {
        tx.Rollback()
        log.Fatalf("Failed to put in transaction: %v", err)
    }

    // Commit the transaction
    if err := tx.Commit(); err != nil {
        log.Fatalf("Failed to commit: %v", err)
    }

    // Scan all key-value pairs
    iter, err := eng.GetIterator()
    if err != nil {
        log.Fatalf("Failed to get iterator: %v", err)
    }

    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
    }
}
```

### Interactive CLI Tool

Included is an interactive CLI tool (`gs`) for exploring and manipulating databases:

```bash
go run ./cmd/gs/main.go [database_path]
```

Will create a directory at the path you create (e.g., /tmp/foo.db will be a
directory called foo.db in /tmp where the database will live).

Example session:

```
gs> PUT user:1 {"name":"John","email":"john@example.com"}
Value stored

gs> GET user:1
{"name":"John","email":"john@example.com"}

gs> BEGIN TRANSACTION
Started read-write transaction

gs> PUT user:2 {"name":"Jane","email":"jane@example.com"}
Value stored in transaction (will be visible after commit)

gs> COMMIT
Transaction committed (0.53 ms)

gs> SCAN user:
user:1: {"name":"John","email":"john@example.com"}
user:2: {"name":"Jane","email":"jane@example.com"}
2 entries found
```

Type `.help` in the CLI for more commands.

## Configuration

Kevo offers extensive configuration options to optimize for different workloads:

```go
// Create custom config for write-intensive workload
config := config.NewDefaultConfig(dbPath)
config.MemTableSize = 64 * 1024 * 1024  // 64MB MemTable
config.WALSyncMode = config.SyncBatch   // Batch sync for better throughput
config.SSTableBlockSize = 32 * 1024     // 32KB blocks

// Create engine with custom config
eng, err := engine.NewEngineWithConfig(config)
```

See [CONFIG_GUIDE.md](./docs/CONFIG_GUIDE.md) for detailed configuration guidance.

## Architecture

Kevo is built on the LSM tree architecture, consisting of:

- **Write-Ahead Log (WAL)**: Ensures durability of writes before they're in memory
- **MemTable**: In-memory data structure (skiplist) for fast writes
- **SSTables**: Immutable, sorted files for persistent storage
- **Compaction**: Background process to merge and optimize SSTables
- **Transactions**: ACID-compliant operations with reader-writer concurrency

## Benchmarking

The storage-bench tool provides comprehensive performance testing:

```bash
go run ./cmd/storage-bench/... -type=all
```

See [storage-bench README](./cmd/storage-bench/README.md) for detailed options.

## Non-Goals

- **Feature Parity with Other Engines**: Not competing with RocksDB, LevelDB, etc.
- **Multi-Node Distribution**: Focusing on single-node operation
- **Complex Query Planning**: Higher-level query features are left to layers built on top

## Building and Testing

```bash
# Build the project
go build ./...

# Run tests
go test ./...

# Run benchmarks
go test ./pkg/path/to/package -bench .
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Copyright 2025 Jeremy Tregunna

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
