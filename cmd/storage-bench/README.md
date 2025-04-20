# Storage Benchmark Utility

This utility benchmarks the performance of the Kevo storage engine under various workloads.

## Usage

```bash
go run ./cmd/storage-bench/... [flags]
```

### Available Flags

- `-type`: Type of benchmark to run (write, read, scan, mixed, tune, or all) [default: all]
- `-duration`: Duration to run each benchmark [default: 10s]
- `-keys`: Number of keys to use [default: 100000]
- `-value-size`: Size of values in bytes [default: 100]
- `-data-dir`: Directory to store benchmark data [default: ./benchmark-data]
- `-sequential`: Use sequential keys instead of random [default: false]
- `-cpu-profile`: Write CPU profile to file [optional]
- `-mem-profile`: Write memory profile to file [optional]
- `-results`: File to write results to (in addition to stdout) [optional]
- `-tune`: Run configuration tuning benchmarks [default: false]

## Example Commands

Run all benchmarks with default settings:
```bash
go run ./cmd/storage-bench/...
```

Run only write benchmark with 1 million keys and 1KB values for 30 seconds:
```bash
go run ./cmd/storage-bench/... -type=write -keys=1000000 -value-size=1024 -duration=30s
```

Run read and scan benchmarks with sequential keys:
```bash
go run ./cmd/storage-bench/... -type=read,scan -sequential
```

Run with profiling enabled:
```bash
go run ./cmd/storage-bench/... -cpu-profile=cpu.prof -mem-profile=mem.prof
```

Run configuration tuning benchmarks:
```bash
go run ./cmd/storage-bench/... -tune
```

## Benchmark Types

1. **Write Benchmark**: Measures throughput and latency of key-value writes
2. **Read Benchmark**: Measures throughput and latency of key lookups
3. **Scan Benchmark**: Measures performance of range scans
4. **Mixed Benchmark**: Simulates real-world workload with 75% reads, 25% writes
5. **Compaction Benchmark**: Tests compaction throughput and overhead (available through code API)
6. **Tuning Benchmark**: Tests different configuration parameters to find optimal settings

## Result Interpretation

Benchmark results include:
- Operations per second (throughput)
- Average latency per operation
- Hit rate for read operations
- Throughput in MB/s for compaction
- Memory usage statistics

## Configuration Tuning

The tuning benchmark tests various configuration parameters including:
- `MemTableSize`: Sizes tested: 16MB, 32MB
- `SSTableBlockSize`: Sizes tested: 8KB, 16KB
- `WALSyncMode`: Modes tested: None, Batch
- `CompactionRatio`: Ratios tested: 10.0, 20.0

Tuning results are saved to:
- `tuning_results.json`: Detailed benchmark metrics for each configuration
- `recommendations.md`: Markdown file with performance analysis and optimal configuration recommendations

The recommendations include:
- Optimal settings for write-heavy workloads
- Optimal settings for read-heavy workloads
- Balanced settings for mixed workloads
- Additional configuration advice

## Profiling

Use the `-cpu-profile` and `-mem-profile` flags to generate profiling data that can be analyzed with:

```bash
go tool pprof cpu.prof
go tool pprof mem.prof
```