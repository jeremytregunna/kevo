# Kevo Go Client SDK

This package provides a Go client for connecting to a Kevo database server. The client uses the gRPC transport layer to communicate with the server and provides an idiomatic Go API for working with Kevo.

## Features

- Simple key-value operations (Get, Put, Delete)
- Batch operations for atomic writes
- Transaction support with ACID guarantees
- Iterator API for efficient range scans
- Connection pooling and automatic retries
- TLS support for secure communication
- Comprehensive error handling
- Configurable timeouts and backoff strategies

## Installation

```bash
go get github.com/KevoDB/kevo
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/KevoDB/kevo/pkg/client"
	_ "github.com/KevoDB/kevo/pkg/grpc/transport" // Register gRPC transport
)

func main() {
	// Create a client with default options
	options := client.DefaultClientOptions()
	options.Endpoint = "localhost:50051"

	c, err := client.NewClient(options)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Connect to the server
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Basic key-value operations
	key := []byte("hello")
	value := []byte("world")

	// Store a value
	if _, err := c.Put(ctx, key, value, true); err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	// Retrieve a value
	val, found, err := c.Get(ctx, key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}

	if found {
		fmt.Printf("Value: %s\n", val)
	} else {
		fmt.Println("Key not found")
	}

	// Delete a value
	if _, err := c.Delete(ctx, key, true); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
}
```

## Configuration Options

The client can be configured using the `ClientOptions` struct:

```go
options := client.ClientOptions{
	// Connection options
	Endpoint:        "localhost:50051",
	ConnectTimeout:  5 * time.Second,
	RequestTimeout:  10 * time.Second,
	TransportType:   "grpc",
	PoolSize:        5,

	// Security options
	TLSEnabled:      true,
	CertFile:        "/path/to/cert.pem",
	KeyFile:         "/path/to/key.pem",
	CAFile:          "/path/to/ca.pem",

	// Retry options
	MaxRetries:      3,
	InitialBackoff:  100 * time.Millisecond,
	MaxBackoff:      2 * time.Second,
	BackoffFactor:   1.5,
	RetryJitter:     0.2,

	// Performance options
	Compression:     client.CompressionGzip,
	MaxMessageSize:  16 * 1024 * 1024, // 16MB
}
```

## Transactions

```go
// Begin a transaction
tx, err := client.BeginTransaction(ctx, false) // readOnly=false
if err != nil {
	log.Fatalf("Failed to begin transaction: %v", err)
}

// Perform operations within the transaction
success, err := tx.Put(ctx, []byte("key1"), []byte("value1"))
if err != nil {
	tx.Rollback(ctx) // Rollback on error
	log.Fatalf("Transaction put failed: %v", err)
}

// Commit the transaction
if err := tx.Commit(ctx); err != nil {
	log.Fatalf("Transaction commit failed: %v", err)
}
```

## Scans and Iterators

```go
// Set up scan options
scanOptions := client.ScanOptions{
	Prefix:   []byte("user:"),  // Optional prefix
	StartKey: []byte("user:1"), // Optional start key (inclusive)
	EndKey:   []byte("user:9"), // Optional end key (exclusive)
	Limit:    100,              // Optional limit
}

// Create a scanner
scanner, err := client.Scan(ctx, scanOptions)
if err != nil {
	log.Fatalf("Failed to create scanner: %v", err)
}
defer scanner.Close()

// Iterate through results
for scanner.Next() {
	fmt.Printf("Key: %s, Value: %s\n", scanner.Key(), scanner.Value())
}

// Check for errors after iteration
if err := scanner.Error(); err != nil {
	log.Fatalf("Scan error: %v", err)
}
```

## Batch Operations

```go
// Create a batch of operations
operations := []client.BatchOperation{
	{Type: "put", Key: []byte("key1"), Value: []byte("value1")},
	{Type: "put", Key: []byte("key2"), Value: []byte("value2")},
	{Type: "delete", Key: []byte("old-key")},
}

// Execute the batch atomically
success, err := client.BatchWrite(ctx, operations, true)
if err != nil {
	log.Fatalf("Batch write failed: %v", err)
}
```

## Error Handling and Retries

The client automatically handles retries for transient errors using exponential backoff with jitter. You can configure the retry behavior using the `RetryPolicy` in the client options.

```go
// Manual retry with custom policy
err = client.RetryWithBackoff(
	ctx,
	func() error {
		_, _, err := c.Get(ctx, key)
		return err
	},
	3,                     // maxRetries
	100*time.Millisecond,  // initialBackoff
	2*time.Second,         // maxBackoff
	2.0,                   // backoffFactor
	0.2,                   // jitter
)
```

## Database Statistics

```go
// Get database statistics
stats, err := client.GetStats(ctx)
if err != nil {
	log.Fatalf("Failed to get stats: %v", err)
}

fmt.Printf("Key count: %d\n", stats.KeyCount)
fmt.Printf("Storage size: %d bytes\n", stats.StorageSize)
fmt.Printf("MemTable count: %d\n", stats.MemtableCount)
fmt.Printf("SSTable count: %d\n", stats.SstableCount)
fmt.Printf("Write amplification: %.2f\n", stats.WriteAmplification)
fmt.Printf("Read amplification: %.2f\n", stats.ReadAmplification)
```

## Compaction

```go
// Trigger compaction
success, err := client.Compact(ctx, false) // force=false
if err != nil {
	log.Fatalf("Compaction failed: %v", err)
}
```
