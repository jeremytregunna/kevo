# Kevo Client SDK Development Guide

This document provides technical guidance for developing client SDKs for Kevo in various programming languages. It focuses on the gRPC API, communication patterns, and best practices.

## gRPC API Overview

Kevo exposes its functionality through a gRPC service defined in `proto/kevo/service.proto`. The service provides operations for:

1. **Key-Value Operations** - Basic get, put, and delete operations
2. **Batch Operations** - Atomic multi-key operations
3. **Iterator Operations** - Range scans and prefix scans
4. **Transaction Operations** - Support for ACID transactions
5. **Administrative Operations** - Statistics and compaction
6. **Replication Operations** - Node role discovery and topology information

## Service Definition

The main service is `KevoService`, which contains the following RPC methods:

### Key-Value Operations

- `Get(GetRequest) returns (GetResponse)`: Retrieves a value by key
- `Put(PutRequest) returns (PutResponse)`: Stores a key-value pair
- `Delete(DeleteRequest) returns (DeleteResponse)`: Removes a key-value pair

### Batch Operations

- `BatchWrite(BatchWriteRequest) returns (BatchWriteResponse)`: Performs multiple operations atomically

### Iterator Operations

- `Scan(ScanRequest) returns (stream ScanResponse)`: Streams key-value pairs in a range

### Transaction Operations

- `BeginTransaction(BeginTransactionRequest) returns (BeginTransactionResponse)`: Starts a new transaction
- `CommitTransaction(CommitTransactionRequest) returns (CommitTransactionResponse)`: Commits a transaction
- `RollbackTransaction(RollbackTransactionRequest) returns (RollbackTransactionResponse)`: Aborts a transaction
- `TxGet(TxGetRequest) returns (TxGetResponse)`: Get operation in a transaction
- `TxPut(TxPutRequest) returns (TxPutResponse)`: Put operation in a transaction
- `TxDelete(TxDeleteRequest) returns (TxDeleteResponse)`: Delete operation in a transaction
- `TxScan(TxScanRequest) returns (stream TxScanResponse)`: Scan operation in a transaction

### Administrative Operations

- `GetStats(GetStatsRequest) returns (GetStatsResponse)`: Retrieves database statistics
- `Compact(CompactRequest) returns (CompactResponse)`: Triggers compaction

### Replication Operations

- `GetNodeInfo(GetNodeInfoRequest) returns (GetNodeInfoResponse)`: Retrieves information about the node's role and replication topology

## Implementation Considerations

When implementing a client SDK, consider the following aspects:

### Connection Management

1. **Establish Connection**: Create and maintain gRPC connection to the server
2. **Connection Pooling**: Implement connection pooling for performance (if the language/platform supports it)
3. **Timeout Handling**: Set appropriate timeouts for connection establishment and requests
4. **TLS Support**: Support secure communications with TLS
5. **Replication Awareness**: Discover node roles and maintain appropriate connections

```
// Connection options example
options = {
  endpoint: "localhost:50051",
  connectTimeout: 5000,  // milliseconds
  requestTimeout: 10000, // milliseconds
  poolSize: 5,           // number of connections
  tlsEnabled: false,
  certPath: "/path/to/cert.pem",
  keyPath: "/path/to/key.pem",
  caPath: "/path/to/ca.pem",
  
  // Replication options
  discoverTopology: true, // automatically discover node role and topology
  autoRouteWrites: true,  // automatically route writes to primary
  autoRouteReads: true    // route reads to replicas when possible
}
```

### Basic Operations

Implement clean, idiomatic methods for basic operations:

```
// Example operations (in pseudo-code)
client.get(key) -> [value, found]
client.put(key, value, sync) -> success
client.delete(key, sync) -> success

// With proper error handling
try {
  value, found = client.get(key)
} catch (Exception e) {
  // Handle errors
}
```

### Batch Operations

Batch operations should be atomic from the client perspective:

```
// Example batch write
operations = [
  { type: "put", key: key1, value: value1 },
  { type: "put", key: key2, value: value2 },
  { type: "delete", key: key3 }
]

success = client.batchWrite(operations, sync)
```

### Streaming Operations

For scan operations, implement both streaming and iterator patterns based on language idioms:

```
// Streaming example
client.scan(prefix, startKey, endKey, limit, function(key, value) {
  // Process each key-value pair
})

// Iterator example
iterator = client.scan(prefix, startKey, endKey, limit)
while (iterator.hasNext()) {
  [key, value] = iterator.next()
  // Process each key-value pair
}
iterator.close()
```

### Transaction Support

Provide a transaction API with proper resource management:

```
// Transaction example
tx = client.beginTransaction(readOnly)
try {
  val = tx.get(key)
  tx.put(key2, value2)
  tx.commit()
} catch (Exception e) {
  tx.rollback()
  throw e
}
```

Consider implementing a transaction callback pattern for better resource management (if the language supports it):

```
// Transaction callback pattern
client.transaction(function(tx) {
  // Operations inside transaction
  val = tx.get(key)
  tx.put(key2, value2)
  // Auto-commit if no exceptions
})
```

### Error Handling and Retries

1. **Error Categories**: Map gRPC error codes to meaningful client-side errors
2. **Retry Policy**: Implement exponential backoff with jitter for transient errors
3. **Error Context**: Provide detailed error information

```
// Retry policy example
retryPolicy = {
  maxRetries: 3,
  initialBackoffMs: 100,
  maxBackoffMs: 2000,
  backoffFactor: 1.5,
  jitter: 0.2
}
```

### Performance Considerations

1. **Message Size Limits**: Handle large messages appropriately
2. **Stream Management**: Properly handle long-running streams

```
// Performance options example
options = {
  maxMessageSize: 16 * 1024 * 1024  // 16MB
}
```

## Key Implementation Areas

### Key and Value Types

All keys and values are represented as binary data (`bytes` in protobuf). Your SDK should handle conversions between language-specific types and byte arrays.

### The `sync` Parameter

In operations that modify data (`Put`, `Delete`, `BatchWrite`), the `sync` parameter determines whether the operation waits for data to be durably persisted before returning. This is a critical parameter for balancing performance vs. durability.

### Transaction IDs

Transaction IDs are strings generated by the server on transaction creation. Clients must store and pass these IDs for all operations within a transaction.

### Scan Operation Parameters

- `prefix`: Optional prefix to filter keys (when provided, start_key/end_key are ignored)
- `start_key`: Start of the key range (inclusive)
- `end_key`: End of the key range (exclusive)
- `limit`: Maximum number of results to return

### Node Role and Replication Support

When implementing an SDK for a Kevo cluster with replication, your client should:

1. **Discover Node Role**: On connection, query the server for node role information
2. **Connection Management**: Maintain appropriate connections based on node role:
   - When connected to a primary, optionally connect to available replicas for reads
   - When connected to a replica, connect to the primary for writes
3. **Operation Routing**: Direct operations to the appropriate node:
   - Read operations: Can be directed to replicas when available
   - Write operations: Must be directed to the primary
4. **Connection Recovery**: Handle connection failures with automatic reconnection

### Node Role Discovery

```
// Get node information on connection
nodeInfo = client.getNodeInfo()

// Check node role
if (nodeInfo.role == "primary") {
  // Connected to primary
  // Optionally connect to replicas for read distribution
  for (replica in nodeInfo.replicas) {
    if (replica.available) {
      connectToReplica(replica.address)
    }
  }
} else if (nodeInfo.role == "replica") {
  // Connected to replica
  // Connect to primary for writes
  connectToPrimary(nodeInfo.primaryAddress)
}
```

### Operation Routing

```
// Get operation
function get(key) {
  if (nodeInfo.role == "primary" && hasReplicaConnections()) {
    // Try to read from replica
    try {
      return readFromReplica(key)
    } catch (error) {
      // Fall back to primary if replica read fails
      return readFromPrimary(key)
    }
  } else {
    // Read from current connection
    return readFromCurrent(key)
  }
}

// Put operation
function put(key, value) {
  if (nodeInfo.role == "replica" && hasPrimaryConnection()) {
    // Route write to primary
    return writeToPrimary(key, value)
  } else {
    // Write to current connection
    return writeToCurrent(key, value)
  }
}
```

## Common Pitfalls

1. **Stream Resource Leaks**: Always close streams properly
2. **Transaction Resource Leaks**: Always commit or rollback transactions
3. **Large Result Sets**: Implement proper pagination or streaming for large scans
4. **Connection Management**: Properly handle connection failures and reconnection
5. **Timeout Handling**: Set appropriate timeouts for different operations
6. **Role Discovery**: Discover node role at connection time and after reconnections
7. **Write Routing**: Always route writes to the primary node
8. **Read-after-Write**: Be aware of potential replica lag in read-after-write scenarios

## Example Usage Patterns

To ensure a consistent experience across different language SDKs, consider implementing these common usage patterns:

### Simple Usage

```
// Create client
client = new KevoClient("localhost:50051")

// Connect
client.connect()

// Key-value operations
client.put("key", "value")
value = client.get("key")
client.delete("key")

// Close connection
client.close()
```

### Advanced Usage with Options

```
// Create client with options
options = {
  endpoint: "kevo-server:50051",
  connectTimeout: 5000,
  requestTimeout: 10000,
  tlsEnabled: true,
  certPath: "/path/to/cert.pem",
  // ... more options
}
client = new KevoClient(options)

// Connect with context
client.connect(context)

// Batch operations
operations = [
  { type: "put", key: "key1", value: "value1" },
  { type: "put", key: "key2", value: "value2" },
  { type: "delete", key: "key3" }
]
client.batchWrite(operations, true)  // sync=true

// Transaction
client.transaction(function(tx) {
  value = tx.get("key1")
  tx.put("key2", "updated-value")
  tx.delete("key3")
})

// Iterator
iterator = client.scan({ prefix: "user:" })
while (iterator.hasNext()) {
  [key, value] = iterator.next()
  // Process each key-value pair
}
iterator.close()

// Close connection
client.close()
```

### Replication Usage

```
// Create client with replication options
options = {
  endpoint: "kevo-replica:50051",  // Connect to any node (primary or replica)
  discoverTopology: true,          // Automatically discover node role
  autoRouteWrites: true,           // Route writes to primary
  autoRouteReads: true             // Distribute reads to replicas when possible
}
client = new KevoClient(options)

// Connect and discover topology
client.connect()

// Get node role information
nodeInfo = client.getNodeInfo()
console.log("Connected to " + nodeInfo.role + " node")

if (nodeInfo.role == "primary") {
  console.log("This node has " + nodeInfo.replicas.length + " replicas")
} else if (nodeInfo.role == "replica") {
  console.log("Primary node is at " + nodeInfo.primaryAddr)
}

// Operations automatically routed to appropriate nodes
client.put("key1", "value1")    // Routed to primary
value = client.get("key1")      // May be routed to a replica if available

// Different routing behavior can be explicitly set
value = client.get("key2", { preferReplica: false })  // Force primary read

// Manual routing for advanced use cases
client.withPrimary(function(primary) {
  // These operations are executed directly on the primary
  primary.get("key3")
  primary.put("key4", "value4")
})

// Close all connections
client.close()
```

## Testing Your SDK

When testing your SDK implementation, consider these scenarios:

1. **Basic Operations**: Simple get, put, delete operations
2. **Concurrency**: Multiple concurrent operations
3. **Error Handling**: Server errors, timeouts, network issues
4. **Connection Management**: Reconnection after server restart
5. **Large Data**: Large keys and values, many operations
6. **Transactions**: ACID properties, concurrent transactions
7. **Performance**: Throughput, latency, resource usage
8. **Replication**: 
   - Node role discovery
   - Write redirection from replica to primary
   - Read distribution to replicas
   - Connection handling when nodes are unavailable
   - Read-after-write scenarios with potential replica lag

## Conclusion

When implementing a Kevo client SDK, focus on providing an idiomatic experience for the target language while correctly handling the underlying gRPC communication details. The goal is to make the client API intuitive for developers familiar with the language, while ensuring correct and efficient interaction with the Kevo server.