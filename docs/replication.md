# Replication System Documentation

The replication system in Kevo implements a primary-replica architecture that allows scaling read operations across multiple replica nodes while maintaining a single writer (primary node). It ensures that replicas maintain a crash-resilient, consistent copy of the primary's data by streaming Write-Ahead Log (WAL) entries in strict logical order.

## Overview

The replication system streams WAL entries from the primary node to replica nodes in real-time.
It guarantees:
- **Durability**: All data is persisted before acknowledgment.
- **Exactly-once application**: WAL entries are applied in order without duplication.
- **Crash resilience**: Both primary and replicas can recover cleanly after restart.
- **Simplicity**: Designed to be minimal, efficient, and extensible.
- **Transparent Client Experience**: Client SDKs automatically handle routing between primary and replicas.

The WAL sequence number acts as a **Lamport clock** to provide total ordering across all operations.

## Implementation Details

The replication system is implemented across several packages:

1. **pkg/replication**: Core replication functionality
   - Primary implementation
   - Replica implementation 
   - WAL streaming protocol
   - Batching and compression

2. **pkg/engine**: Engine integration
   - EngineFacade integration with ReplicationManager
   - Read-only mode for replicas

3. **pkg/client**: Client SDK integration
   - Node role discovery protocol
   - Automatic operation routing
   - Failover handling

## Node Roles

Kevo supports three node roles:

1. **Standalone**: A single node with no replication
   - Handles both reads and writes
   - Default mode when replication is not configured

2. **Primary**: The single writer node in a replication cluster
   - Processes all write operations
   - Streams WAL entries to replicas
   - Can serve read operations but typically offloads them to replicas

3. **Replica**: Read-only nodes that replicate data from the primary
   - Process read operations
   - Apply WAL entries from the primary
   - Reject write operations with redirection information

## Replication Manager

The `ReplicationManager` is the central component of the replication system. It:

1. Handles node configuration and setup
2. Starts the appropriate mode (primary or replica) based on configuration
3. Integrates with the storage engine and WAL
4. Exposes replication topology information
5. Manages the replication state machine

### Configuration

The ReplicationManager is configured via the `ManagerConfig` struct:

```go
type ManagerConfig struct {
    Enabled      bool   // Enable replication
    Mode         string // "primary", "replica", or "standalone"
    ListenAddr   string // Address for primary to listen on (e.g., ":50053")
    PrimaryAddr  string // Address of the primary (for replica mode)
    
    // Advanced settings
    MaxBatchSize int    // Maximum batch size for streaming
    RetentionTime time.Duration // How long to retain WAL entries
    CompressionEnabled bool // Enable compression
}
```

### Status Information

The ReplicationManager provides status information through its `Status()` method:

```go
// Example status information
{
    "enabled": true,
    "mode": "primary",
    "active": true,
    "listen_address": ":50053",
    "connected_replicas": 2,
    "last_sequence": 12345,
    "bytes_transferred": 1048576
}
```

## Primary Node Implementation

The primary node is responsible for:

1. Observing WAL entries as they are written
2. Streaming entries to connected replicas
3. Handling acknowledgments from replicas
4. Tracking replica state and lag

### WAL Observer

The primary implements the `WALEntryObserver` interface to be notified of new WAL entries:

```go
// Simplified implementation
func (p *Primary) OnEntryWritten(entry *wal.Entry) {
    p.buffer.Add(entry)
    p.notifyReplicas()
}
```

### Streaming Implementation

The primary streams entries using a gRPC service:

```go
// Simplified streaming implementation
func (p *Primary) StreamWAL(req *proto.WALStreamRequest, stream proto.WALReplication_StreamWALServer) error {
    startSeq := req.StartSequence
    
    // Send initial entries from WAL
    entries, err := p.wal.GetEntriesFrom(startSeq)
    if err != nil {
        return err
    }
    
    if err := p.sendEntries(entries, stream); err != nil {
        return err
    }
    
    // Subscribe to new entries
    subscription := p.subscribe()
    defer p.unsubscribe(subscription)
    
    for {
        select {
        case entries := <-subscription.Entries():
            if err := p.sendEntries(entries, stream); err != nil {
                return err
            }
        case <-stream.Context().Done():
            return stream.Context().Err()
        }
    }
}
```

## Replica Node Implementation

The replica node is responsible for:

1. Connecting to the primary
2. Receiving WAL entries
3. Applying entries to the local storage engine
4. Acknowledging successfully applied entries

### State Machine

The replica uses a state machine to manage its lifecycle:

```
CONNECTING → STREAMING_ENTRIES → APPLYING_ENTRIES → FSYNC_PENDING → ACKNOWLEDGING → WAITING_FOR_DATA
```

### Entry Application

Entries are applied in strict sequence order:

```go
// Simplified implementation
func (r *Replica) applyEntries(entries []*wal.Entry) error {
    // Verify entries are in proper sequence
    for _, entry := range entries {
        if entry.Sequence != r.nextExpectedSequence {
            return ErrSequenceGap
        }
        r.nextExpectedSequence++
    }
    
    // Apply entries to the engine
    if err := r.engine.ApplyBatch(entries); err != nil {
        return err
    }
    
    // Update last applied sequence
    r.lastAppliedSequence = entries[len(entries)-1].Sequence
    
    return nil
}
```

## Client SDK Integration

The client SDK provides a seamless experience for applications using Kevo with replication:

1. **Node Role Discovery**: On connection, clients discover the node's role and replication topology
2. **Automatic Write Redirection**: Write operations to replicas are transparently redirected to the primary
3. **Read Distribution**: When connected to a primary with replicas, reads can be distributed to replicas
4. **Connection Recovery**: Connection failures are handled with automatic retry and reconnection

### Node Information

When connecting, the client retrieves node information:

```go
// NodeInfo structure returned by the server
type NodeInfo struct {
    Role         string        // "primary", "replica", or "standalone"
    PrimaryAddr  string        // Address of the primary node (for replicas)
    Replicas     []ReplicaInfo // Available replica nodes (for primary)
    LastSequence uint64        // Last applied sequence number
    ReadOnly     bool          // Whether the node is in read-only mode
}

// Example ReplicaInfo
type ReplicaInfo struct {
    Address      string            // Host:port of the replica
    LastSequence uint64            // Last applied sequence number
    Available    bool              // Whether the replica is available
    Region       string            // Optional region information
    Meta         map[string]string // Additional metadata
}
```

### Smart Routing

The client automatically routes operations to the appropriate node:

```go
// Get retrieves a value by key
// If connected to a primary with replicas, it will route reads to a replica
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
    // Check if we should route to replica
    shouldUseReplica := c.nodeInfo != nil &&
        c.nodeInfo.Role == "primary" &&
        len(c.replicaConn) > 0
    
    if shouldUseReplica {
        // Select a replica for reading
        selectedReplica := c.replicaConn[0]
        
        // Try the replica first
        resp, err := selectedReplica.Send(ctx, request)
        
        // Fall back to primary if replica fails
        if err != nil {
            resp, err = c.client.Send(ctx, request)
        }
    } else {
        // Use default connection
        resp, err = c.client.Send(ctx, request)
    }
    
    // Process response...
}

// Put stores a key-value pair
// If connected to a replica, it will automatically route the write to the primary
func (c *Client) Put(ctx context.Context, key, value []byte) (bool, error) {
    // Check if we should route to primary
    shouldUsePrimary := c.nodeInfo != nil &&
        c.nodeInfo.Role == "replica" &&
        c.primaryConn != nil
    
    if shouldUsePrimary {
        // Use primary connection for writes when connected to replica
        resp, err = c.primaryConn.Send(ctx, request)
    } else {
        // Use default connection
        resp, err = c.client.Send(ctx, request)
        
        // If we get a read-only error, try to discover topology and retry
        if err != nil && isReadOnlyError(err) {
            if err := c.discoverTopology(ctx); err == nil {
                // Retry with primary if we now have one
                if c.primaryConn != nil {
                    resp, err = c.primaryConn.Send(ctx, request)
                }
            }
        }
    }
    
    // Process response...
}
```

## Server Configuration

To run a Kevo server with replication, use the following configuration options:

### Standalone Mode (Default)

```bash
kevo -server [database_path]
```

### Primary Mode

```bash
kevo -server [database_path] -replication.enabled=true -replication.mode=primary -replication.listen=:50053
```

### Replica Mode

```bash
kevo -server [database_path] -replication.enabled=true -replication.mode=replica -replication.primary=localhost:50053
```

## Implementation Considerations

### Durability

- Primary: All entries are durably written to WAL before being streamed
- Replica: Entries are applied and fsynced before acknowledgment
- WAL retention on primary ensures replicas can recover from short-term failures

### Consistency

- Primary is always the source of truth for writes
- Replicas may temporarily lag behind the primary
- Last sequence number indicates replication status
- Clients can choose to verify replica freshness for critical operations

### Performance

- Batch size is configurable to balance latency and throughput
- Compression can be enabled to reduce network bandwidth
- Read operations can be distributed across replicas for scaling
- Replicas operate in read-only mode, eliminating write contention

### Fault Tolerance

- Replica node restart: Recover local state, catch up missing entries
- Primary node restart: Resume serving WAL entries to replicas
- Network failures: Automatic reconnection with exponential backoff
- Gap detection: Replicas verify sequence continuity

## Protocol Details

The replication protocol is defined using Protocol Buffers:

```proto
service WALReplication {
  rpc StreamWAL (WALStreamRequest) returns (stream WALStreamResponse);
  rpc Acknowledge (Ack) returns (AckResponse);
}

message WALStreamRequest {
  uint64 start_sequence = 1;
  uint32 protocol_version = 2;
  bool compression_supported = 3;
}

message WALStreamResponse {
  repeated WALEntry entries = 1;
  bool compressed = 2;
}

message WALEntry {
  uint64 sequence_number = 1;
  bytes payload = 2;
  FragmentType fragment_type = 3;
}

message Ack {
  uint64 acknowledged_up_to = 1;
}

message AckResponse {
  bool success = 1;
  string message = 2;
}
```

The protocol ensures:
- Entries are streamed in order
- Gaps are detected using sequence numbers
- Large entries can be fragmented
- Compression is negotiated for efficiency

## Limitations and Trade-offs

1. **Single Writer Model**: The system follows a strict single-writer architecture, limiting write throughput to a single primary node
2. **Replica Lag**: Replicas may be slightly behind the primary, requiring careful consideration for read-after-write scenarios
3. **Manual Failover**: The system does not implement automatic failover; if the primary fails, manual intervention is required
4. **Cold Start**: If WAL entries are pruned, new replicas require a full resync from the primary

## Future Work

The current implementation provides a robust foundation for replication, with several planned enhancements:

1. **Multi-region Replication**: Optimize for cross-region replication
2. **Replica Groups**: Support for replica tiers and read preferences
3. **Snapshot Transfer**: Efficient initialization of new replicas without WAL replay
4. **Flow Control**: Backpressure mechanisms to handle slow replicas