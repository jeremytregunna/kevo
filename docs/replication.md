# Replication Package Documentation

The `replication` package implements the primary-replica replication protocol for the Kevo database engine. It ensures that replicas maintain a crash-resilient, consistent copy of the primary’s data by streaming Write-Ahead Log (WAL) entries in strict logical order.

## Overview

The replication system streams WAL entries from the primary node to replica nodes in real-time.
It guarantees:
- **Durability**: All data is persisted before acknowledgment.
- **Exactly-once application**: WAL entries are applied in order without duplication.
- **Crash resilience**: Both primary and replicas can recover cleanly after restart.
- **Simplicity**: Designed to be minimal, efficient, and extensible.

The WAL sequence number acts as a **Lamport clock** to provide total ordering across all operations.

---

## Replication Architecture

- **Primary**: The sole writer; streams WAL entries.
- **Replica**: Receives WAL entries, applies them, and persists them locally.
- **Single-writer model**: Only the primary generates new WAL entries.

### Key Responsibilities

Primary:
- Manage WAL files and entry streaming.
- Handle retransmission requests from replicas.
- Enforce authentication and security.

Replica:
- Request WAL entries starting from a specific sequence.
- Apply entries strictly in order.
- Persist entries before acknowledging.
- Handle gaps and perform recovery if needed.

---

## Message Types and Protocol

### gRPC Service Definition

```proto
service WALReplication {
  rpc StreamWAL (WALStreamRequest) returns (stream WALStreamResponse);
  rpc Acknowledge (Ack) returns (AckResponse);
}
```

### Message Structures

#### WALStreamRequest
- `start_sequence (uint64)`: Starting Lamport clock.
- `protocol_version (uint32)`: Protocol negotiation.
- `compression_supported (bool)`: Indicates compression capability.

#### WALStreamResponse
- `entries (repeated WALEntry)`: WAL entries.
- `compressed (bool)`: Whether payloads are compressed.

#### WALEntry
- `sequence_number (uint64)`: Lamport clock.
- `payload (bytes)`: Serialized WAL record.
- `fragment_type (enum)`: FULL, FIRST, MIDDLE, LAST.

#### Ack
- `acknowledged_up_to (uint64)`: Highest sequence number persisted.

#### AckResponse
- `success (bool)`: Acknowledgment result.
- `message (string)`: Optional response detail.

#### Nack (Optional Extension)
- `missing_from_sequence (uint64)`: Resend from this sequence.

---

## Replication State Machine

```plaintext
+------------------+
|    CONNECTING    |
| (Establish gRPC) |
+--------+---------+
         |
         v
+----------------------+
|  STREAMING_ENTRIES   |
| (Receiving WAL)      |
+--------+-------------+
         |
         v
+----------------------+
| APPLYING_ENTRIES     |
| (Enqueue and order)  |
+--------+-------------+
         |
         v
+----------------------+
|   FSYNC_PENDING      |
| (Group writes)       |
+--------+-------------+
         |
         v
+----------------------+
|    ACKNOWLEDGING     |
| (Send Ack to primary)|
+--------+-------------+
         |
         v
+----------------------+
|   WAITING_FOR_DATA   |
| (No new WAL yet)     |
+--------+-------------+
         |
    +----+----+
    |         |
    v         v
ERROR     STREAMING_ENTRIES
(RECONNECT / NACK) (New WAL arrived)
```

The replica progresses through the following states:

State | Description | Triggers
CONNECTING | Establish secure connection to primary | Start, reconnect, or recover
STREAMING_ENTRIES | Actively receiving WAL entries | Connection established, WAL available
APPLYING_ENTRIES | WAL entries are validated and ordered | Entries received
FSYNC_PENDING | Buffering writes to durable storage | After applying batch
ACKNOWLEDGING | Sending Ack for persisted entries | After fsync complete
WAITING_FOR_DATA | No entries available, waiting | WAL drained
ERROR | Handle connection loss, gaps, invalid data | Connection failure, NACK required

---

## Replication Session Flow

```plaintext
Replica                                      Primary
  |                                              |
  |--- Secure Connection Setup ----------------->|
  |                                              |
  |--- WALStreamRequest(start_seq=X) ------------>|
  |                                              |
  |<-- WALStreamResponse(entries) ---------------|
  |                                              |
  | Apply entries sequentially                   |
  | Buffer entries to durable WAL                |
  | fsync to disk                                |
  |                                              |
  |--- Ack(ack_up_to_seq=Y) --------------------->|
  |                                              |
  |<-- WALStreamResponse(next entries) ----------|
  |                                              |
[repeat until EOF or failure]

(if gap detected)
  |                                              |
  |--- Nack(from_seq=Z) ------------------------>|
  |                                              |
  |<-- WALStreamResponse(retransmit from Z) -----|

(on error)
  |                                              |
  |--- Reconnect and resume -------------------->|
```

## Streaming Behavior

The replica lifecycle:

1. **Connection Setup**
   - Replica establishes secure (TLS) gRPC connection to the primary.

2. **Streaming Request**
   - Sends `WALStreamRequest { start_sequence = last_applied_sequence + 1 }`.

3. **WAL Entry Streaming**
   - Primary streams WAL entries in batches.
   - Entries are fragmented as needed (based on WAL fragmentation rules).

4. **Application and fsync**
   - Replica applies entries strictly in order.
   - Replica performs `fsync` after grouped application.

5. **Acknowledgment**
   - Replica sends `Ack` for the highest persisted Lamport clock.
   - Primary can then prune acknowledged WAL entries.

6. **Gap Recovery**
   - If a sequence gap is detected, replica issues a `Nack`.
   - Primary retransmits from the missing sequence.

7. **Heartbeat / Keepalive**
   - Optional periodic messages to detect dead peers.

---

## WAL Entry Framing

Each gRPC `WALStreamResponse` frame includes:

| Field | Description |
|:------|:------------|
| MessageType (1 byte) | WALStreamResponse |
| CompressionFlag (1 byte) | Whether payload is compressed |
| EntryCount (4 bytes) | Number of entries |
| Entries | List of WALEntry |

Each `WALEntry`:

| Field | Description |
|:------|:------------|
| Sequence Number (8 bytes) | Logical Lamport clock |
| Fragment Type (1 byte) | FULL, FIRST, MIDDLE, LAST |
| Payload Size (4 bytes) | Payload size |
| Payload (variable) | WAL operation |

---

## Durability Guarantees

- Replica **must fsync** WAL entries before sending an acknowledgment.
- Acknowledgments cover *all entries up to and including* the acknowledged sequence number.
- If a batch is streamed, multiple entries can be fsynced together to amortize disk costs.

---

## Compression Support

- Compression is negotiated via `compression_supported` in `WALStreamRequest`.
- Payload compression uses lightweight codecs (e.g., snappy or zstd).
- Compressed payloads are transparently decompressed by the replica.

---

## Cold Restart Semantics

### Replica Restart
1. Recover in-memory state by replaying local WAL files.
2. Identify the `last_applied_sequence`.
3. Reconnect to the primary, requesting WAL from `last_applied_sequence + 1`.
4. Continue streaming.

If the requested sequence is no longer available:
- Primary sends an `Error(OUT_OF_RANGE)`.
- Replica must trigger **full resync** (out of scope for this document).

### Primary Restart
1. Recover in-memory WAL pointers by scanning local WAL files.
2. Retain WAL files long enough for lagging replicas (configurable retention window).
3. Serve WAL entries starting from requested sequence if available.

If requested sequence is missing:
- Return `Error(OUT_OF_RANGE)`.

---

## Failure and Recovery

| Failure Type | Recovery Behavior |
|:-------------|:------------------|
| Replica crash | Replay local WAL and reconnect |
| Primary crash | Recover WAL state and resume serving |
| WAL gap detected | Replica issues `Nack`, primary retransmits |
| Stale WAL requested | Full resync required |
| Network failure | Replica reconnects and resumes |
| Data corruption | Skips bad records when possible, follows WAL corruption recovery model |

---

## Transport and Tuning Parameters

| Setting | Recommended Value |
|:--------|:------------------|
| `grpc.max_receive_message_length` | 16MB |
| `grpc.max_send_message_length` | 16MB |
| Keepalive time | 10s |
| Keepalive timeout | 5s |
| Permit without stream | true |
| TLS security | Mandatory with mTLS |
| Retry policy | Application-level reconnect |

---

## Performance Considerations

### Batching
- Form batches up to 256KB–512KB before sending.
- Respect transaction boundaries when batching.

#### Example smart batching algorithm in psudeocode

```psuedo
function formBatch(entries):
    batch = []
    batch_size = 0

    for entry in entries:
        if batch_size + entry.size > 512KB:
            break
        batch.append(entry)
        batch_size += entry.size

    return batch
```

### Flow Control
- Replica should apply and fsync frequently to avoid unbounded memory growth.

### WAL Retention
- Retain WAL files for at least the maximum replica lag window (e.g., 1–2 hours).

---

## Example Usage

### Starting Replication

```go
// Start replication stream from last applied Lamport clock
client := replication.NewWALReplicationClient(conn)
stream, err := client.StreamWAL(context.TODO(), &replication.WALStreamRequest{
    StartSequence: lastAppliedSequence + 1,
    ProtocolVersion: 1,
    CompressionSupported: true,
})

// Receive entries
for {
    resp, err := stream.Recv()
    if err != nil {
        log.Fatal(err)
    }
    for _, entry := range resp.Entries {
        applyEntry(entry)
    }
}
```

### Acknowledging Entries

```go
// After fsyncing applied entries
_, err := client.Acknowledge(context.TODO(), &replication.Ack{
    AcknowledgedUpTo: latestSequence,
})
if err != nil {
    log.Fatal(err)
}
```

---

## Trade-offs and Limitations

| Aspect | Trade-off |
|:-------|:----------|
| Write Amplification | WAL streamed first, then MemTable rebuild |
| Replica Lag | Dependent on disk fsync speed and network conditions |
| WAL Retention | Must balance storage cost against replica tolerance for lag |
| Cold Start | If WAL entries are pruned, full resync becomes necessary |

---

## Future Work (Optional Extensions)

- Full Resync Protocol (snapshot transfer + WAL catch-up).
- Multi-replica support (fan-out streaming).
- Cross-region replication optimizations.
- Tunable flow control and backpressure management.
