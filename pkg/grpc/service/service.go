package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/filtered"
	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transaction"
	pb "github.com/KevoDB/kevo/proto/kevo"
)

// Using the transaction registry directly

// KevoServiceServer implements the gRPC KevoService interface
type KevoServiceServer struct {
	pb.UnimplementedKevoServiceServer
	engine             interfaces.Engine
	txRegistry         transaction.Registry
	activeTx           sync.Map // map[string]transaction.Transaction
	txMu               sync.Mutex
	compactionSem      chan struct{}           // Semaphore for limiting concurrent compactions
	maxKeySize         int                     // Maximum allowed key size
	maxValueSize       int                     // Maximum allowed value size
	maxBatchSize       int                     // Maximum number of operations in a batch
	maxTransactions    int                     // Maximum number of concurrent transactions
	transactionTTL     int64                   // Maximum time in seconds a transaction can be idle
	activeTransCount   int32                   // Count of active transactions
	replicationManager ReplicationInfoProvider // Interface to the replication manager
}

// CleanupConnection implements the ConnectionCleanup interface
func (s *KevoServiceServer) CleanupConnection(connectionID string) {
	// Forward call to the transaction registry if it supports connection cleanup
	if registry, ok := s.txRegistry.(interface{ CleanupConnection(string) }); ok {
		registry.CleanupConnection(connectionID)
	}
}

// ReplicationInfoProvider defines an interface for accessing replication topology information
type ReplicationInfoProvider interface {
	// GetNodeInfo returns information about the replication topology
	// Returns: nodeRole, primaryAddr, replicas, lastSequence, readOnly
	GetNodeInfo() (string, string, []ReplicaInfo, uint64, bool)
}

// ReplicaInfo contains information about a replica node
// This should mirror the structure in pkg/replication/info_provider.go
type ReplicaInfo = replication.ReplicationNodeInfo

// NewKevoServiceServer creates a new KevoServiceServer
func NewKevoServiceServer(engine interfaces.Engine, txRegistry transaction.Registry, replicationManager ReplicationInfoProvider) *KevoServiceServer {
	return &KevoServiceServer{
		engine:             engine,
		txRegistry:         txRegistry,
		replicationManager: replicationManager,
		compactionSem:      make(chan struct{}, 1), // Allow only one compaction at a time
		maxKeySize:         4096,                   // 4KB
		maxValueSize:       10 * 1024 * 1024,       // 10MB
		maxBatchSize:       1000,
		maxTransactions:    1000,
		transactionTTL:     300, // 5 minutes
	}
}

// Get retrieves a value for a given key
func (s *KevoServiceServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size")
	}

	value, err := s.engine.Get(req.Key)
	if err != nil {
		// Key not found or other error, return not found
		return &pb.GetResponse{Found: false}, nil
	}

	return &pb.GetResponse{
		Value: value,
		Found: true,
	}, nil
}

// Put stores a key-value pair
func (s *KevoServiceServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size")
	}

	if len(req.Value) > s.maxValueSize {
		return nil, fmt.Errorf("value too large")
	}

	if err := s.engine.Put(req.Key, req.Value); err != nil {
		return &pb.PutResponse{Success: false}, err
	}

	return &pb.PutResponse{Success: true}, nil
}

// Delete removes a key-value pair
func (s *KevoServiceServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size")
	}

	if err := s.engine.Delete(req.Key); err != nil {
		return &pb.DeleteResponse{Success: false}, err
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// BatchWrite performs multiple operations in a batch
func (s *KevoServiceServer) BatchWrite(ctx context.Context, req *pb.BatchWriteRequest) (*pb.BatchWriteResponse, error) {
	if len(req.Operations) == 0 {
		return &pb.BatchWriteResponse{Success: true}, nil
	}

	if len(req.Operations) > s.maxBatchSize {
		return nil, fmt.Errorf("batch size exceeds maximum allowed (%d)", s.maxBatchSize)
	}

	// Start a transaction for atomic batch operations
	tx, err := s.engine.BeginTransaction(false) // Read-write transaction
	if err != nil {
		return &pb.BatchWriteResponse{Success: false}, fmt.Errorf("failed to start transaction: %w", err)
	}

	// Ensure we either commit or rollback
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Process each operation
	for _, op := range req.Operations {
		if len(op.Key) == 0 || len(op.Key) > s.maxKeySize {
			err = fmt.Errorf("invalid key size in batch operation")
			return &pb.BatchWriteResponse{Success: false}, err
		}

		switch op.Type {
		case pb.Operation_PUT:
			if len(op.Value) > s.maxValueSize {
				err = fmt.Errorf("value too large in batch operation")
				return &pb.BatchWriteResponse{Success: false}, err
			}
			if err = tx.Put(op.Key, op.Value); err != nil {
				return &pb.BatchWriteResponse{Success: false}, err
			}
		case pb.Operation_DELETE:
			if err = tx.Delete(op.Key); err != nil {
				return &pb.BatchWriteResponse{Success: false}, err
			}
		default:
			err = fmt.Errorf("unknown operation type")
			return &pb.BatchWriteResponse{Success: false}, err
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return &pb.BatchWriteResponse{Success: false}, err
	}

	return &pb.BatchWriteResponse{Success: true}, nil
}

// Scan iterates over a range of keys
func (s *KevoServiceServer) Scan(req *pb.ScanRequest, stream pb.KevoService_ScanServer) error {
	var limit int32 = 0
	if req.Limit > 0 {
		limit = req.Limit
	}

	// Use a longer timeout for scan operations
	// We create a timeout context but don't need to use it explicitly as the gRPC context
	// will handle timeouts at the transport level

	tx, err := s.engine.BeginTransaction(true)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Always rollback read-only TX when done

	// Create appropriate iterator based on request parameters
	var iter iterator.Iterator
	if len(req.Prefix) > 0 && len(req.Suffix) > 0 {
		// Create a combined prefix-suffix iterator
		baseIter := tx.NewIterator()
		prefixIter := filtered.NewPrefixIterator(baseIter, req.Prefix)
		iter = filtered.NewSuffixIterator(prefixIter, req.Suffix)
	} else if len(req.Prefix) > 0 {
		// Create a prefix iterator
		baseIter := tx.NewIterator()
		iter = filtered.NewPrefixIterator(baseIter, req.Prefix)
	} else if len(req.Suffix) > 0 {
		// Create a suffix iterator
		baseIter := tx.NewIterator()
		iter = filtered.NewSuffixIterator(baseIter, req.Suffix)
	} else if len(req.StartKey) > 0 || len(req.EndKey) > 0 {
		// Create a range iterator
		iter = tx.NewRangeIterator(req.StartKey, req.EndKey)
	} else {
		// Create a full scan iterator
		iter = tx.NewIterator()
	}

	count := int32(0)
	// Position iterator at the first entry
	iter.SeekToFirst()

	// Iterate through all valid entries
	for iter.Valid() {
		if limit > 0 && count >= limit {
			break
		}

		// Skip tombstones (deletion markers)
		if !iter.IsTombstone() {
			if err := stream.Send(&pb.ScanResponse{
				Key:   iter.Key(),
				Value: iter.Value(),
			}); err != nil {
				return err
			}
			count++
		}

		// Move to the next entry
		iter.Next()
	}

	return nil
}


// BeginTransaction starts a new transaction
func (s *KevoServiceServer) BeginTransaction(ctx context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	// Force clean up of old transactions before creating new ones
	if cleaner, ok := s.txRegistry.(*transaction.RegistryImpl); ok {
		cleaner.CleanupStaleTransactions()
	}

	txID, err := s.txRegistry.Begin(ctx, s.engine, req.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &pb.BeginTransactionResponse{
		TransactionId: txID,
	}, nil
}

// CommitTransaction commits an ongoing transaction
func (s *KevoServiceServer) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	tx, exists := s.txRegistry.Get(req.TransactionId)
	if !exists {
		log.Warn("Commit failed - transaction not found: %s", req.TransactionId)
		return nil, fmt.Errorf("transaction not found: %s", req.TransactionId)
	}

	// Remove the transaction after commit
	defer func() {
		s.txRegistry.Remove(req.TransactionId)
	}()

	if err := tx.Commit(); err != nil {
		log.Error("Failed to commit transaction %s: %v", req.TransactionId, err)
		return &pb.CommitTransactionResponse{Success: false}, err
	}

	log.Debug("Successfully committed transaction: %s", req.TransactionId)
	return &pb.CommitTransactionResponse{Success: true}, nil
}

// RollbackTransaction aborts an ongoing transaction
func (s *KevoServiceServer) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	tx, exists := s.txRegistry.Get(req.TransactionId)
	if !exists {
		log.Warn("Rollback failed - transaction not found: %s", req.TransactionId)
		return nil, fmt.Errorf("transaction not found: %s", req.TransactionId)
	}

	// Remove the transaction after rollback
	defer func() {
		s.txRegistry.Remove(req.TransactionId)
	}()

	if err := tx.Rollback(); err != nil {
		log.Error("Failed to roll back transaction %s: %v", req.TransactionId, err)
		return &pb.RollbackTransactionResponse{Success: false}, err
	}

	log.Debug("Successfully rolled back transaction: %s", req.TransactionId)
	return &pb.RollbackTransactionResponse{Success: true}, nil
}

// TxGet retrieves a value for a given key within a transaction
func (s *KevoServiceServer) TxGet(ctx context.Context, req *pb.TxGetRequest) (*pb.TxGetResponse, error) {
	tx, exists := s.txRegistry.Get(req.TransactionId)
	if !exists {
		log.Warn("TxGet failed - transaction not found: %s", req.TransactionId)
		return nil, fmt.Errorf("transaction not found: %s", req.TransactionId)
	}

	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		// For invalid inputs, consider automatically releasing the transaction
		s.txRegistry.Remove(req.TransactionId)
		return nil, fmt.Errorf("invalid key size")
	}

	value, err := tx.Get(req.Key)
	if err != nil {
		// Key not found or other error
		// Convert error to a more readable format and log it
		keyStr := fmt.Sprintf("%q", req.Key)
		if req.Key != nil && len(req.Key) > 0 && req.Key[0] < 32 {
			// If not human-readable, use hex encoding
			keyStr = fmt.Sprintf("%x", req.Key)
		}

		log.Debug("Transaction get failed for key %s: %v", keyStr, err)

		// Return a specific "not found" response
		return &pb.TxGetResponse{
			Value: nil,
			Found: false,
		}, nil
	}

	return &pb.TxGetResponse{
		Value: value,
		Found: true,
	}, nil
}

// TxPut stores a key-value pair within a transaction
func (s *KevoServiceServer) TxPut(ctx context.Context, req *pb.TxPutRequest) (*pb.TxPutResponse, error) {
	tx, exists := s.txRegistry.Get(req.TransactionId)
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", req.TransactionId)
	}

	if tx.IsReadOnly() {
		return nil, fmt.Errorf("cannot write to read-only transaction")
	}

	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size")
	}

	if len(req.Value) > s.maxValueSize {
		return nil, fmt.Errorf("value too large")
	}

	if err := tx.Put(req.Key, req.Value); err != nil {
		return &pb.TxPutResponse{Success: false}, err
	}

	return &pb.TxPutResponse{Success: true}, nil
}

// TxDelete removes a key-value pair within a transaction
func (s *KevoServiceServer) TxDelete(ctx context.Context, req *pb.TxDeleteRequest) (*pb.TxDeleteResponse, error) {
	tx, exists := s.txRegistry.Get(req.TransactionId)
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", req.TransactionId)
	}

	if tx.IsReadOnly() {
		return nil, fmt.Errorf("cannot delete in read-only transaction")
	}

	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size")
	}

	if err := tx.Delete(req.Key); err != nil {
		return &pb.TxDeleteResponse{Success: false}, err
	}

	return &pb.TxDeleteResponse{Success: true}, nil
}

// TxScan iterates over a range of keys within a transaction
func (s *KevoServiceServer) TxScan(req *pb.TxScanRequest, stream pb.KevoService_TxScanServer) error {
	// Use a longer timeout for transaction scan operations
	// The gRPC framework will handle timeouts at the transport level

	tx, exists := s.txRegistry.Get(req.TransactionId)
	if !exists {
		return fmt.Errorf("transaction not found: %s", req.TransactionId)
	}

	var limit int32 = 0
	if req.Limit > 0 {
		limit = req.Limit
	}

	// Create appropriate iterator based on request parameters
	var iter iterator.Iterator
	if len(req.Prefix) > 0 && len(req.Suffix) > 0 {
		// Create a combined prefix-suffix iterator
		baseIter := tx.NewIterator()
		prefixIter := filtered.NewPrefixIterator(baseIter, req.Prefix)
		iter = filtered.NewSuffixIterator(prefixIter, req.Suffix)
	} else if len(req.Prefix) > 0 {
		// Create a prefix iterator
		baseIter := tx.NewIterator()
		iter = filtered.NewPrefixIterator(baseIter, req.Prefix)
	} else if len(req.Suffix) > 0 {
		// Create a suffix iterator
		baseIter := tx.NewIterator()
		iter = filtered.NewSuffixIterator(baseIter, req.Suffix)
	} else if len(req.StartKey) > 0 || len(req.EndKey) > 0 {
		// Create a range iterator
		iter = tx.NewRangeIterator(req.StartKey, req.EndKey)
	} else {
		// Create a full scan iterator
		iter = tx.NewIterator()
	}

	count := int32(0)
	// Position iterator at the first entry
	iter.SeekToFirst()

	// Iterate through all valid entries
	for iter.Valid() {
		if limit > 0 && count >= limit {
			break
		}

		// Skip tombstones (deletion markers)
		if !iter.IsTombstone() {
			if err := stream.Send(&pb.TxScanResponse{
				Key:   iter.Key(),
				Value: iter.Value(),
			}); err != nil {
				return err
			}
			count++
		}

		// Move to the next entry
		iter.Next()
	}

	return nil
}

// GetStats retrieves database statistics
func (s *KevoServiceServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	// Collect basic stats that we know are available
	keyCount := int64(0)
	sstableCount := int32(0)
	memtableCount := int32(1) // At least 1 active memtable

	// Create a read-only transaction to count keys
	tx, err := s.engine.BeginTransaction(true)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for stats: %w", err)
	}
	defer tx.Rollback()

	// Use an iterator to count keys
	iter := tx.NewIterator()

	// Count keys and estimate size
	var totalSize int64
	for iter.Next() {
		keyCount++
		totalSize += int64(len(iter.Key()) + len(iter.Value()))
	}

	// Get statistics from the engine
	statsProvider, ok := s.engine.(interface {
		GetStatsProvider() interface{}
	})

	response := &pb.GetStatsResponse{
		KeyCount:           keyCount,
		StorageSize:        totalSize,
		MemtableCount:      memtableCount,
		SstableCount:       sstableCount,
		WriteAmplification: 1.0, // Placeholder
		ReadAmplification:  1.0, // Placeholder
		OperationCounts:    make(map[string]uint64),
		LatencyStats:       make(map[string]*pb.LatencyStats),
		ErrorCounts:        make(map[string]uint64),
		RecoveryStats:      &pb.RecoveryStats{},
	}

	// Populate detailed stats if the engine implements stats collection
	if ok {
		if collector, ok := statsProvider.GetStatsProvider().(interface {
			GetStats() map[string]interface{}
		}); ok {
			allStats := collector.GetStats()

			// Populate operation counts
			for key, value := range allStats {
				if isOperationCounter(key) {
					if count, ok := value.(uint64); ok {
						response.OperationCounts[key] = count
					}
				}
			}

			// Populate latency statistics
			for key, value := range allStats {
				if isLatencyStat(key) {
					if latency, ok := value.(map[string]interface{}); ok {
						stats := &pb.LatencyStats{}

						if count, ok := latency["count"].(uint64); ok {
							stats.Count = count
						}
						if avg, ok := latency["avg_ns"].(uint64); ok {
							stats.AvgNs = avg
						}
						if min, ok := latency["min_ns"].(uint64); ok {
							stats.MinNs = min
						}
						if max, ok := latency["max_ns"].(uint64); ok {
							stats.MaxNs = max
						}

						response.LatencyStats[key] = stats
					}
				}
			}

			// Populate error counts
			if errors, ok := allStats["errors"].(map[string]uint64); ok {
				for errType, count := range errors {
					response.ErrorCounts[errType] = count
				}
			}

			// Populate performance metrics
			if val, ok := allStats["total_bytes_read"].(uint64); ok {
				response.TotalBytesRead = int64(val)
			}
			if val, ok := allStats["total_bytes_written"].(uint64); ok {
				response.TotalBytesWritten = int64(val)
			}
			if val, ok := allStats["flush_count"].(uint64); ok {
				response.FlushCount = int64(val)
			}
			if val, ok := allStats["compaction_count"].(uint64); ok {
				response.CompactionCount = int64(val)
			}

			// Populate recovery stats
			if recovery, ok := allStats["recovery"].(map[string]interface{}); ok {
				if val, ok := recovery["wal_files_recovered"].(uint64); ok {
					response.RecoveryStats.WalFilesRecovered = val
				}
				if val, ok := recovery["wal_entries_recovered"].(uint64); ok {
					response.RecoveryStats.WalEntriesRecovered = val
				}
				if val, ok := recovery["wal_corrupted_entries"].(uint64); ok {
					response.RecoveryStats.WalCorruptedEntries = val
				}
				if val, ok := recovery["wal_recovery_duration_ms"].(int64); ok {
					response.RecoveryStats.WalRecoveryDurationMs = val
				}
			}
		}
	}

	return response, nil
}

// isOperationCounter checks if a stat key represents an operation counter
func isOperationCounter(key string) bool {
	return len(key) > 4 && key[len(key)-4:] == "_ops"
}

// isLatencyStat checks if a stat key represents latency statistics
func isLatencyStat(key string) bool {
	return len(key) > 8 && key[len(key)-8:] == "_latency"
}

// Compact triggers database compaction
func (s *KevoServiceServer) Compact(ctx context.Context, req *pb.CompactRequest) (*pb.CompactResponse, error) {
	// Use a semaphore to prevent multiple concurrent compactions
	select {
	case s.compactionSem <- struct{}{}:
		// We got the semaphore, proceed with compaction
		defer func() { <-s.compactionSem }()
	default:
		// Semaphore is full, compaction is already running
		return &pb.CompactResponse{Success: false}, fmt.Errorf("compaction is already in progress")
	}

	// For now, Compact just performs a memtable flush as we don't have a public
	// Compact method on the engine yet
	tx, err := s.engine.BeginTransaction(false)
	if err != nil {
		return &pb.CompactResponse{Success: false}, err
	}

	// Do a dummy write to force a flush
	if req.Force {
		err = tx.Put([]byte("__compact_marker__"), []byte("force"))
		if err != nil {
			tx.Rollback()
			return &pb.CompactResponse{Success: false}, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return &pb.CompactResponse{Success: false}, err
	}

	return &pb.CompactResponse{Success: true}, nil
}

// GetNodeInfo returns information about this node and the replication topology
func (s *KevoServiceServer) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	// Create default response for standalone mode
	response := &pb.GetNodeInfoResponse{
		NodeRole:       pb.GetNodeInfoResponse_STANDALONE, // Default to standalone
		ReadOnly:       false,
		PrimaryAddress: "",
		Replicas:       nil,
		LastSequence:   0,
	}

	// Return default values if replication manager is nil
	if s.replicationManager == nil {
		return response, nil
	}

	// Get node role and replication info from the manager
	nodeRole, primaryAddr, replicas, lastSeq, readOnly := s.replicationManager.GetNodeInfo()

	// Set node role
	switch nodeRole {
	case "primary":
		response.NodeRole = pb.GetNodeInfoResponse_PRIMARY
	case "replica":
		response.NodeRole = pb.GetNodeInfoResponse_REPLICA
	default:
		response.NodeRole = pb.GetNodeInfoResponse_STANDALONE
	}

	// Set primary address if available
	response.PrimaryAddress = primaryAddr

	// Set replicas information if any
	if replicas != nil {
		for _, replica := range replicas {
			replicaInfo := &pb.ReplicaInfo{
				Address:      replica.Address,
				LastSequence: replica.LastSequence,
				Available:    replica.Available,
				Region:       replica.Region,
				Meta:         replica.Meta,
			}
			response.Replicas = append(response.Replicas, replicaInfo)
		}
	}

	// Set sequence and read-only status
	response.LastSequence = lastSeq
	response.ReadOnly = readOnly

	return response, nil
}
