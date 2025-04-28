package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
)

const (
	// Record types
	RecordTypeFull   = 1
	RecordTypeFirst  = 2
	RecordTypeMiddle = 3
	RecordTypeLast   = 4

	// Operation types
	OpTypePut    = 1
	OpTypeDelete = 2
	OpTypeMerge  = 3
	OpTypeBatch  = 4

	// Header layout
	// - CRC (4 bytes)
	// - Length (2 bytes)
	// - Type (1 byte)
	HeaderSize = 7

	// Maximum size of a record payload
	MaxRecordSize = 32 * 1024 // 32KB

	// Default WAL file size
	DefaultWALFileSize = 64 * 1024 * 1024 // 64MB
)

var (
	ErrCorruptRecord     = errors.New("corrupt record")
	ErrInvalidRecordType = errors.New("invalid record type")
	ErrInvalidOpType     = errors.New("invalid operation type")
	ErrWALClosed         = errors.New("WAL is closed")
	ErrWALRotating       = errors.New("WAL is rotating")
	ErrWALFull           = errors.New("WAL file is full")
)

// Entry represents a logical entry in the WAL
type Entry struct {
	SequenceNumber uint64
	Type           uint8 // OpTypePut, OpTypeDelete, etc.
	Key            []byte
	Value          []byte
}

// Global variable to control whether to print recovery logs
var DisableRecoveryLogs bool = false

// WAL status constants
const (
	WALStatusActive   = 0
	WALStatusRotating = 1
	WALStatusClosed   = 2
)

// WAL represents a write-ahead log
type WAL struct {
	cfg           *config.Config
	dir           string
	file          *os.File
	writer        *bufio.Writer
	nextSequence  uint64
	bytesWritten  int64
	lastSync      time.Time
	batchByteSize int64
	status        int32 // Using atomic int32 for status flags
	closed        int32 // Atomic flag indicating if WAL is closed
	mu            sync.Mutex
	
	// Observer-related fields
	observers   map[string]WALEntryObserver
	observersMu sync.RWMutex
}

// NewWAL creates a new write-ahead log
func NewWAL(cfg *config.Config, dir string) (*WAL, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Create a new WAL file
	filename := fmt.Sprintf("%020d.wal", time.Now().UnixNano())
	path := filepath.Join(dir, filename)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL file: %w", err)
	}

	wal := &WAL{
		cfg:          cfg,
		dir:          dir,
		file:         file,
		writer:       bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		nextSequence: 1,
		lastSync:     time.Now(),
		status:       WALStatusActive,
		observers:    make(map[string]WALEntryObserver),
	}

	return wal, nil
}

// ReuseWAL attempts to reuse an existing WAL file for appending
// Returns nil, nil if no suitable WAL file is found
func ReuseWAL(cfg *config.Config, dir string, nextSeq uint64) (*WAL, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	// Find existing WAL files
	files, err := FindWALFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to find WAL files: %w", err)
	}

	// No files found
	if len(files) == 0 {
		return nil, nil
	}

	// Try the most recent one (last in sorted order)
	latestWAL := files[len(files)-1]

	// Try to open for append
	file, err := os.OpenFile(latestWAL, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		// Don't log in tests
		if !DisableRecoveryLogs {
			fmt.Printf("Cannot open latest WAL for append: %v\n", err)
		}
		return nil, nil
	}

	// Check if file is not too large
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}

	// Define maximum WAL size to check against
	maxWALSize := int64(64 * 1024 * 1024) // Default 64MB
	if cfg.WALMaxSize > 0 {
		maxWALSize = cfg.WALMaxSize
	}

	if stat.Size() >= maxWALSize {
		file.Close()
		if !DisableRecoveryLogs {
			fmt.Printf("Latest WAL file is too large to reuse (%d bytes)\n", stat.Size())
		}
		return nil, nil
	}

	if !DisableRecoveryLogs {
		fmt.Printf("Reusing existing WAL file: %s with next sequence %d\n",
			latestWAL, nextSeq)
	}

	wal := &WAL{
		cfg:          cfg,
		dir:          dir,
		file:         file,
		writer:       bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		nextSequence: nextSeq,
		bytesWritten: stat.Size(),
		lastSync:     time.Now(),
		status:       WALStatusActive,
		observers:    make(map[string]WALEntryObserver),
	}

	return wal, nil
}

// Append adds an entry to the WAL
func (w *WAL) Append(entryType uint8, key, value []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	status := atomic.LoadInt32(&w.status)
	if status == WALStatusClosed {
		return 0, ErrWALClosed
	} else if status == WALStatusRotating {
		return 0, ErrWALRotating
	}

	if entryType != OpTypePut && entryType != OpTypeDelete && entryType != OpTypeMerge {
		return 0, ErrInvalidOpType
	}

	// Sequence number for this entry
	seqNum := w.nextSequence
	w.nextSequence++

	// Encode the entry
	// Format: type(1) + seq(8) + keylen(4) + key + vallen(4) + val
	entrySize := 1 + 8 + 4 + len(key)
	if entryType != OpTypeDelete {
		entrySize += 4 + len(value)
	}

	// Check if we need to split the record
	if entrySize <= MaxRecordSize {
		// Single record case
		recordType := uint8(RecordTypeFull)
		if err := w.writeRecord(recordType, entryType, seqNum, key, value); err != nil {
			return 0, err
		}
	} else {
		// Split into multiple records
		if err := w.writeFragmentedRecord(entryType, seqNum, key, value); err != nil {
			return 0, err
		}
	}
	
	// Create an entry object for notification
	entry := &Entry{
		SequenceNumber: seqNum,
		Type:           entryType,
		Key:            key,
		Value:          value,
	}
	
	// Notify observers of the new entry
	w.notifyEntryObservers(entry)

	// Sync the file if needed
	if err := w.maybeSync(); err != nil {
		return 0, err
	}

	return seqNum, nil
}

// Write a single record
func (w *WAL) writeRecord(recordType uint8, entryType uint8, seqNum uint64, key, value []byte) error {
	// Calculate the record size
	payloadSize := 1 + 8 + 4 + len(key) // type + seq + keylen + key
	if entryType != OpTypeDelete {
		payloadSize += 4 + len(value) // vallen + value
	}

	if payloadSize > MaxRecordSize {
		return fmt.Errorf("record too large: %d > %d", payloadSize, MaxRecordSize)
	}

	// Prepare the header
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint16(header[4:6], uint16(payloadSize))
	header[6] = recordType

	// Prepare the payload
	payload := make([]byte, payloadSize)
	offset := 0

	// Write entry type
	payload[offset] = entryType
	offset++

	// Write sequence number
	binary.LittleEndian.PutUint64(payload[offset:offset+8], seqNum)
	offset += 8

	// Write key length and key
	binary.LittleEndian.PutUint32(payload[offset:offset+4], uint32(len(key)))
	offset += 4
	copy(payload[offset:], key)
	offset += len(key)

	// Write value length and value (if applicable)
	if entryType != OpTypeDelete {
		binary.LittleEndian.PutUint32(payload[offset:offset+4], uint32(len(value)))
		offset += 4
		copy(payload[offset:], value)
	}

	// Calculate CRC
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(header[0:4], crc)

	// Write the record
	if _, err := w.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write record header: %w", err)
	}
	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("failed to write record payload: %w", err)
	}

	// Update bytes written
	w.bytesWritten += int64(HeaderSize + payloadSize)
	w.batchByteSize += int64(HeaderSize + payloadSize)

	return nil
}

// writeRawRecord writes a raw record with provided data as payload
func (w *WAL) writeRawRecord(recordType uint8, data []byte) error {
	if len(data) > MaxRecordSize {
		return fmt.Errorf("record too large: %d > %d", len(data), MaxRecordSize)
	}

	// Prepare the header
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(data)))
	header[6] = recordType

	// Calculate CRC
	crc := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(header[0:4], crc)

	// Write the record
	if _, err := w.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write record header: %w", err)
	}
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write record payload: %w", err)
	}

	// Update bytes written
	w.bytesWritten += int64(HeaderSize + len(data))
	w.batchByteSize += int64(HeaderSize + len(data))

	return nil
}

// Write a fragmented record
func (w *WAL) writeFragmentedRecord(entryType uint8, seqNum uint64, key, value []byte) error {
	// First fragment contains metadata: type, sequence, key length, and as much of the key as fits
	headerSize := 1 + 8 + 4 // type + seq + keylen

	// Calculate how much of the key can fit in the first fragment
	maxKeyInFirst := MaxRecordSize - headerSize
	keyInFirst := min(len(key), maxKeyInFirst)

	// Create the first fragment
	firstFragment := make([]byte, headerSize+keyInFirst)
	offset := 0

	// Add metadata to first fragment
	firstFragment[offset] = entryType
	offset++

	binary.LittleEndian.PutUint64(firstFragment[offset:offset+8], seqNum)
	offset += 8

	binary.LittleEndian.PutUint32(firstFragment[offset:offset+4], uint32(len(key)))
	offset += 4

	// Add as much of the key as fits
	copy(firstFragment[offset:], key[:keyInFirst])

	// Write the first fragment
	if err := w.writeRawRecord(uint8(RecordTypeFirst), firstFragment); err != nil {
		return err
	}

	// Prepare the remaining data
	var remaining []byte

	// Add any remaining key bytes
	if keyInFirst < len(key) {
		remaining = append(remaining, key[keyInFirst:]...)
	}

	// Add value data if this isn't a delete operation
	if entryType != OpTypeDelete {
		// Add value length
		valueLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueLenBuf, uint32(len(value)))
		remaining = append(remaining, valueLenBuf...)

		// Add value
		remaining = append(remaining, value...)
	}

	// Write middle fragments (all full-sized except possibly the last)
	for len(remaining) > MaxRecordSize {
		chunk := remaining[:MaxRecordSize]
		remaining = remaining[MaxRecordSize:]

		if err := w.writeRawRecord(uint8(RecordTypeMiddle), chunk); err != nil {
			return err
		}
	}

	// Write the last fragment if there's any remaining data
	if len(remaining) > 0 {
		if err := w.writeRawRecord(uint8(RecordTypeLast), remaining); err != nil {
			return err
		}
	}

	return nil
}

// maybeSync syncs the WAL file if needed based on configuration
func (w *WAL) maybeSync() error {
	needSync := false

	switch w.cfg.WALSyncMode {
	case config.SyncImmediate:
		needSync = true
	case config.SyncBatch:
		// Sync if we've written enough bytes
		if w.batchByteSize >= w.cfg.WALSyncBytes {
			needSync = true
		}
	case config.SyncNone:
		// No syncing
	}

	if needSync {
		// Use syncLocked since we're already holding the mutex
		if err := w.syncLocked(); err != nil {
			return err
		}
	}

	return nil
}

// syncLocked performs the sync operation assuming the mutex is already held
func (w *WAL) syncLocked() error {
	status := atomic.LoadInt32(&w.status)
	if status == WALStatusClosed {
		return ErrWALClosed
	} else if status == WALStatusRotating {
		return ErrWALRotating
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	w.lastSync = time.Now()
	w.batchByteSize = 0
	
	// Notify observers about the sync
	w.notifySyncObservers(w.nextSequence - 1)

	return nil
}

// Sync flushes all buffered data to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.syncLocked()
}

// AppendBatch adds a batch of entries to the WAL
func (w *WAL) AppendBatch(entries []*Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	status := atomic.LoadInt32(&w.status)
	if status == WALStatusClosed {
		return 0, ErrWALClosed
	} else if status == WALStatusRotating {
		return 0, ErrWALRotating
	}

	if len(entries) == 0 {
		return w.nextSequence, nil
	}

	// Start sequence number for the batch
	startSeqNum := w.nextSequence

	// Record this as a batch operation with the number of entries
	batchHeader := make([]byte, 1+8+4) // opType(1) + seqNum(8) + entryCount(4)
	offset := 0

	// Write operation type (batch)
	batchHeader[offset] = OpTypeBatch
	offset++

	// Write sequence number
	binary.LittleEndian.PutUint64(batchHeader[offset:offset+8], startSeqNum)
	offset += 8

	// Write entry count
	binary.LittleEndian.PutUint32(batchHeader[offset:offset+4], uint32(len(entries)))

	// Write the batch header
	if err := w.writeRawRecord(RecordTypeFull, batchHeader); err != nil {
		return 0, fmt.Errorf("failed to write batch header: %w", err)
	}

	// Process each entry in the batch
	for i, entry := range entries {
		// Assign sequential sequence numbers to each entry
		seqNum := startSeqNum + uint64(i)

		// Write the entry
		if entry.Value == nil {
			// Deletion
			if err := w.writeRecord(RecordTypeFull, OpTypeDelete, seqNum, entry.Key, nil); err != nil {
				return 0, fmt.Errorf("failed to write entry %d: %w", i, err)
			}
		} else {
			// Put
			if err := w.writeRecord(RecordTypeFull, OpTypePut, seqNum, entry.Key, entry.Value); err != nil {
				return 0, fmt.Errorf("failed to write entry %d: %w", i, err)
			}
		}
	}

	// Update next sequence number
	w.nextSequence = startSeqNum + uint64(len(entries))
	
	// Notify observers about the batch
	w.notifyBatchObservers(startSeqNum, entries)

	// Sync if needed
	if err := w.maybeSync(); err != nil {
		return 0, err
	}

	return startSeqNum, nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	status := atomic.LoadInt32(&w.status)
	if status == WALStatusClosed {
		return nil
	}

	// Flush the buffer first before changing status
	// This ensures all data is flushed to disk even if status is changing
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer during close: %w", err)
	}
	
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file during close: %w", err)
	}
	
	// Now mark as rotating to block new operations
	atomic.StoreInt32(&w.status, WALStatusRotating)

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	atomic.StoreInt32(&w.status, WALStatusClosed)
	return nil
}

// SetRotating marks the WAL as rotating
func (w *WAL) SetRotating() {
	atomic.StoreInt32(&w.status, WALStatusRotating)
}

// SetActive marks the WAL as active
func (w *WAL) SetActive() {
	atomic.StoreInt32(&w.status, WALStatusActive)
}

// UpdateNextSequence sets the next sequence number for the WAL
// This is used after recovery to ensure new entries have increasing sequence numbers
func (w *WAL) UpdateNextSequence(nextSeq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if nextSeq > w.nextSequence {
		w.nextSequence = nextSeq
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RegisterObserver adds an observer to be notified of WAL operations
func (w *WAL) RegisterObserver(id string, observer WALEntryObserver) {
	if observer == nil {
		return
	}

	w.observersMu.Lock()
	defer w.observersMu.Unlock()

	w.observers[id] = observer
}

// UnregisterObserver removes an observer
func (w *WAL) UnregisterObserver(id string) {
	w.observersMu.Lock()
	defer w.observersMu.Unlock()

	delete(w.observers, id)
}

// notifyEntryObservers sends notifications for a single entry
func (w *WAL) notifyEntryObservers(entry *Entry) {
	w.observersMu.RLock()
	defer w.observersMu.RUnlock()

	for _, observer := range w.observers {
		observer.OnWALEntryWritten(entry)
	}
}

// notifyBatchObservers sends notifications for a batch of entries
func (w *WAL) notifyBatchObservers(startSeq uint64, entries []*Entry) {
	w.observersMu.RLock()
	defer w.observersMu.RUnlock()

	for _, observer := range w.observers {
		observer.OnWALBatchWritten(startSeq, entries)
	}
}

// notifySyncObservers notifies observers when WAL is synced
func (w *WAL) notifySyncObservers(upToSeq uint64) {
	w.observersMu.RLock()
	defer w.observersMu.RUnlock()

	for _, observer := range w.observers {
		observer.OnWALSync(upToSeq)
	}
}

// GetEntriesFrom retrieves WAL entries starting from the given sequence number
func (w *WAL) GetEntriesFrom(sequenceNumber uint64) ([]*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	status := atomic.LoadInt32(&w.status)
	if status == WALStatusClosed {
		return nil, ErrWALClosed
	}
	
	// If we're requesting future entries, return empty slice
	if sequenceNumber >= w.nextSequence {
		return []*Entry{}, nil
	}
	
	// Ensure current WAL file is synced so Reader can access consistent data
	if err := w.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	
	// Find all WAL files
	files, err := FindWALFiles(w.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to find WAL files: %w", err)
	}
	
	currentFilePath := w.file.Name()
	currentFileName := filepath.Base(currentFilePath)
	
	// Process files in chronological order (oldest first)
	// This preserves the WAL ordering which is critical
	var result []*Entry
	
	// First process all older files
	for _, file := range files {
		fileName := filepath.Base(file)
		
		// Skip current file (we'll process it last to get the latest data)
		if fileName == currentFileName {
			continue
		}
		
		// Try to find entries in this file
		fileEntries, err := w.getEntriesFromFile(file, sequenceNumber)
		if err != nil {
			// Log error but continue with other files
			continue
		}
		
		// Append entries maintaining chronological order
		result = append(result, fileEntries...)
	}
	
	// Finally, process the current file
	currentEntries, err := w.getEntriesFromFile(currentFilePath, sequenceNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get entries from current WAL file: %w", err)
	}
	
	// Append the current entries at the end (they are the most recent)
	result = append(result, currentEntries...)
	
	return result, nil
}

// getEntriesFromFile reads entries from a specific WAL file starting from a sequence number
func (w *WAL) getEntriesFromFile(filename string, minSequence uint64) ([]*Entry, error) {
	reader, err := OpenReader(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for %s: %w", filename, err)
	}
	defer reader.Close()
	
	var entries []*Entry
	
	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			// Skip corrupted entries but continue reading
			if strings.Contains(err.Error(), "corrupt") || strings.Contains(err.Error(), "invalid") {
				continue
			}
			return entries, err
		}
		
		// Store only entries with sequence numbers >= the minimum requested
		if entry.SequenceNumber >= minSequence {
			entries = append(entries, entry)
		}
	}
	
	return entries, nil
}
