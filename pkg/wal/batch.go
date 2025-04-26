package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
)

const (
	BatchHeaderSize = 12 // count(4) + seq(8)
)

var (
	ErrEmptyBatch    = errors.New("batch is empty")
	ErrBatchTooLarge = errors.New("batch too large")
)

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Type  uint8 // OpTypePut, OpTypeDelete, etc.
	Key   []byte
	Value []byte
}

// Batch represents a collection of operations to be performed atomically
type Batch struct {
	Operations []BatchOperation
	Seq        uint64 // Base sequence number
}

// NewBatch creates a new empty batch
func NewBatch() *Batch {
	return &Batch{
		Operations: make([]BatchOperation, 0, 16),
	}
}

// Put adds a Put operation to the batch
func (b *Batch) Put(key, value []byte) {
	b.Operations = append(b.Operations, BatchOperation{
		Type:  OpTypePut,
		Key:   key,
		Value: value,
	})
}

// Delete adds a Delete operation to the batch
func (b *Batch) Delete(key []byte) {
	b.Operations = append(b.Operations, BatchOperation{
		Type: OpTypeDelete,
		Key:  key,
	})
}

// Count returns the number of operations in the batch
func (b *Batch) Count() int {
	return len(b.Operations)
}

// Reset clears all operations from the batch
func (b *Batch) Reset() {
	b.Operations = b.Operations[:0]
	b.Seq = 0
}

// Size estimates the size of the batch in the WAL
func (b *Batch) Size() int {
	size := BatchHeaderSize // count + seq

	for _, op := range b.Operations {
		// Type(1) + KeyLen(4) + Key
		size += 1 + 4 + len(op.Key)

		// ValueLen(4) + Value for Put operations
		if op.Type != OpTypeDelete {
			size += 4 + len(op.Value)
		}
	}

	return size
}

// Write writes the batch to the WAL
func (b *Batch) Write(w *WAL) error {
	if len(b.Operations) == 0 {
		return ErrEmptyBatch
	}

	// Estimate batch size
	size := b.Size()
	if size > MaxRecordSize {
		return fmt.Errorf("%w: %d > %d", ErrBatchTooLarge, size, MaxRecordSize)
	}

	// Serialize batch
	data := make([]byte, size)
	offset := 0

	// Write count
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(b.Operations)))
	offset += 4

	// Write sequence base (will be set by WAL.AppendBatch)
	offset += 8

	// Write operations
	for _, op := range b.Operations {
		// Write type
		data[offset] = op.Type
		offset++

		// Write key length
		binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(op.Key)))
		offset += 4

		// Write key
		copy(data[offset:], op.Key)
		offset += len(op.Key)

		// Write value for non-delete operations
		if op.Type != OpTypeDelete {
			// Write value length
			binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(op.Value)))
			offset += 4

			// Write value
			copy(data[offset:], op.Value)
			offset += len(op.Value)
		}
	}

	// Append to WAL
	w.mu.Lock()
	defer w.mu.Unlock()

	if atomic.LoadInt32(&w.closed) == 1 {
		return ErrWALClosed
	}

	// Set the sequence number - use Lamport clock if available
	var seqNum uint64
	if w.clock != nil {
		// Generate Lamport timestamp for the batch
		seqNum = w.clock.Tick()
		// Keep the nextSequence in sync with the highest timestamp
		if seqNum >= w.nextSequence {
			w.nextSequence = seqNum + 1
		}
	} else {
		// Use traditional sequence number
		seqNum = w.nextSequence
		// Increment sequence for future operations
		w.nextSequence += uint64(len(b.Operations))
	}

	b.Seq = seqNum
	binary.LittleEndian.PutUint64(data[4:12], b.Seq)

	// Write as a batch entry
	if err := w.writeRecord(uint8(RecordTypeFull), OpTypeBatch, b.Seq, data, nil); err != nil {
		return err
	}

	// Sync if needed
	return w.maybeSync()
}

// DecodeBatch decodes a batch entry from a WAL record
func DecodeBatch(entry *Entry) (*Batch, error) {
	if entry.Type != OpTypeBatch {
		return nil, fmt.Errorf("not a batch entry: type %d", entry.Type)
	}

	// For batch entries, the batch data is in the Key field, not Value
	data := entry.Key
	if len(data) < BatchHeaderSize {
		return nil, fmt.Errorf("%w: batch header too small", ErrCorruptRecord)
	}

	// Read count and sequence
	count := binary.LittleEndian.Uint32(data[0:4])
	seq := binary.LittleEndian.Uint64(data[4:12])

	batch := &Batch{
		Operations: make([]BatchOperation, 0, count),
		Seq:        seq,
	}

	offset := BatchHeaderSize

	// Read operations
	for i := uint32(0); i < count; i++ {
		// Check if we have enough data for type
		if offset >= len(data) {
			return nil, fmt.Errorf("%w: unexpected end of batch data", ErrCorruptRecord)
		}

		// Read type
		opType := data[offset]
		offset++

		// Validate operation type
		if opType != OpTypePut && opType != OpTypeDelete && opType != OpTypeMerge {
			return nil, fmt.Errorf("%w: %d", ErrInvalidOpType, opType)
		}

		// Check if we have enough data for key length
		if offset+4 > len(data) {
			return nil, fmt.Errorf("%w: unexpected end of batch data", ErrCorruptRecord)
		}

		// Read key length
		keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Validate key length
		if offset+int(keyLen) > len(data) {
			return nil, fmt.Errorf("%w: invalid key length %d", ErrCorruptRecord, keyLen)
		}

		// Read key
		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		var value []byte
		if opType != OpTypeDelete {
			// Check if we have enough data for value length
			if offset+4 > len(data) {
				return nil, fmt.Errorf("%w: unexpected end of batch data", ErrCorruptRecord)
			}

			// Read value length
			valueLen := binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

			// Validate value length
			if offset+int(valueLen) > len(data) {
				return nil, fmt.Errorf("%w: invalid value length %d", ErrCorruptRecord, valueLen)
			}

			// Read value
			value = make([]byte, valueLen)
			copy(value, data[offset:offset+int(valueLen)])
			offset += int(valueLen)
		}

		batch.Operations = append(batch.Operations, BatchOperation{
			Type:  opType,
			Key:   key,
			Value: value,
		})
	}

	return batch, nil
}
