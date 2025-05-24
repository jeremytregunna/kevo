package block

import (
	"bytes"
	"encoding/binary"
)

// Iterator allows iterating through key-value pairs in a block
type Iterator struct {
	reader        *Reader
	currentPos    uint32
	currentKey    []byte
	currentVal    []byte
	currentSeqNum uint64 // Sequence number of the current entry
	restartIdx    int
	initialized   bool
	dataEnd       uint32 // Position where the actual entries data ends (before restart points)
}

// SeekToFirst positions the iterator at the first entry
func (it *Iterator) SeekToFirst() {
	if len(it.reader.restartPoints) == 0 {
		it.currentKey = nil
		it.currentVal = nil
		it.initialized = true
		return
	}

	it.currentPos = 0
	it.restartIdx = 0
	it.initialized = true

	key, val, ok := it.decodeCurrent()
	if ok {
		it.currentKey = key
		it.currentVal = val
	} else {
		it.currentKey = nil
		it.currentVal = nil
	}
}

// SeekToLast positions the iterator at the last entry
func (it *Iterator) SeekToLast() {
	if len(it.reader.restartPoints) == 0 {
		it.currentKey = nil
		it.currentVal = nil
		it.initialized = true
		return
	}

	// Start from the last restart point
	it.restartIdx = len(it.reader.restartPoints) - 1
	it.currentPos = it.reader.restartPoints[it.restartIdx]
	it.initialized = true

	// Skip forward to the last entry
	key, val, ok := it.decodeCurrent()
	if !ok {
		it.currentKey = nil
		it.currentVal = nil
		return
	}

	it.currentKey = key
	it.currentVal = val

	// Continue moving forward as long as there are more entries
	for {
		lastPos := it.currentPos
		lastKey := it.currentKey
		lastVal := it.currentVal

		key, val, ok = it.decodeNext()
		if !ok {
			// Restore position to the last valid entry
			it.currentPos = lastPos
			it.currentKey = lastKey
			it.currentVal = lastVal
			return
		}

		it.currentKey = key
		it.currentVal = val
	}
}

// Seek positions the iterator at the first key >= target
func (it *Iterator) Seek(target []byte) bool {
	if len(it.reader.restartPoints) == 0 {
		return false
	}

	// Binary search through restart points
	left, right := 0, len(it.reader.restartPoints)-1
	for left < right {
		mid := (left + right) / 2
		it.restartIdx = mid
		it.currentPos = it.reader.restartPoints[mid]

		key, _, ok := it.decodeCurrent()
		if !ok {
			return false
		}

		if bytes.Compare(key, target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// Position at the found restart point
	it.restartIdx = left
	it.currentPos = it.reader.restartPoints[left]
	it.initialized = true

	// First check the current position
	key, val, ok := it.decodeCurrent()
	if !ok {
		return false
	}

	// If the key at this position is already >= target, we're done
	if bytes.Compare(key, target) >= 0 {
		it.currentKey = key
		it.currentVal = val
		return true
	}

	// Otherwise, scan forward until we find the first key >= target
	for {
		savePos := it.currentPos
		key, val, ok = it.decodeNext()
		if !ok {
			// Restore position to the last valid entry
			it.currentPos = savePos
			key, val, ok = it.decodeCurrent()
			if ok {
				it.currentKey = key
				it.currentVal = val
				return true
			}
			return false
		}

		if bytes.Compare(key, target) >= 0 {
			it.currentKey = key
			it.currentVal = val
			return true
		}

		// Update current key/value for the next iteration
		it.currentKey = key
		it.currentVal = val
	}
}

// Next advances the iterator to the next entry
func (it *Iterator) Next() bool {
	if !it.initialized {
		it.SeekToFirst()
		return it.Valid()
	}

	if it.currentKey == nil {
		return false
	}

	key, val, ok := it.decodeNext()
	if !ok {
		it.currentKey = nil
		it.currentVal = nil
		return false
	}

	it.currentKey = key
	it.currentVal = val
	return true
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	return it.currentKey
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	return it.currentVal
}

// Valid returns true if the iterator is positioned at a valid entry
func (it *Iterator) Valid() bool {
	return it.currentKey != nil && len(it.currentKey) > 0
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *Iterator) IsTombstone() bool {
	// For block iterators, a nil value means it's a tombstone
	return it.Valid() && it.currentVal == nil
}

// SequenceNumber returns the sequence number of the current entry
func (it *Iterator) SequenceNumber() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.currentSeqNum
}

// decodeCurrent decodes the entry at the current position
func (it *Iterator) decodeCurrent() ([]byte, []byte, bool) {
	if it.currentPos >= it.dataEnd {
		return nil, nil, false
	}

	data := it.reader.data[it.currentPos:]

	// Read key
	if len(data) < 2 {
		return nil, nil, false
	}
	keyLen := binary.LittleEndian.Uint16(data)
	data = data[2:]
	if uint32(len(data)) < uint32(keyLen) {
		return nil, nil, false
	}

	key := make([]byte, keyLen)
	copy(key, data[:keyLen])
	data = data[keyLen:]

	// Read sequence number if format includes it (check if enough data for both seq num and value len)
	seqNum := uint64(0)
	if len(data) >= 12 { // 8 for seq num + 4 for value len
		seqNum = binary.LittleEndian.Uint64(data)
		data = data[8:]
	}

	// Read value
	if len(data) < 4 {
		return nil, nil, false
	}

	valueLen := binary.LittleEndian.Uint32(data)
	data = data[4:]

	if uint32(len(data)) < valueLen {
		return nil, nil, false
	}

	value := make([]byte, valueLen)
	copy(value, data[:valueLen])

	it.currentKey = key
	it.currentVal = value
	it.currentSeqNum = seqNum

	return key, value, true
}

// decodeNext decodes the next entry
func (it *Iterator) decodeNext() ([]byte, []byte, bool) {
	if it.currentPos >= it.dataEnd {
		return nil, nil, false
	}

	data := it.reader.data[it.currentPos:]
	var key []byte

	// Check if we're at a restart point
	isRestart := false
	for i, offset := range it.reader.restartPoints {
		if offset == it.currentPos {
			isRestart = true
			it.restartIdx = i
			break
		}
	}

	if isRestart || it.currentKey == nil {
		// Full key at restart point
		if len(data) < 2 {
			return nil, nil, false
		}

		keyLen := binary.LittleEndian.Uint16(data)
		data = data[2:]

		if uint32(len(data)) < uint32(keyLen) {
			return nil, nil, false
		}

		key = make([]byte, keyLen)
		copy(key, data[:keyLen])
		data = data[keyLen:]
		it.currentPos += 2 + uint32(keyLen)
	} else {
		// Delta-encoded key
		if len(data) < 4 {
			return nil, nil, false
		}

		sharedLen := binary.LittleEndian.Uint16(data)
		data = data[2:]
		unsharedLen := binary.LittleEndian.Uint16(data)
		data = data[2:]

		if sharedLen > uint16(len(it.currentKey)) ||
			uint32(len(data)) < uint32(unsharedLen) {
			return nil, nil, false
		}

		// Reconstruct key: shared prefix + unshared suffix
		key = make([]byte, sharedLen+unsharedLen)
		copy(key[:sharedLen], it.currentKey[:sharedLen])
		copy(key[sharedLen:], data[:unsharedLen])

		data = data[unsharedLen:]
		it.currentPos += 4 + uint32(unsharedLen)
	}

	// Read sequence number if format includes it (check if enough data for both seq num and value len)
	seqNum := uint64(0)
	if len(data) >= 12 { // 8 for seq num + 4 for value len
		seqNum = binary.LittleEndian.Uint64(data)
		data = data[8:]
		it.currentPos += 8
	}

	// Read value
	if len(data) < 4 {
		return nil, nil, false
	}

	valueLen := binary.LittleEndian.Uint32(data)
	data = data[4:]

	if uint32(len(data)) < valueLen {
		return nil, nil, false
	}

	value := make([]byte, valueLen)
	copy(value, data[:valueLen])

	it.currentSeqNum = seqNum
	it.currentPos += 4 + uint32(valueLen)

	return key, value, true
}
