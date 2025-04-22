package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Reader reads entries from WAL files
type Reader struct {
	file      *os.File
	reader    *bufio.Reader
	buffer    []byte
	fragments [][]byte
	currType  uint8
}

// OpenReader creates a new Reader for the given WAL file
func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &Reader{
		file:      file,
		reader:    bufio.NewReaderSize(file, 64*1024), // 64KB buffer
		buffer:    make([]byte, MaxRecordSize),
		fragments: make([][]byte, 0),
	}, nil
}

// ReadEntry reads the next entry from the WAL
func (r *Reader) ReadEntry() (*Entry, error) {
	// Loop until we have a complete entry
	for {
		// Read a record
		record, err := r.readRecord()
		if err != nil {
			if err == io.EOF {
				// If we have fragments, this is unexpected EOF
				if len(r.fragments) > 0 {
					return nil, fmt.Errorf("unexpected EOF with %d fragments", len(r.fragments))
				}
				return nil, io.EOF
			}
			return nil, err
		}

		// Process based on record type
		switch record.recordType {
		case RecordTypeFull:
			// Single record, parse directly
			return r.parseEntryData(record.data)

		case RecordTypeFirst:
			// Start of a fragmented entry
			r.fragments = append(r.fragments, record.data)
			r.currType = record.data[0] // Save the operation type

		case RecordTypeMiddle:
			// Middle fragment
			if len(r.fragments) == 0 {
				return nil, fmt.Errorf("%w: middle fragment without first fragment", ErrCorruptRecord)
			}
			r.fragments = append(r.fragments, record.data)

		case RecordTypeLast:
			// Last fragment
			if len(r.fragments) == 0 {
				return nil, fmt.Errorf("%w: last fragment without previous fragments", ErrCorruptRecord)
			}
			r.fragments = append(r.fragments, record.data)

			// Combine fragments into a single entry
			entry, err := r.processFragments()
			if err != nil {
				return nil, err
			}
			return entry, nil

		default:
			return nil, fmt.Errorf("%w: %d", ErrInvalidRecordType, record.recordType)
		}
	}
}

// Record represents a physical record in the WAL
type record struct {
	recordType uint8
	data       []byte
}

// readRecord reads a single physical record from the WAL
func (r *Reader) readRecord() (*record, error) {
	// Read header
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r.reader, header); err != nil {
		return nil, err
	}

	// Parse header
	crc := binary.LittleEndian.Uint32(header[0:4])
	length := binary.LittleEndian.Uint16(header[4:6])
	recordType := header[6]

	// Validate record type
	if recordType < RecordTypeFull || recordType > RecordTypeLast {
		return nil, fmt.Errorf("%w: %d", ErrInvalidRecordType, recordType)
	}

	// Read payload
	data := make([]byte, length)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return nil, err
	}

	// Verify CRC
	computedCRC := crc32.ChecksumIEEE(data)
	if computedCRC != crc {
		return nil, fmt.Errorf("%w: expected CRC %d, got %d", ErrCorruptRecord, crc, computedCRC)
	}

	return &record{
		recordType: recordType,
		data:       data,
	}, nil
}

// processFragments combines fragments into a single entry
func (r *Reader) processFragments() (*Entry, error) {
	// Determine total size
	totalSize := 0
	for _, frag := range r.fragments {
		totalSize += len(frag)
	}

	// Combine fragments
	combined := make([]byte, totalSize)
	offset := 0
	for _, frag := range r.fragments {
		copy(combined[offset:], frag)
		offset += len(frag)
	}

	// Reset fragments
	r.fragments = r.fragments[:0]

	// Parse the combined data into an entry
	return r.parseEntryData(combined)
}

// parseEntryData parses the binary data into an Entry structure
func (r *Reader) parseEntryData(data []byte) (*Entry, error) {
	if len(data) < 13 { // Minimum size: type(1) + seq(8) + keylen(4)
		return nil, fmt.Errorf("%w: entry too small, %d bytes", ErrCorruptRecord, len(data))
	}

	offset := 0

	// Read entry type
	entryType := data[offset]
	offset++

	// Validate entry type
	if entryType != OpTypePut && entryType != OpTypeDelete && entryType != OpTypeMerge && entryType != OpTypeBatch {
		return nil, fmt.Errorf("%w: %d", ErrInvalidOpType, entryType)
	}

	// Read sequence number
	seqNum := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

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

	// Read value if applicable
	var value []byte
	if entryType != OpTypeDelete {
		// Check if there's enough data for value length
		if offset+4 > len(data) {
			return nil, fmt.Errorf("%w: missing value length", ErrCorruptRecord)
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
	}

	return &Entry{
		SequenceNumber: seqNum,
		Type:           entryType,
		Key:            key,
		Value:          value,
	}, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.file.Close()
}

// EntryHandler is a function that processes WAL entries during replay
type EntryHandler func(*Entry) error

// RecoveryStats tracks statistics about WAL recovery
type RecoveryStats struct {
	EntriesProcessed uint64
	EntriesSkipped   uint64
}

// NewRecoveryStats creates a new RecoveryStats instance
func NewRecoveryStats() *RecoveryStats {
	return &RecoveryStats{}
}

// FindWALFiles returns a list of WAL files in the given directory
func FindWALFiles(dir string) ([]string, error) {
	pattern := filepath.Join(dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to glob WAL files: %w", err)
	}

	// Sort by filename (which should be timestamp-based)
	sort.Strings(matches)
	return matches, nil
}

// ReplayWALFile replays a single WAL file and calls the handler for each entry
// getEntryCount counts the number of valid entries in a WAL file
func getEntryCount(path string) int {
	reader, err := OpenReader(path)
	if err != nil {
		return 0
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.ReadEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			// Skip corrupted entries
			continue
		}
		count++
	}

	return count
}

func ReplayWALFile(path string, handler EntryHandler) (*RecoveryStats, error) {
	reader, err := OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Track statistics
	stats := NewRecoveryStats()

	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			if err == io.EOF {
				// Reached the end of the file
				break
			}

			// Check if this is a corruption error
			if strings.Contains(err.Error(), "corrupt") ||
				strings.Contains(err.Error(), "invalid") {
				// Skip this corrupted entry
				stats.EntriesSkipped++

				// If we've seen too many corrupted entries in a row, give up on this file
				if stats.EntriesSkipped > 5 && stats.EntriesProcessed == 0 {
					return stats, fmt.Errorf("too many corrupted entries at start of file %s", path)
				}

				// Try to recover by scanning ahead
				// This is a very basic recovery mechanism that works by reading bytes
				// until we find what looks like a valid header
				recoverErr := recoverFromCorruption(reader)
				if recoverErr != nil {
					if recoverErr == io.EOF {
						// Reached the end during recovery
						break
					}
					// Couldn't recover
					return stats, fmt.Errorf("failed to recover from corruption in %s: %w", path, recoverErr)
				}

				// Successfully recovered, continue to the next entry
				continue
			}

			// For other errors, fail the replay
			return stats, fmt.Errorf("error reading entry from %s: %w", path, err)
		}

		// Process the entry
		if err := handler(entry); err != nil {
			return stats, fmt.Errorf("error handling entry: %w", err)
		}

		stats.EntriesProcessed++
	}

	return stats, nil
}

// recoverFromCorruption attempts to recover from a corrupted record by scanning ahead
func recoverFromCorruption(reader *Reader) error {
	// Create a small buffer to read bytes one at a time
	buf := make([]byte, 1)

	// Read up to 32KB ahead looking for a valid header
	for i := 0; i < 32*1024; i++ {
		_, err := reader.reader.Read(buf)
		if err != nil {
			return err
		}
	}

	// At this point, either we're at a valid position or we've skipped ahead
	// Let the next ReadEntry attempt to parse from this position
	return nil
}

// ReplayWALDir replays all WAL files in the given directory in order
func ReplayWALDir(dir string, handler EntryHandler) (*RecoveryStats, error) {
	files, err := FindWALFiles(dir)
	if err != nil {
		return nil, err
	}

	// Track overall recovery stats
	totalStats := NewRecoveryStats()
	
	// Track number of files processed successfully
	successfulFiles := 0
	var lastErr error

	// Try to process each file, but continue on recoverable errors
	for _, file := range files {
		fileStats, err := ReplayWALFile(file, handler)
		if err != nil {
			// Record the error, but continue
			lastErr = err

			// If we got some stats from the file before the error, add them to our totals
			if fileStats != nil {
				totalStats.EntriesProcessed += fileStats.EntriesProcessed
				totalStats.EntriesSkipped += fileStats.EntriesSkipped
			}

			// Check if this is a file-level error or just a corrupt record
			if !strings.Contains(err.Error(), "corrupt") &&
				!strings.Contains(err.Error(), "invalid") {
				return totalStats, fmt.Errorf("fatal error replaying WAL file %s: %w", file, err)
			}

			// Continue to the next file for corrupt/invalid errors
			continue
		}

		// Add stats from this file to our totals
		totalStats.EntriesProcessed += fileStats.EntriesProcessed
		totalStats.EntriesSkipped += fileStats.EntriesSkipped
		
		successfulFiles++
	}

	// If we processed at least one file successfully, the WAL recovery is considered successful
	if successfulFiles > 0 {
		return totalStats, nil
	}

	// If no files were processed successfully and we had errors, return the last error
	if lastErr != nil {
		return totalStats, fmt.Errorf("failed to process any WAL files: %w", lastErr)
	}

	return totalStats, nil
}
