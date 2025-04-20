package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jer/kevo/pkg/config"
	"github.com/jer/kevo/pkg/sstable"
)

// BaseCompactionStrategy provides common functionality for compaction strategies
type BaseCompactionStrategy struct {
	// Configuration
	cfg *config.Config

	// SSTable directory
	sstableDir string

	// File information by level
	levels map[int][]*SSTableInfo
}

// NewBaseCompactionStrategy creates a new base compaction strategy
func NewBaseCompactionStrategy(cfg *config.Config, sstableDir string) *BaseCompactionStrategy {
	return &BaseCompactionStrategy{
		cfg:        cfg,
		sstableDir: sstableDir,
		levels:     make(map[int][]*SSTableInfo),
	}
}

// LoadSSTables scans the SSTable directory and loads metadata for all files
func (s *BaseCompactionStrategy) LoadSSTables() error {
	// Clear existing data
	s.levels = make(map[int][]*SSTableInfo)

	// Read all files from the SSTable directory
	entries, err := os.ReadDir(s.sstableDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet
		}
		return fmt.Errorf("failed to read SSTable directory: %w", err)
	}

	// Parse filenames and collect information
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue // Skip directories and non-SSTable files
		}

		// Parse filename to extract level, sequence, and timestamp
		// Filename format: level_sequence_timestamp.sst
		var level int
		var sequence uint64
		var timestamp int64

		if n, err := fmt.Sscanf(entry.Name(), "%d_%06d_%020d.sst",
			&level, &sequence, &timestamp); n != 3 || err != nil {
			// Skip files that don't match our naming pattern
			continue
		}

		// Get file info for size
		fi, err := entry.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", entry.Name(), err)
		}

		// Open the file to extract key range information
		path := filepath.Join(s.sstableDir, entry.Name())
		reader, err := sstable.OpenReader(path)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}

		// Create iterator to get first and last keys
		iter := reader.NewIterator()
		var firstKey, lastKey []byte

		// Get first key
		iter.SeekToFirst()
		if iter.Valid() {
			firstKey = append([]byte{}, iter.Key()...)
		}

		// Get last key
		iter.SeekToLast()
		if iter.Valid() {
			lastKey = append([]byte{}, iter.Key()...)
		}

		// Create SSTable info
		info := &SSTableInfo{
			Path:      path,
			Level:     level,
			Sequence:  sequence,
			Timestamp: timestamp,
			Size:      fi.Size(),
			KeyCount:  reader.GetKeyCount(),
			FirstKey:  firstKey,
			LastKey:   lastKey,
			Reader:    reader,
		}

		// Add to appropriate level
		s.levels[level] = append(s.levels[level], info)
	}

	// Sort files within each level by sequence number
	for level, files := range s.levels {
		sort.Slice(files, func(i, j int) bool {
			return files[i].Sequence < files[j].Sequence
		})
		s.levels[level] = files
	}

	return nil
}

// Close closes all open SSTable readers
func (s *BaseCompactionStrategy) Close() error {
	var lastErr error

	for _, files := range s.levels {
		for _, file := range files {
			if file.Reader != nil {
				if err := file.Reader.Close(); err != nil && lastErr == nil {
					lastErr = err
				}
				file.Reader = nil
			}
		}
	}

	return lastErr
}

// GetLevelSize returns the total size of all files in a level
func (s *BaseCompactionStrategy) GetLevelSize(level int) int64 {
	var size int64
	for _, file := range s.levels[level] {
		size += file.Size
	}
	return size
}
