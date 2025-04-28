package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// WALRetentionConfig defines the configuration for WAL file retention.
type WALRetentionConfig struct {
	// Maximum number of WAL files to retain
	MaxFileCount int

	// Maximum age of WAL files to retain
	MaxAge time.Duration

	// Minimum sequence number to keep
	// Files containing entries with sequence numbers >= MinSequenceKeep will be retained
	MinSequenceKeep uint64
}

// WALFileInfo stores information about a WAL file for retention management
type WALFileInfo struct {
	Path      string    // Full path to the WAL file
	Size      int64     // Size of the file in bytes
	CreatedAt time.Time // Time when the file was created
	MinSeq    uint64    // Minimum sequence number in the file
	MaxSeq    uint64    // Maximum sequence number in the file
}

// ManageRetention applies the retention policy to WAL files.
// Returns the number of files deleted and any error encountered.
func (w *WAL) ManageRetention(config WALRetentionConfig) (int, error) {
	// Check if WAL is closed
	status := atomic.LoadInt32(&w.status)
	if status == WALStatusClosed {
		return 0, ErrWALClosed
	}

	// Get list of WAL files
	files, err := FindWALFiles(w.dir)
	if err != nil {
		return 0, fmt.Errorf("failed to find WAL files: %w", err)
	}

	// If no files or just one file (the current one), nothing to do
	if len(files) <= 1 {
		return 0, nil
	}

	// Get the current WAL file path (we should never delete this one)
	currentFile := ""
	w.mu.Lock()
	if w.file != nil {
		currentFile = w.file.Name()
	}
	w.mu.Unlock()

	// Collect file information for decision making
	var fileInfos []WALFileInfo
	now := time.Now()

	for _, filePath := range files {
		// Skip the current file
		if filePath == currentFile {
			continue
		}

		// Get file info
		stat, err := os.Stat(filePath)
		if err != nil {
			// Skip files we can't stat
			continue
		}

		// Extract timestamp from filename (assuming standard format)
		baseName := filepath.Base(filePath)
		fileTime := extractTimestampFromFilename(baseName)

		// Get sequence number bounds
		minSeq, maxSeq, err := getSequenceBounds(filePath)
		if err != nil {
			// If we can't determine sequence bounds, use conservative values
			minSeq = 0
			maxSeq = ^uint64(0) // Max uint64 value, to ensure we don't delete it based on sequence
		}

		fileInfos = append(fileInfos, WALFileInfo{
			Path:      filePath,
			Size:      stat.Size(),
			CreatedAt: fileTime,
			MinSeq:    minSeq,
			MaxSeq:    maxSeq,
		})
	}

	// Sort by creation time (oldest first)
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].CreatedAt.Before(fileInfos[j].CreatedAt)
	})

	// Apply retention policies
	toDelete := make(map[string]bool)

	// Apply file count retention if configured
	if config.MaxFileCount > 0 {
		// File count includes the current file, so we need to keep config.MaxFileCount - 1 old files
		filesLeftToKeep := config.MaxFileCount - 1

		// If count is 1 or less, we should delete all old files (keep only current)
		if filesLeftToKeep <= 0 {
			for _, fi := range fileInfos {
				toDelete[fi.Path] = true
			}
		} else if len(fileInfos) > filesLeftToKeep {
			// Otherwise, keep only the newest files, totalToKeep including current
			filesToDelete := len(fileInfos) - filesLeftToKeep

			for i := 0; i < filesToDelete; i++ {
				toDelete[fileInfos[i].Path] = true
			}
		}
	}

	// Apply age-based retention if configured
	if config.MaxAge > 0 {
		for _, fi := range fileInfos {
			age := now.Sub(fi.CreatedAt)
			if age > config.MaxAge {
				toDelete[fi.Path] = true
			}
		}
	}

	// Apply sequence-based retention if configured
	if config.MinSequenceKeep > 0 {
		for _, fi := range fileInfos {
			// If the highest sequence number in this file is less than what we need to keep,
			// we can safely delete this file
			if fi.MaxSeq < config.MinSequenceKeep {
				toDelete[fi.Path] = true
			}
		}
	}

	// Delete the files marked for deletion
	deleted := 0
	for _, fi := range fileInfos {
		if toDelete[fi.Path] {
			if err := os.Remove(fi.Path); err != nil {
				// Log the error but continue with other files
				continue
			}
			deleted++
		}
	}

	return deleted, nil
}

// extractTimestampFromFilename extracts the timestamp from a WAL filename
// WAL filenames are expected to be in the format: <timestamp>.wal
func extractTimestampFromFilename(filename string) time.Time {
	// Use file stat information to get the actual modification time
	info, err := os.Stat(filename)
	if err == nil {
		return info.ModTime()
	}

	// Fallback to parsing from filename if stat fails
	base := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	timestamp, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		// If parsing fails, return zero time
		return time.Time{}
	}

	// Convert nanoseconds to time
	return time.Unix(0, timestamp)
}

// getSequenceBounds scans a WAL file to determine the minimum and maximum sequence numbers
func getSequenceBounds(filePath string) (uint64, uint64, error) {
	reader, err := OpenReader(filePath)
	if err != nil {
		return 0, 0, err
	}
	defer reader.Close()

	var minSeq uint64 = ^uint64(0) // Max uint64 value
	var maxSeq uint64 = 0

	// Read all entries
	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			break // End of file or error
		}

		// Update min/max sequence
		if entry.SequenceNumber < minSeq {
			minSeq = entry.SequenceNumber
		}
		if entry.SequenceNumber > maxSeq {
			maxSeq = entry.SequenceNumber
		}
	}

	// If we didn't find any entries, return an error
	if minSeq == ^uint64(0) {
		return 0, 0, fmt.Errorf("no valid entries found in WAL file")
	}

	return minSeq, maxSeq, nil
}
