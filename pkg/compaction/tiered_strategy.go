package compaction

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/KevoDB/kevo/pkg/config"
)

// TieredCompactionStrategy implements a tiered compaction strategy
type TieredCompactionStrategy struct {
	*BaseCompactionStrategy

	// Executor for compacting files
	executor CompactionExecutor

	// Next file sequence number
	nextFileSeq uint64
}

// NewTieredCompactionStrategy creates a new tiered compaction strategy
func NewTieredCompactionStrategy(cfg *config.Config, sstableDir string, executor CompactionExecutor) *TieredCompactionStrategy {
	return &TieredCompactionStrategy{
		BaseCompactionStrategy: NewBaseCompactionStrategy(cfg, sstableDir),
		executor:               executor,
		nextFileSeq:            1,
	}
}

// SelectCompaction selects files for tiered compaction
func (s *TieredCompactionStrategy) SelectCompaction() (*CompactionTask, error) {
	// Determine the maximum level
	maxLevel := 0
	for level := range s.levels {
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Check L0 first (special case due to potential overlaps)
	if len(s.levels[0]) >= s.cfg.MaxMemTables {
		return s.selectL0Compaction()
	}

	// Check size-based conditions for other levels
	for level := 0; level < maxLevel; level++ {
		// If this level is too large compared to the next level
		thisLevelSize := s.GetLevelSize(level)
		nextLevelSize := s.GetLevelSize(level + 1)

		// If level is empty, skip it
		if thisLevelSize == 0 {
			continue
		}

		// If next level is empty, promote a file
		if nextLevelSize == 0 && len(s.levels[level]) > 0 {
			return s.selectPromotionCompaction(level)
		}

		// Check size ratio
		sizeRatio := float64(thisLevelSize) / float64(nextLevelSize)
		if sizeRatio >= s.cfg.CompactionRatio {
			return s.selectOverlappingCompaction(level)
		}
	}

	// No compaction needed
	return nil, nil
}

// selectL0Compaction selects files from L0 for compaction
func (s *TieredCompactionStrategy) selectL0Compaction() (*CompactionTask, error) {
	// Require at least some files in L0
	if len(s.levels[0]) < 2 {
		return nil, nil
	}

	// Sort L0 files by sequence number to prioritize older files
	files := make([]*SSTableInfo, len(s.levels[0]))
	copy(files, s.levels[0])
	sort.Slice(files, func(i, j int) bool {
		return files[i].Sequence < files[j].Sequence
	})

	// Take up to maxCompactFiles from L0
	maxCompactFiles := s.cfg.MaxMemTables
	if maxCompactFiles > len(files) {
		maxCompactFiles = len(files)
	}

	selectedFiles := files[:maxCompactFiles]

	// Determine the key range covered by selected files
	var minKey, maxKey []byte
	for _, file := range selectedFiles {
		if len(minKey) == 0 || bytes.Compare(file.FirstKey, minKey) < 0 {
			minKey = file.FirstKey
		}
		if len(maxKey) == 0 || bytes.Compare(file.LastKey, maxKey) > 0 {
			maxKey = file.LastKey
		}
	}

	// Find overlapping files in L1
	var l1Files []*SSTableInfo
	for _, file := range s.levels[1] {
		// Create a temporary SSTableInfo with the key range
		rangeInfo := &SSTableInfo{
			FirstKey: minKey,
			LastKey:  maxKey,
		}

		if file.Overlaps(rangeInfo) {
			l1Files = append(l1Files, file)
		}
	}

	// Create the compaction task
	task := &CompactionTask{
		InputFiles: map[int][]*SSTableInfo{
			0: selectedFiles,
			1: l1Files,
		},
		TargetLevel:        1,
		OutputPathTemplate: filepath.Join(s.sstableDir, "%d_%06d_%020d.sst"),
	}

	return task, nil
}

// selectPromotionCompaction selects a file to promote to the next level
func (s *TieredCompactionStrategy) selectPromotionCompaction(level int) (*CompactionTask, error) {
	// Sort files by sequence number
	files := make([]*SSTableInfo, len(s.levels[level]))
	copy(files, s.levels[level])
	sort.Slice(files, func(i, j int) bool {
		return files[i].Sequence < files[j].Sequence
	})

	// Select the oldest file
	file := files[0]

	// Create task to promote this file to the next level
	// No need to merge with any other files since the next level is empty
	task := &CompactionTask{
		InputFiles: map[int][]*SSTableInfo{
			level: {file},
		},
		TargetLevel:        level + 1,
		OutputPathTemplate: filepath.Join(s.sstableDir, "%d_%06d_%020d.sst"),
	}

	return task, nil
}

// selectOverlappingCompaction selects files for compaction based on key overlap
func (s *TieredCompactionStrategy) selectOverlappingCompaction(level int) (*CompactionTask, error) {
	// Sort files by sequence number to start with oldest
	files := make([]*SSTableInfo, len(s.levels[level]))
	copy(files, s.levels[level])
	sort.Slice(files, func(i, j int) bool {
		return files[i].Sequence < files[j].Sequence
	})

	// Select an initial file from this level
	file := files[0]

	// Find all overlapping files in the next level
	var nextLevelFiles []*SSTableInfo
	for _, nextFile := range s.levels[level+1] {
		if file.Overlaps(nextFile) {
			nextLevelFiles = append(nextLevelFiles, nextFile)
		}
	}

	// Create the compaction task
	task := &CompactionTask{
		InputFiles: map[int][]*SSTableInfo{
			level:     {file},
			level + 1: nextLevelFiles,
		},
		TargetLevel:        level + 1,
		OutputPathTemplate: filepath.Join(s.sstableDir, "%d_%06d_%020d.sst"),
	}

	return task, nil
}

// CompactRange performs compaction on a specific key range
func (s *TieredCompactionStrategy) CompactRange(minKey, maxKey []byte) error {
	// Create a range info to check for overlaps
	rangeInfo := &SSTableInfo{
		FirstKey: minKey,
		LastKey:  maxKey,
	}

	// Find files overlapping with the given range in each level
	task := &CompactionTask{
		InputFiles:         make(map[int][]*SSTableInfo),
		TargetLevel:        0, // Will be updated
		OutputPathTemplate: filepath.Join(s.sstableDir, "%d_%06d_%020d.sst"),
	}

	// Get the maximum level
	var maxLevel int
	for level := range s.levels {
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Find overlapping files in each level
	for level := 0; level <= maxLevel; level++ {
		var overlappingFiles []*SSTableInfo

		for _, file := range s.levels[level] {
			if file.Overlaps(rangeInfo) {
				overlappingFiles = append(overlappingFiles, file)
			}
		}

		if len(overlappingFiles) > 0 {
			task.InputFiles[level] = overlappingFiles
		}
	}

	// If no files overlap with the range, no compaction needed
	totalInputFiles := 0
	for _, files := range task.InputFiles {
		totalInputFiles += len(files)
	}

	if totalInputFiles == 0 {
		return nil
	}

	// Set target level to the maximum level + 1
	task.TargetLevel = maxLevel + 1

	// Perform the compaction
	_, err := s.executor.CompactFiles(task)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Gather all input file paths for cleanup
	var inputPaths []string
	for _, files := range task.InputFiles {
		for _, file := range files {
			inputPaths = append(inputPaths, file.Path)
		}
	}

	// Delete the original files that were compacted
	if err := s.executor.DeleteCompactedFiles(inputPaths); err != nil {
		return fmt.Errorf("failed to clean up compacted files: %w", err)
	}

	// Reload SSTables to refresh our file list
	if err := s.LoadSSTables(); err != nil {
		return fmt.Errorf("failed to reload SSTables: %w", err)
	}

	return nil
}
