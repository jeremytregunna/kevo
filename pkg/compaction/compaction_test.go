package compaction

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jer/kevo/pkg/config"
	"github.com/jer/kevo/pkg/sstable"
)

func createTestSSTable(t *testing.T, dir string, level, seq int, timestamp int64, keyValues map[string]string) string {
	filename := fmt.Sprintf("%d_%06d_%020d.sst", level, seq, timestamp)
	path := filepath.Join(dir, filename)

	writer, err := sstable.NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Get the keys and sort them to ensure they're added in order
	var keys []string
	for k := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Add keys in sorted order
	for _, k := range keys {
		if err := writer.Add([]byte(k), []byte(keyValues[k])); err != nil {
			t.Fatalf("Failed to add entry to SSTable: %v", err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}

	return path
}

func setupCompactionTest(t *testing.T) (string, *config.Config, func()) {
	// Create a temp directory for testing
	tempDir, err := os.MkdirTemp("", "compaction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create the SSTable directory
	sstDir := filepath.Join(tempDir, "sst")
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		t.Fatalf("Failed to create SSTable directory: %v", err)
	}

	// Create a test configuration
	cfg := &config.Config{
		Version:                config.CurrentManifestVersion,
		SSTDir:                 sstDir,
		CompactionLevels:       4,
		CompactionRatio:        10.0,
		CompactionThreads:      1,
		MaxMemTables:           2,
		SSTableMaxSize:         1000,
		MaxLevelWithTombstones: 3,
	}

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return sstDir, cfg, cleanup
}

func TestCompactorLoadSSTables(t *testing.T) {
	sstDir, cfg, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create test SSTables
	data1 := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	data2 := map[string]string{
		"d": "4",
		"e": "5",
		"f": "6",
	}

	// Keys will be sorted in the createTestSSTable function

	timestamp := time.Now().UnixNano()
	createTestSSTable(t, sstDir, 0, 1, timestamp, data1)
	createTestSSTable(t, sstDir, 0, 2, timestamp+1, data2)

	// Create the strategy
	strategy := NewBaseCompactionStrategy(cfg, sstDir)

	// Load SSTables
	err := strategy.LoadSSTables()
	if err != nil {
		t.Fatalf("Failed to load SSTables: %v", err)
	}

	// Verify the correct number of files was loaded
	if len(strategy.levels[0]) != 2 {
		t.Errorf("Expected 2 files in level 0, got %d", len(strategy.levels[0]))
	}

	// Verify key ranges
	for _, file := range strategy.levels[0] {
		if bytes.Equal(file.FirstKey, []byte("a")) {
			if !bytes.Equal(file.LastKey, []byte("c")) {
				t.Errorf("Expected last key 'c', got '%s'", string(file.LastKey))
			}
		} else if bytes.Equal(file.FirstKey, []byte("d")) {
			if !bytes.Equal(file.LastKey, []byte("f")) {
				t.Errorf("Expected last key 'f', got '%s'", string(file.LastKey))
			}
		} else {
			t.Errorf("Unexpected first key: %s", string(file.FirstKey))
		}
	}
}

func TestSSTableInfoOverlaps(t *testing.T) {
	// Create test SSTable info objects
	info1 := &SSTableInfo{
		FirstKey: []byte("a"),
		LastKey:  []byte("c"),
	}

	info2 := &SSTableInfo{
		FirstKey: []byte("b"),
		LastKey:  []byte("d"),
	}

	info3 := &SSTableInfo{
		FirstKey: []byte("e"),
		LastKey:  []byte("g"),
	}

	// Test overlapping ranges
	if !info1.Overlaps(info2) {
		t.Errorf("Expected info1 to overlap with info2")
	}

	if !info2.Overlaps(info1) {
		t.Errorf("Expected info2 to overlap with info1")
	}

	// Test non-overlapping ranges
	if info1.Overlaps(info3) {
		t.Errorf("Expected info1 not to overlap with info3")
	}

	if info3.Overlaps(info1) {
		t.Errorf("Expected info3 not to overlap with info1")
	}
}

func TestCompactorSelectLevel0Compaction(t *testing.T) {
	sstDir, cfg, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create 3 test SSTables in L0
	data1 := map[string]string{
		"a": "1",
		"b": "2",
	}

	data2 := map[string]string{
		"c": "3",
		"d": "4",
	}

	data3 := map[string]string{
		"e": "5",
		"f": "6",
	}

	timestamp := time.Now().UnixNano()
	createTestSSTable(t, sstDir, 0, 1, timestamp, data1)
	createTestSSTable(t, sstDir, 0, 2, timestamp+1, data2)
	createTestSSTable(t, sstDir, 0, 3, timestamp+2, data3)

	// Create the compactor
	// Create a tombstone tracker
	tracker := NewTombstoneTracker(24 * time.Hour)
	executor := NewCompactionExecutor(cfg, sstDir, tracker)
	// Create the compactor
	strategy := NewTieredCompactionStrategy(cfg, sstDir, executor)

	// Load SSTables
	err := strategy.LoadSSTables()
	if err != nil {
		t.Fatalf("Failed to load SSTables: %v", err)
	}

	// Select compaction task
	task, err := strategy.SelectCompaction()
	if err != nil {
		t.Fatalf("Failed to select compaction: %v", err)
	}

	// Verify the task
	if task == nil {
		t.Fatalf("Expected compaction task, got nil")
	}

	// L0 should have files to compact (since we have > cfg.MaxMemTables files)
	if len(task.InputFiles[0]) == 0 {
		t.Errorf("Expected L0 files to compact, got none")
	}

	// Target level should be 1
	if task.TargetLevel != 1 {
		t.Errorf("Expected target level 1, got %d", task.TargetLevel)
	}
}

func TestCompactFiles(t *testing.T) {
	sstDir, cfg, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create test SSTables with overlapping key ranges
	data1 := map[string]string{
		"a": "1-L0", // Will be overwritten by L1
		"b": "2-L0",
		"c": "3-L0",
	}

	data2 := map[string]string{
		"a": "1-L1", // Newer version than L0 (lower level has priority)
		"d": "4-L1",
		"e": "5-L1",
	}

	timestamp := time.Now().UnixNano()
	sstPath1 := createTestSSTable(t, sstDir, 0, 1, timestamp, data1)
	sstPath2 := createTestSSTable(t, sstDir, 1, 1, timestamp+1, data2)

	// Log the created test files
	t.Logf("Created test SSTables: %s, %s", sstPath1, sstPath2)

	// Create the compactor
	tracker := NewTombstoneTracker(24 * time.Hour)
	executor := NewCompactionExecutor(cfg, sstDir, tracker)
	strategy := NewBaseCompactionStrategy(cfg, sstDir)

	// Load SSTables
	err := strategy.LoadSSTables()
	if err != nil {
		t.Fatalf("Failed to load SSTables: %v", err)
	}

	// Create a compaction task
	task := &CompactionTask{
		InputFiles: map[int][]*SSTableInfo{
			0: {strategy.levels[0][0]},
			1: {strategy.levels[1][0]},
		},
		TargetLevel:        1,
		OutputPathTemplate: filepath.Join(sstDir, "%d_%06d_%020d.sst"),
	}

	// Perform compaction
	outputFiles, err := executor.CompactFiles(task)
	if err != nil {
		t.Fatalf("Failed to compact files: %v", err)
	}

	if len(outputFiles) == 0 {
		t.Fatalf("Expected output files, got none")
	}

	// Open the output file and verify its contents
	reader, err := sstable.OpenReader(outputFiles[0])
	if err != nil {
		t.Fatalf("Failed to open output SSTable: %v", err)
	}
	defer reader.Close()

	// Check each key
	checks := map[string]string{
		"a": "1-L0", // L0 has priority over L1
		"b": "2-L0",
		"c": "3-L0",
		"d": "4-L1",
		"e": "5-L1",
	}

	for k, expectedValue := range checks {
		value, err := reader.Get([]byte(k))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", k, err)
			continue
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Key %s: expected value '%s', got '%s'",
				k, expectedValue, string(value))
		}
	}

	// Clean up the output file
	for _, file := range outputFiles {
		os.Remove(file)
	}
}

func TestTombstoneTracking(t *testing.T) {
	// Create a tombstone tracker with a short retention period for testing
	tracker := NewTombstoneTracker(100 * time.Millisecond)

	// Add some tombstones
	tracker.AddTombstone([]byte("key1"))
	tracker.AddTombstone([]byte("key2"))

	// Should keep tombstones initially
	if !tracker.ShouldKeepTombstone([]byte("key1")) {
		t.Errorf("Expected to keep tombstone for key1")
	}

	if !tracker.ShouldKeepTombstone([]byte("key2")) {
		t.Errorf("Expected to keep tombstone for key2")
	}

	// Wait for the retention period to expire
	time.Sleep(200 * time.Millisecond)

	// Garbage collect expired tombstones
	tracker.CollectGarbage()

	// Should no longer keep the tombstones
	if tracker.ShouldKeepTombstone([]byte("key1")) {
		t.Errorf("Expected to discard tombstone for key1 after expiration")
	}

	if tracker.ShouldKeepTombstone([]byte("key2")) {
		t.Errorf("Expected to discard tombstone for key2 after expiration")
	}
}

func TestCompactionManager(t *testing.T) {
	sstDir, cfg, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create test SSTables in multiple levels
	data1 := map[string]string{
		"a": "1",
		"b": "2",
	}

	data2 := map[string]string{
		"c": "3",
		"d": "4",
	}

	data3 := map[string]string{
		"e": "5",
		"f": "6",
	}

	timestamp := time.Now().UnixNano()
	// Create test SSTables and remember their paths for verification
	sst1 := createTestSSTable(t, sstDir, 0, 1, timestamp, data1)
	sst2 := createTestSSTable(t, sstDir, 0, 2, timestamp+1, data2)
	sst3 := createTestSSTable(t, sstDir, 1, 1, timestamp+2, data3)

	// Log the created files for debugging
	t.Logf("Created test SSTables: %s, %s, %s", sst1, sst2, sst3)

	// Create the compaction manager
	manager := NewCompactionManager(cfg, sstDir)

	// Start the manager
	err := manager.Start()
	if err != nil {
		t.Fatalf("Failed to start compaction manager: %v", err)
	}

	// Force a compaction cycle
	err = manager.TriggerCompaction()
	if err != nil {
		t.Fatalf("Failed to trigger compaction: %v", err)
	}

	// Mark some files as obsolete
	manager.MarkFileObsolete(sst1)
	manager.MarkFileObsolete(sst2)

	// Clean up obsolete files
	err = manager.CleanupObsoleteFiles()
	if err != nil {
		t.Fatalf("Failed to clean up obsolete files: %v", err)
	}

	// Verify the files were deleted
	if _, err := os.Stat(sst1); !os.IsNotExist(err) {
		t.Errorf("Expected %s to be deleted, but it still exists", sst1)
	}

	if _, err := os.Stat(sst2); !os.IsNotExist(err) {
		t.Errorf("Expected %s to be deleted, but it still exists", sst2)
	}

	// Stop the manager
	err = manager.Stop()
	if err != nil {
		t.Fatalf("Failed to stop compaction manager: %v", err)
	}
}
