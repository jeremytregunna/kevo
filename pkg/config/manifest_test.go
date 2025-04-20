package config

import (
	"os"
	"testing"
)

func TestNewManifest(t *testing.T) {
	dbPath := "/tmp/testdb"
	cfg := NewDefaultConfig(dbPath)

	manifest, err := NewManifest(dbPath, cfg)
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	if manifest.DBPath != dbPath {
		t.Errorf("expected DBPath %s, got %s", dbPath, manifest.DBPath)
	}

	if len(manifest.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(manifest.Entries))
	}

	if manifest.Current == nil {
		t.Error("current entry is nil")
	} else if manifest.Current.Config != cfg {
		t.Error("current config does not match the provided config")
	}
}

func TestManifestUpdateConfig(t *testing.T) {
	dbPath := "/tmp/testdb"
	cfg := NewDefaultConfig(dbPath)

	manifest, err := NewManifest(dbPath, cfg)
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	// Update config
	err = manifest.UpdateConfig(func(c *Config) {
		c.MemTableSize = 64 * 1024 * 1024 // 64MB
		c.MaxMemTables = 8
	})
	if err != nil {
		t.Fatalf("failed to update config: %v", err)
	}

	// Verify entries count
	if len(manifest.Entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(manifest.Entries))
	}

	// Verify updated config
	current := manifest.GetConfig()
	if current.MemTableSize != 64*1024*1024 {
		t.Errorf("expected memtable size %d, got %d", 64*1024*1024, current.MemTableSize)
	}
	if current.MaxMemTables != 8 {
		t.Errorf("expected max memtables %d, got %d", 8, current.MaxMemTables)
	}
}

func TestManifestFileTracking(t *testing.T) {
	dbPath := "/tmp/testdb"
	cfg := NewDefaultConfig(dbPath)

	manifest, err := NewManifest(dbPath, cfg)
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	// Add files
	err = manifest.AddFile("sst/000001.sst", 1)
	if err != nil {
		t.Fatalf("failed to add file: %v", err)
	}

	err = manifest.AddFile("sst/000002.sst", 2)
	if err != nil {
		t.Fatalf("failed to add file: %v", err)
	}

	// Verify files
	files := manifest.GetFiles()
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}

	if files["sst/000001.sst"] != 1 {
		t.Errorf("expected sequence number 1, got %d", files["sst/000001.sst"])
	}

	if files["sst/000002.sst"] != 2 {
		t.Errorf("expected sequence number 2, got %d", files["sst/000002.sst"])
	}

	// Remove file
	err = manifest.RemoveFile("sst/000001.sst")
	if err != nil {
		t.Fatalf("failed to remove file: %v", err)
	}

	// Verify files after removal
	files = manifest.GetFiles()
	if len(files) != 1 {
		t.Errorf("expected 1 file, got %d", len(files))
	}

	if _, exists := files["sst/000001.sst"]; exists {
		t.Error("file should have been removed")
	}
}

func TestManifestSaveLoad(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "manifest_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a manifest
	cfg := NewDefaultConfig(tempDir)
	manifest, err := NewManifest(tempDir, cfg)
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	// Update config
	err = manifest.UpdateConfig(func(c *Config) {
		c.MemTableSize = 64 * 1024 * 1024 // 64MB
	})
	if err != nil {
		t.Fatalf("failed to update config: %v", err)
	}

	// Add some files
	err = manifest.AddFile("sst/000001.sst", 1)
	if err != nil {
		t.Fatalf("failed to add file: %v", err)
	}

	// Save the manifest
	if err := manifest.Save(); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}

	// Load the manifest
	loadedManifest, err := LoadManifest(tempDir)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	// Verify entries count
	if len(loadedManifest.Entries) != len(manifest.Entries) {
		t.Errorf("expected %d entries, got %d", len(manifest.Entries), len(loadedManifest.Entries))
	}

	// Verify config
	loadedConfig := loadedManifest.GetConfig()
	if loadedConfig.MemTableSize != 64*1024*1024 {
		t.Errorf("expected memtable size %d, got %d", 64*1024*1024, loadedConfig.MemTableSize)
	}

	// Verify files
	loadedFiles := loadedManifest.GetFiles()
	if len(loadedFiles) != 1 {
		t.Errorf("expected 1 file, got %d", len(loadedFiles))
	}

	if loadedFiles["sst/000001.sst"] != 1 {
		t.Errorf("expected sequence number 1, got %d", loadedFiles["sst/000001.sst"])
	}
}
