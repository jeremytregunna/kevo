package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewDefaultConfig(t *testing.T) {
	dbPath := "/tmp/testdb"
	cfg := NewDefaultConfig(dbPath)

	if cfg.Version != CurrentManifestVersion {
		t.Errorf("expected version %d, got %d", CurrentManifestVersion, cfg.Version)
	}

	if cfg.WALDir != filepath.Join(dbPath, "wal") {
		t.Errorf("expected WAL dir %s, got %s", filepath.Join(dbPath, "wal"), cfg.WALDir)
	}

	if cfg.SSTDir != filepath.Join(dbPath, "sst") {
		t.Errorf("expected SST dir %s, got %s", filepath.Join(dbPath, "sst"), cfg.SSTDir)
	}

	// Test default values
	if cfg.WALSyncMode != SyncBatch {
		t.Errorf("expected WAL sync mode %d, got %d", SyncBatch, cfg.WALSyncMode)
	}

	if cfg.MemTableSize != 32*1024*1024 {
		t.Errorf("expected memtable size %d, got %d", 32*1024*1024, cfg.MemTableSize)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := NewDefaultConfig("/tmp/testdb")

	// Valid config
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid config, got error: %v", err)
	}

	// Test invalid configs
	testCases := []struct {
		name     string
		mutate   func(*Config)
		expected string
	}{
		{
			name: "invalid version",
			mutate: func(c *Config) {
				c.Version = 0
			},
			expected: "invalid configuration: invalid version 0",
		},
		{
			name: "empty WAL dir",
			mutate: func(c *Config) {
				c.WALDir = ""
			},
			expected: "invalid configuration: WAL directory not specified",
		},
		{
			name: "empty SST dir",
			mutate: func(c *Config) {
				c.SSTDir = ""
			},
			expected: "invalid configuration: SSTable directory not specified",
		},
		{
			name: "zero memtable size",
			mutate: func(c *Config) {
				c.MemTableSize = 0
			},
			expected: "invalid configuration: MemTable size must be positive",
		},
		{
			name: "negative max memtables",
			mutate: func(c *Config) {
				c.MaxMemTables = -1
			},
			expected: "invalid configuration: Max MemTables must be positive",
		},
		{
			name: "zero block size",
			mutate: func(c *Config) {
				c.SSTableBlockSize = 0
			},
			expected: "invalid configuration: SSTable block size must be positive",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewDefaultConfig("/tmp/testdb")
			tc.mutate(cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if err.Error() != tc.expected {
				t.Errorf("expected error %q, got %q", tc.expected, err.Error())
			}
		})
	}
}

func TestConfigManifestSaveLoad(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a config and save it
	cfg := NewDefaultConfig(tempDir)
	cfg.MemTableSize = 16 * 1024 * 1024 // 16MB
	cfg.CompactionThreads = 4

	if err := cfg.SaveManifest(tempDir); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}

	// Load the config
	loadedCfg, err := LoadConfigFromManifest(tempDir)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	// Verify loaded config
	if loadedCfg.MemTableSize != cfg.MemTableSize {
		t.Errorf("expected memtable size %d, got %d", cfg.MemTableSize, loadedCfg.MemTableSize)
	}

	if loadedCfg.CompactionThreads != cfg.CompactionThreads {
		t.Errorf("expected compaction threads %d, got %d", cfg.CompactionThreads, loadedCfg.CompactionThreads)
	}

	// Test loading non-existent manifest
	nonExistentDir := filepath.Join(tempDir, "nonexistent")
	_, err = LoadConfigFromManifest(nonExistentDir)
	if err != ErrManifestNotFound {
		t.Errorf("expected ErrManifestNotFound, got %v", err)
	}
}

func TestConfigUpdate(t *testing.T) {
	cfg := NewDefaultConfig("/tmp/testdb")

	// Update config
	cfg.Update(func(c *Config) {
		c.MemTableSize = 64 * 1024 * 1024 // 64MB
		c.MaxMemTables = 8
	})

	// Verify update
	if cfg.MemTableSize != 64*1024*1024 {
		t.Errorf("expected memtable size %d, got %d", 64*1024*1024, cfg.MemTableSize)
	}

	if cfg.MaxMemTables != 8 {
		t.Errorf("expected max memtables %d, got %d", 8, cfg.MaxMemTables)
	}
}
