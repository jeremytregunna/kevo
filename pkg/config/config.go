package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/KevoDB/kevo/pkg/telemetry"
)

const (
	DefaultManifestFileName = "MANIFEST"
	CurrentManifestVersion  = 1
)

var (
	ErrInvalidConfig    = errors.New("invalid configuration")
	ErrManifestNotFound = errors.New("manifest not found")
	ErrInvalidManifest  = errors.New("invalid manifest")
)

type SyncMode int

const (
	SyncNone SyncMode = iota
	SyncBatch
	SyncImmediate
)

type Config struct {
	Version int `json:"version"`

	// WAL configuration
	WALDir       string   `json:"wal_dir"`
	WALSyncMode  SyncMode `json:"wal_sync_mode"`
	WALSyncBytes int64    `json:"wal_sync_bytes"`
	WALMaxSize   int64    `json:"wal_max_size"`

	// MemTable configuration
	MemTableSize    int64 `json:"memtable_size"`
	MaxMemTables    int   `json:"max_memtables"`
	MaxMemTableAge  int64 `json:"max_memtable_age"`
	MemTablePoolCap int   `json:"memtable_pool_cap"`

	// SSTable configuration
	SSTDir             string `json:"sst_dir"`
	SSTableBlockSize   int    `json:"sstable_block_size"`
	SSTableIndexSize   int    `json:"sstable_index_size"`
	SSTableMaxSize     int64  `json:"sstable_max_size"`
	SSTableRestartSize int    `json:"sstable_restart_size"`

	// Compaction configuration
	CompactionLevels       int     `json:"compaction_levels"`
	CompactionRatio        float64 `json:"compaction_ratio"`
	CompactionThreads      int     `json:"compaction_threads"`
	CompactionInterval     int64   `json:"compaction_interval"`
	MaxLevelWithTombstones int     `json:"max_level_with_tombstones"` // Levels higher than this discard tombstones

	// Transaction configuration
	ReadOnlyTxTTL       int64 `json:"read_only_tx_ttl"`      // Time-to-live for read-only transactions in seconds (default: 180s)
	ReadWriteTxTTL      int64 `json:"read_write_tx_ttl"`     // Time-to-live for read-write transactions in seconds (default: 60s)
	IdleTxTimeout       int64 `json:"idle_tx_timeout"`       // Time after which an inactive transaction is considered idle in seconds (default: 30s)
	TxCleanupInterval   int64 `json:"tx_cleanup_interval"`   // Interval for checking transaction TTLs in seconds (default: 30s)
	TxWarningThreshold  int   `json:"tx_warning_threshold"`  // Percentage of TTL after which to log warnings (default: 75)
	TxCriticalThreshold int   `json:"tx_critical_threshold"` // Percentage of TTL after which to log critical warnings (default: 90)

	// Telemetry configuration
	Telemetry telemetry.Config `json:"telemetry"`

	mu sync.RWMutex
}

// NewDefaultConfig creates a Config with recommended default values
func NewDefaultConfig(dbPath string) *Config {
	walDir := filepath.Join(dbPath, "wal")
	sstDir := filepath.Join(dbPath, "sst")

	return &Config{
		Version: CurrentManifestVersion,

		// WAL defaults
		WALDir:       walDir,
		WALSyncMode:  SyncImmediate,
		WALSyncBytes: 1024 * 1024, // 1MB

		// MemTable defaults
		MemTableSize:    32 * 1024 * 1024, // 32MB
		MaxMemTables:    4,
		MaxMemTableAge:  600, // 10 minutes
		MemTablePoolCap: 4,

		// SSTable defaults
		SSTDir:             sstDir,
		SSTableBlockSize:   16 * 1024,        // 16KB
		SSTableIndexSize:   64 * 1024,        // 64KB
		SSTableMaxSize:     64 * 1024 * 1024, // 64MB
		SSTableRestartSize: 16,               // Restart points every 16 keys

		// Compaction defaults
		CompactionLevels:       7,
		CompactionRatio:        10,
		CompactionThreads:      2,
		CompactionInterval:     30, // 30 seconds
		MaxLevelWithTombstones: 1,  // Keep tombstones in levels 0 and 1

		// Transaction defaults
		ReadOnlyTxTTL:       180, // 3 minutes
		ReadWriteTxTTL:      60,  // 1 minute
		IdleTxTimeout:       30,  // 30 seconds
		TxCleanupInterval:   30,  // 30 seconds
		TxWarningThreshold:  75,  // 75% of TTL
		TxCriticalThreshold: 90,  // 90% of TTL

		// Telemetry defaults
		Telemetry: telemetry.DefaultConfig(),
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Version <= 0 {
		return fmt.Errorf("%w: invalid version %d", ErrInvalidConfig, c.Version)
	}

	if c.WALDir == "" {
		return fmt.Errorf("%w: WAL directory not specified", ErrInvalidConfig)
	}

	if c.SSTDir == "" {
		return fmt.Errorf("%w: SSTable directory not specified", ErrInvalidConfig)
	}

	if c.MemTableSize <= 0 {
		return fmt.Errorf("%w: MemTable size must be positive", ErrInvalidConfig)
	}

	if c.MaxMemTables <= 0 {
		return fmt.Errorf("%w: Max MemTables must be positive", ErrInvalidConfig)
	}

	if c.SSTableBlockSize <= 0 {
		return fmt.Errorf("%w: SSTable block size must be positive", ErrInvalidConfig)
	}

	if c.SSTableIndexSize <= 0 {
		return fmt.Errorf("%w: SSTable index size must be positive", ErrInvalidConfig)
	}

	if c.CompactionLevels <= 0 {
		return fmt.Errorf("%w: Compaction levels must be positive", ErrInvalidConfig)
	}

	if c.CompactionRatio <= 1.0 {
		return fmt.Errorf("%w: Compaction ratio must be greater than 1.0", ErrInvalidConfig)
	}

	// Validate Transaction settings
	if c.ReadOnlyTxTTL <= 0 {
		return fmt.Errorf("%w: Read-only transaction TTL must be positive", ErrInvalidConfig)
	}

	if c.ReadWriteTxTTL <= 0 {
		return fmt.Errorf("%w: Read-write transaction TTL must be positive", ErrInvalidConfig)
	}

	if c.IdleTxTimeout <= 0 {
		return fmt.Errorf("%w: Idle transaction timeout must be positive", ErrInvalidConfig)
	}

	if c.TxCleanupInterval <= 0 {
		return fmt.Errorf("%w: Transaction cleanup interval must be positive", ErrInvalidConfig)
	}

	if c.TxWarningThreshold <= 0 || c.TxWarningThreshold >= 100 {
		return fmt.Errorf("%w: Transaction warning threshold must be between 1 and 99", ErrInvalidConfig)
	}

	if c.TxCriticalThreshold <= c.TxWarningThreshold || c.TxCriticalThreshold >= 100 {
		return fmt.Errorf("%w: Transaction critical threshold must be between warning threshold and 99", ErrInvalidConfig)
	}

	// Validate telemetry configuration
	if err := c.Telemetry.Validate(); err != nil {
		return fmt.Errorf("%w: telemetry configuration invalid: %v", ErrInvalidConfig, err)
	}

	return nil
}

// LoadConfigFromManifest loads just the configuration portion from the manifest file
func LoadConfigFromManifest(dbPath string) (*Config, error) {
	manifestPath := filepath.Join(dbPath, DefaultManifestFileName)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrManifestNotFound
		}
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// SaveManifest saves the configuration to the manifest file
func (c *Config) SaveManifest(dbPath string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.Validate(); err != nil {
		return err
	}

	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	manifestPath := filepath.Join(dbPath, DefaultManifestFileName)
	tempPath := manifestPath + ".tmp"

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	if err := os.Rename(tempPath, manifestPath); err != nil {
		return fmt.Errorf("failed to rename manifest: %w", err)
	}

	return nil
}

// LoadTelemetryFromEnv loads telemetry configuration from environment variables
func (c *Config) LoadTelemetryFromEnv() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Telemetry.LoadFromEnv()
}

// Update applies the given function to modify the configuration
func (c *Config) Update(fn func(*Config)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c)
}
