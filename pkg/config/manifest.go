package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ManifestEntry struct {
	Timestamp  int64            `json:"timestamp"`
	Version    int              `json:"version"`
	Config     *Config          `json:"config"`
	FileSystem map[string]int64 `json:"filesystem,omitempty"` // Map of file paths to sequence numbers
}

type Manifest struct {
	DBPath     string
	Entries    []ManifestEntry
	Current    *ManifestEntry
	LastUpdate time.Time
	mu         sync.RWMutex
}

// NewManifest creates a new manifest for the given database path
func NewManifest(dbPath string, config *Config) (*Manifest, error) {
	if config == nil {
		config = NewDefaultConfig(dbPath)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	entry := ManifestEntry{
		Timestamp: time.Now().Unix(),
		Version:   CurrentManifestVersion,
		Config:    config,
	}

	m := &Manifest{
		DBPath:     dbPath,
		Entries:    []ManifestEntry{entry},
		Current:    &entry,
		LastUpdate: time.Now(),
	}

	return m, nil
}

// LoadManifest loads an existing manifest from the database directory
func LoadManifest(dbPath string) (*Manifest, error) {
	manifestPath := filepath.Join(dbPath, DefaultManifestFileName)
	file, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrManifestNotFound
		}
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var entries []ManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: no entries in manifest", ErrInvalidManifest)
	}

	current := &entries[len(entries)-1]
	if err := current.Config.Validate(); err != nil {
		return nil, err
	}

	m := &Manifest{
		DBPath:     dbPath,
		Entries:    entries,
		Current:    current,
		LastUpdate: time.Now(),
	}

	return m, nil
}

// Save persists the manifest to disk
func (m *Manifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.Current.Config.Validate(); err != nil {
		return err
	}

	if err := os.MkdirAll(m.DBPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	manifestPath := filepath.Join(m.DBPath, DefaultManifestFileName)
	tempPath := manifestPath + ".tmp"

	data, err := json.MarshalIndent(m.Entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	if err := os.Rename(tempPath, manifestPath); err != nil {
		return fmt.Errorf("failed to rename manifest: %w", err)
	}

	m.LastUpdate = time.Now()
	return nil
}

// UpdateConfig creates a new configuration entry
func (m *Manifest) UpdateConfig(fn func(*Config)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create a copy of the current config
	currentJSON, err := json.Marshal(m.Current.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal current config: %w", err)
	}

	var newConfig Config
	if err := json.Unmarshal(currentJSON, &newConfig); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Apply the update function
	fn(&newConfig)

	// Validate the new config
	if err := newConfig.Validate(); err != nil {
		return err
	}

	// Create a new entry
	entry := ManifestEntry{
		Timestamp: time.Now().Unix(),
		Version:   CurrentManifestVersion,
		Config:    &newConfig,
	}

	m.Entries = append(m.Entries, entry)
	m.Current = &m.Entries[len(m.Entries)-1]

	return nil
}

// AddFile registers a file in the manifest
func (m *Manifest) AddFile(path string, seqNum int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Current.FileSystem == nil {
		m.Current.FileSystem = make(map[string]int64)
	}

	m.Current.FileSystem[path] = seqNum
	return nil
}

// RemoveFile removes a file from the manifest
func (m *Manifest) RemoveFile(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Current.FileSystem == nil {
		return nil
	}

	delete(m.Current.FileSystem, path)
	return nil
}

// GetConfig returns the current configuration
func (m *Manifest) GetConfig() *Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Current.Config
}

// GetFiles returns all files registered in the manifest
func (m *Manifest) GetFiles() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.Current.FileSystem == nil {
		return make(map[string]int64)
	}

	// Return a copy to prevent concurrent map access
	files := make(map[string]int64, len(m.Current.FileSystem))
	for k, v := range m.Current.FileSystem {
		files[k] = v
	}

	return files
}
