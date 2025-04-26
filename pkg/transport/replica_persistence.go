package transport

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// ErrPersistenceDisabled indicates persistence operations cannot be performed
	ErrPersistenceDisabled = errors.New("persistence is disabled")

	// ErrInvalidReplicaData indicates the stored replica data is invalid
	ErrInvalidReplicaData = errors.New("invalid replica data")
)

// PersistentReplicaInfo contains replica information that can be persisted
type PersistentReplicaInfo struct {
	ID          string              `json:"id"`
	Address     string              `json:"address"`
	Role        string              `json:"role"`
	LastSeen    int64               `json:"last_seen"`
	CurrentLSN  uint64              `json:"current_lsn"`
	Credentials *ReplicaCredentials `json:"credentials,omitempty"`
}

// ReplicaPersistence manages persistence of replica information
type ReplicaPersistence struct {
	mu        sync.RWMutex
	dataDir   string
	enabled   bool
	autoSave  bool
	replicas  map[string]*PersistentReplicaInfo
	dirty     bool
	lastSave  time.Time
	saveTimer *time.Timer
}

// NewReplicaPersistence creates a new persistence manager
func NewReplicaPersistence(dataDir string, enabled bool, autoSave bool) (*ReplicaPersistence, error) {
	rp := &ReplicaPersistence{
		dataDir:  dataDir,
		enabled:  enabled,
		autoSave: autoSave,
		replicas: make(map[string]*PersistentReplicaInfo),
	}

	// Create data directory if it doesn't exist
	if enabled {
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, err
		}

		// Load existing data
		if err := rp.Load(); err != nil {
			return nil, err
		}

		// Start auto-save timer if needed
		if autoSave {
			rp.saveTimer = time.AfterFunc(10*time.Second, rp.autoSaveFunc)
		}
	}

	return rp, nil
}

// autoSaveFunc is called periodically to save replica data
func (rp *ReplicaPersistence) autoSaveFunc() {
	rp.mu.RLock()
	dirty := rp.dirty
	rp.mu.RUnlock()

	if dirty {
		if err := rp.Save(); err != nil {
			// In a production system, this should be logged properly
			println("Error auto-saving replica data:", err.Error())
		}
	}

	// Reschedule timer
	rp.saveTimer.Reset(10 * time.Second)
}

// FromReplicaInfo converts a ReplicaInfo to a persistent form
func (rp *ReplicaPersistence) FromReplicaInfo(info *ReplicaInfo, creds *ReplicaCredentials) *PersistentReplicaInfo {
	return &PersistentReplicaInfo{
		ID:          info.ID,
		Address:     info.Address,
		Role:        string(info.Role),
		LastSeen:    info.LastSeen.UnixMilli(),
		CurrentLSN:  info.CurrentLSN,
		Credentials: creds,
	}
}

// ToReplicaInfo converts from persistent form to ReplicaInfo
func (rp *ReplicaPersistence) ToReplicaInfo(pinfo *PersistentReplicaInfo) *ReplicaInfo {
	info := &ReplicaInfo{
		ID:         pinfo.ID,
		Address:    pinfo.Address,
		Role:       ReplicaRole(pinfo.Role),
		LastSeen:   time.UnixMilli(pinfo.LastSeen),
		CurrentLSN: pinfo.CurrentLSN,
	}
	return info
}

// Save persists all replica information to disk
func (rp *ReplicaPersistence) Save() error {
	if !rp.enabled {
		return ErrPersistenceDisabled
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Nothing to save if no replicas or not dirty
	if len(rp.replicas) == 0 || !rp.dirty {
		return nil
	}

	// Save each replica to its own file for better concurrency
	for id, replica := range rp.replicas {
		filename := filepath.Join(rp.dataDir, "replica_"+id+".json")

		data, err := json.MarshalIndent(replica, "", "  ")
		if err != nil {
			return err
		}

		// Write to temp file first, then rename for atomic update
		tempFile := filename + ".tmp"
		if err := os.WriteFile(tempFile, data, 0644); err != nil {
			return err
		}

		if err := os.Rename(tempFile, filename); err != nil {
			return err
		}
	}

	rp.dirty = false
	rp.lastSave = time.Now()
	return nil
}

// IsEnabled returns whether persistence is enabled
func (rp *ReplicaPersistence) IsEnabled() bool {
	return rp.enabled
}

// Load reads all persisted replica information from disk
func (rp *ReplicaPersistence) Load() error {
	if !rp.enabled {
		return ErrPersistenceDisabled
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Clear existing data
	rp.replicas = make(map[string]*PersistentReplicaInfo)

	// Find all replica files
	pattern := filepath.Join(rp.dataDir, "replica_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Load each file
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			// Skip files with read errors
			continue
		}

		var replica PersistentReplicaInfo
		if err := json.Unmarshal(data, &replica); err != nil {
			// Skip files with parse errors
			continue
		}

		// Validate replica data
		if replica.ID == "" {
			continue
		}

		rp.replicas[replica.ID] = &replica
	}

	rp.dirty = false
	return nil
}

// SaveReplica persists a single replica's information
func (rp *ReplicaPersistence) SaveReplica(info *ReplicaInfo, creds *ReplicaCredentials) error {
	if !rp.enabled {
		return ErrPersistenceDisabled
	}

	if info == nil || info.ID == "" {
		return ErrInvalidReplicaData
	}

	pinfo := rp.FromReplicaInfo(info, creds)

	rp.mu.Lock()
	rp.replicas[info.ID] = pinfo
	rp.dirty = true
	// For immediate save option
	shouldSave := !rp.autoSave
	rp.mu.Unlock()

	// Save immediately if auto-save is disabled
	if shouldSave {
		return rp.Save()
	}

	return nil
}

// LoadReplica loads a single replica's information
func (rp *ReplicaPersistence) LoadReplica(id string) (*ReplicaInfo, *ReplicaCredentials, error) {
	if !rp.enabled {
		return nil, nil, ErrPersistenceDisabled
	}

	if id == "" {
		return nil, nil, ErrInvalidReplicaData
	}

	rp.mu.RLock()
	defer rp.mu.RUnlock()

	pinfo, exists := rp.replicas[id]
	if !exists {
		return nil, nil, nil // Not found but not an error
	}

	return rp.ToReplicaInfo(pinfo), pinfo.Credentials, nil
}

// DeleteReplica removes a replica's persisted information
func (rp *ReplicaPersistence) DeleteReplica(id string) error {
	if !rp.enabled {
		return ErrPersistenceDisabled
	}

	if id == "" {
		return ErrInvalidReplicaData
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Remove from memory
	delete(rp.replicas, id)
	rp.dirty = true

	// Remove file
	filename := filepath.Join(rp.dataDir, "replica_"+id+".json")
	err := os.Remove(filename)
	// Ignore file not found errors
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// GetAllReplicas returns all persisted replicas
func (rp *ReplicaPersistence) GetAllReplicas() (map[string]*ReplicaInfo, map[string]*ReplicaCredentials, error) {
	if !rp.enabled {
		return nil, nil, ErrPersistenceDisabled
	}

	rp.mu.RLock()
	defer rp.mu.RUnlock()

	infoMap := make(map[string]*ReplicaInfo, len(rp.replicas))
	credsMap := make(map[string]*ReplicaCredentials, len(rp.replicas))

	for id, pinfo := range rp.replicas {
		infoMap[id] = rp.ToReplicaInfo(pinfo)
		credsMap[id] = pinfo.Credentials
	}

	return infoMap, credsMap, nil
}

// Close shuts down the persistence manager
func (rp *ReplicaPersistence) Close() error {
	if !rp.enabled {
		return nil
	}

	// Stop auto-save timer
	if rp.autoSave && rp.saveTimer != nil {
		rp.saveTimer.Stop()
	}

	// Save any pending changes
	return rp.Save()
}
