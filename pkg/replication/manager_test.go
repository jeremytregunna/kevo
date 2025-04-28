package replication

import (
	"testing"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/wal"
)

// MockEngine implements a minimal mock engine for testing
type MockEngine struct {
	wal      *wal.WAL
	readOnly bool
}

// Implement only essential methods for the test
func (m *MockEngine) GetWAL() *wal.WAL {
	return m.wal
}

func (m *MockEngine) SetReadOnly(readOnly bool) {
	m.readOnly = readOnly
}

func (m *MockEngine) FlushImMemTables() error {
	return nil
}

// Implement required interface methods with minimal stubs
func (m *MockEngine) Put(key, value []byte) error {
	return nil
}

func (m *MockEngine) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (m *MockEngine) Delete(key []byte) error {
	return nil
}

func (m *MockEngine) IsDeleted(key []byte) (bool, error) {
	return false, nil
}

func (m *MockEngine) GetIterator() (iterator.Iterator, error) {
	return nil, nil
}

func (m *MockEngine) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	return nil, nil
}

func (m *MockEngine) ApplyBatch(entries []*wal.Entry) error {
	return nil
}

func (m *MockEngine) BeginTransaction(readOnly bool) (interfaces.Transaction, error) {
	return nil, nil
}

func (m *MockEngine) TriggerCompaction() error {
	return nil
}

func (m *MockEngine) CompactRange(startKey, endKey []byte) error {
	return nil
}

func (m *MockEngine) GetStats() map[string]interface{} {
	return map[string]interface{}{}
}

func (m *MockEngine) GetCompactionStats() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func (m *MockEngine) Close() error {
	return nil
}

// TestNewManager tests the creation of a new replication manager
func TestNewManager(t *testing.T) {
	engine := &MockEngine{}

	// Test with nil config
	manager, err := NewManager(engine, nil)
	if err != nil {
		t.Fatalf("Expected no error when creating manager with nil config, got: %v", err)
	}
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}
	if manager.config.Enabled {
		t.Error("Expected Enabled to be false")
	}
	if manager.config.Mode != "standalone" {
		t.Errorf("Expected Mode to be 'standalone', got '%s'", manager.config.Mode)
	}

	// Test with custom config
	config := &ManagerConfig{
		Enabled:     true,
		Mode:        "primary",
		ListenAddr:  ":50053",
		PrimaryAddr: "localhost:50053",
	}
	manager, err = NewManager(engine, config)
	if err != nil {
		t.Fatalf("Expected no error when creating manager with custom config, got: %v", err)
	}
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}
	if !manager.config.Enabled {
		t.Error("Expected Enabled to be true")
	}
	if manager.config.Mode != "primary" {
		t.Errorf("Expected Mode to be 'primary', got '%s'", manager.config.Mode)
	}
}

// TestManagerStartStandalone tests starting the manager in standalone mode
func TestManagerStartStandalone(t *testing.T) {
	engine := &MockEngine{}

	config := &ManagerConfig{
		Enabled: true,
		Mode:    "standalone",
	}

	manager, err := NewManager(engine, config)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	err = manager.Start()
	if err != nil {
		t.Errorf("Expected no error when starting in standalone mode, got: %v", err)
	}
	if manager.serviceStatus {
		t.Error("Expected serviceStatus to be false")
	}

	err = manager.Stop()
	if err != nil {
		t.Errorf("Expected no error when stopping, got: %v", err)
	}
}

// TestManagerStatus tests the status reporting functionality
func TestManagerStatus(t *testing.T) {
	engine := &MockEngine{}

	// Test disabled mode
	config := &ManagerConfig{
		Enabled: false,
		Mode:    "standalone",
	}

	manager, _ := NewManager(engine, config)
	status := manager.Status()

	if status["enabled"].(bool) != false {
		t.Error("Expected 'enabled' to be false")
	}
	if status["mode"].(string) != "standalone" {
		t.Errorf("Expected 'mode' to be 'standalone', got '%s'", status["mode"].(string))
	}
	if status["active"].(bool) != false {
		t.Error("Expected 'active' to be false")
	}

	// Test primary mode
	config = &ManagerConfig{
		Enabled:    true,
		Mode:       "primary",
		ListenAddr: ":50057",
	}

	manager, _ = NewManager(engine, config)
	manager.serviceStatus = true
	status = manager.Status()

	if status["enabled"].(bool) != true {
		t.Error("Expected 'enabled' to be true")
	}
	if status["mode"].(string) != "primary" {
		t.Errorf("Expected 'mode' to be 'primary', got '%s'", status["mode"].(string))
	}
	if status["active"].(bool) != true {
		t.Error("Expected 'active' to be true")
	}

	// There will be no listen_address in the status until the primary is actually created
	// so we skip checking that field
}

// TestEngineApplier tests the engine applier implementation
func TestEngineApplier(t *testing.T) {
	engine := &MockEngine{}

	applier := NewEngineApplier(engine)

	// Test Put
	entry := &wal.Entry{
		Type:  wal.OpTypePut,
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	err := applier.Apply(entry)
	if err != nil {
		t.Errorf("Expected no error for Put, got: %v", err)
	}

	// Test Delete
	entry = &wal.Entry{
		Type: wal.OpTypeDelete,
		Key:  []byte("test-key"),
	}
	err = applier.Apply(entry)
	if err != nil {
		t.Errorf("Expected no error for Delete, got: %v", err)
	}

	// Test Batch
	entry = &wal.Entry{
		Type: wal.OpTypeBatch,
		Key:  []byte("test-key"),
	}
	err = applier.Apply(entry)
	if err != nil {
		t.Errorf("Expected no error for Batch, got: %v", err)
	}

	// Test unsupported type
	entry = &wal.Entry{
		Type: 99, // Invalid type
		Key:  []byte("test-key"),
	}
	err = applier.Apply(entry)
	if err == nil {
		t.Error("Expected error for unsupported entry type")
	}
}
