package memtable

import (
	"testing"
	"time"

	"github.com/jeremytregunna/kevo/pkg/config"
)

func createTestConfig() *config.Config {
	cfg := config.NewDefaultConfig("/tmp/db")
	cfg.MemTableSize = 1024 // Small size for testing
	cfg.MaxMemTableAge = 1  // 1 second
	cfg.MaxMemTables = 4    // Allow up to 4 memtables
	cfg.MemTablePoolCap = 4 // Pool capacity
	return cfg
}

func TestMemPoolBasicOperations(t *testing.T) {
	cfg := createTestConfig()
	pool := NewMemTablePool(cfg)

	// Test Put and Get
	pool.Put([]byte("key1"), []byte("value1"), 1)

	value, found := pool.Get([]byte("key1"))
	if !found {
		t.Fatalf("expected to find key1, but got not found")
	}
	if string(value) != "value1" {
		t.Errorf("expected value1, got %s", string(value))
	}

	// Test Delete
	pool.Delete([]byte("key1"), 2)

	value, found = pool.Get([]byte("key1"))
	if !found {
		t.Fatalf("expected tombstone to be found for key1")
	}
	if value != nil {
		t.Errorf("expected nil value for deleted key, got %v", value)
	}
}

func TestMemPoolSwitchMemTable(t *testing.T) {
	cfg := createTestConfig()
	pool := NewMemTablePool(cfg)

	// Add data to the active memtable
	pool.Put([]byte("key1"), []byte("value1"), 1)

	// Switch to a new memtable
	old := pool.SwitchToNewMemTable()
	if !old.IsImmutable() {
		t.Errorf("expected switched memtable to be immutable")
	}

	// Verify the data is in the old table
	value, found := old.Get([]byte("key1"))
	if !found {
		t.Fatalf("expected to find key1 in old table, but got not found")
	}
	if string(value) != "value1" {
		t.Errorf("expected value1 in old table, got %s", string(value))
	}

	// Verify the immutable count is correct
	if count := pool.ImmutableCount(); count != 1 {
		t.Errorf("expected immutable count to be 1, got %d", count)
	}

	// Add data to the new active memtable
	pool.Put([]byte("key2"), []byte("value2"), 2)

	// Verify we can still retrieve data from both tables
	value, found = pool.Get([]byte("key1"))
	if !found {
		t.Fatalf("expected to find key1 through pool, but got not found")
	}
	if string(value) != "value1" {
		t.Errorf("expected value1 through pool, got %s", string(value))
	}

	value, found = pool.Get([]byte("key2"))
	if !found {
		t.Fatalf("expected to find key2 through pool, but got not found")
	}
	if string(value) != "value2" {
		t.Errorf("expected value2 through pool, got %s", string(value))
	}
}

func TestMemPoolFlushConditions(t *testing.T) {
	// Create a config with small thresholds for testing
	cfg := createTestConfig()
	cfg.MemTableSize = 100 // Very small size to trigger flush
	pool := NewMemTablePool(cfg)

	// Initially no flush should be needed
	if pool.IsFlushNeeded() {
		t.Errorf("expected no flush needed initially")
	}

	// Add enough data to trigger a size-based flush
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		value := make([]byte, 20) // 20 bytes per value
		pool.Put(key, value, uint64(i+1))
	}

	// Should trigger a flush
	if !pool.IsFlushNeeded() {
		t.Errorf("expected flush needed after reaching size threshold")
	}

	// Switch to a new memtable
	old := pool.SwitchToNewMemTable()
	if !old.IsImmutable() {
		t.Errorf("expected old memtable to be immutable")
	}

	// The flush pending flag should be reset
	if pool.IsFlushNeeded() {
		t.Errorf("expected flush pending to be reset after switch")
	}

	// Now test age-based flushing
	// Wait for the age threshold to trigger
	time.Sleep(1200 * time.Millisecond) // Just over 1 second

	// Add a small amount of data to check conditions
	pool.Put([]byte("trigger"), []byte("check"), 100)

	// Should trigger an age-based flush
	if !pool.IsFlushNeeded() {
		t.Errorf("expected flush needed after reaching age threshold")
	}
}

func TestMemPoolGetImmutablesForFlush(t *testing.T) {
	cfg := createTestConfig()
	pool := NewMemTablePool(cfg)

	// Switch memtables a few times to accumulate immutables
	for i := 0; i < 3; i++ {
		pool.Put([]byte{byte(i)}, []byte{byte(i)}, uint64(i+1))
		pool.SwitchToNewMemTable()
	}

	// Should have 3 immutable memtables
	if count := pool.ImmutableCount(); count != 3 {
		t.Errorf("expected 3 immutable memtables, got %d", count)
	}

	// Get immutables for flush
	immutables := pool.GetImmutablesForFlush()

	// Should get all 3 immutables
	if len(immutables) != 3 {
		t.Errorf("expected to get 3 immutables for flush, got %d", len(immutables))
	}

	// The pool should now have 0 immutables
	if count := pool.ImmutableCount(); count != 0 {
		t.Errorf("expected 0 immutable memtables after flush, got %d", count)
	}
}

func TestMemPoolGetMemTables(t *testing.T) {
	cfg := createTestConfig()
	pool := NewMemTablePool(cfg)

	// Initially should have just the active memtable
	tables := pool.GetMemTables()
	if len(tables) != 1 {
		t.Errorf("expected 1 memtable initially, got %d", len(tables))
	}

	// Add an immutable table
	pool.Put([]byte("key"), []byte("value"), 1)
	pool.SwitchToNewMemTable()

	// Now should have 2 memtables (active + 1 immutable)
	tables = pool.GetMemTables()
	if len(tables) != 2 {
		t.Errorf("expected 2 memtables after switch, got %d", len(tables))
	}

	// The active table should be first
	if tables[0].IsImmutable() {
		t.Errorf("expected first table to be active (not immutable)")
	}

	// The second table should be immutable
	if !tables[1].IsImmutable() {
		t.Errorf("expected second table to be immutable")
	}
}

func TestMemPoolGetNextSequenceNumber(t *testing.T) {
	cfg := createTestConfig()
	pool := NewMemTablePool(cfg)

	// Initial sequence number should be 0
	if seq := pool.GetNextSequenceNumber(); seq != 0 {
		t.Errorf("expected initial sequence number to be 0, got %d", seq)
	}

	// Add entries with sequence numbers
	pool.Put([]byte("key"), []byte("value"), 5)

	// Next sequence number should be 6
	if seq := pool.GetNextSequenceNumber(); seq != 6 {
		t.Errorf("expected sequence number to be 6, got %d", seq)
	}

	// Switch to a new memtable
	pool.SwitchToNewMemTable()

	// Sequence number should reset for the new table
	if seq := pool.GetNextSequenceNumber(); seq != 0 {
		t.Errorf("expected sequence number to reset to 0, got %d", seq)
	}
}
