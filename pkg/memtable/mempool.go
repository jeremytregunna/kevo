package memtable

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
)

// MemTablePool manages a pool of MemTables
// It maintains one active MemTable and a set of immutable MemTables
type MemTablePool struct {
	cfg          *config.Config
	active       *MemTable
	immutables   []*MemTable
	maxAge       time.Duration
	maxSize      int64
	totalSize    int64
	flushPending atomic.Bool
	metrics      MemTableMetrics
	mu           sync.RWMutex
}

// NewMemTablePool creates a new MemTable pool
func NewMemTablePool(cfg *config.Config) *MemTablePool {
	return &MemTablePool{
		cfg:        cfg,
		active:     NewMemTable(),
		immutables: make([]*MemTable, 0, cfg.MaxMemTables-1),
		maxAge:     time.Duration(cfg.MaxMemTableAge) * time.Second,
		maxSize:    cfg.MemTableSize,
		metrics:    NewNoopMemTableMetrics(), // Default to no-op, will be replaced by SetTelemetry
	}
}

// Put adds a key-value pair to the active MemTable
func (p *MemTablePool) Put(key, value []byte, seqNum uint64) {
	start := time.Now()
	ctx := context.Background()

	p.mu.RLock()
	p.active.Put(key, value, seqNum)
	p.mu.RUnlock()

	// Record operation metrics
	if p.metrics != nil {
		p.metrics.RecordOperation(ctx, "put", time.Since(start))
	}

	// Check if we need to flush after this write
	p.checkFlushConditions()
}

// Delete marks a key as deleted in the active MemTable
func (p *MemTablePool) Delete(key []byte, seqNum uint64) {
	start := time.Now()
	ctx := context.Background()

	p.mu.RLock()
	p.active.Delete(key, seqNum)
	p.mu.RUnlock()

	// Record operation metrics
	if p.metrics != nil {
		p.metrics.RecordOperation(ctx, "delete", time.Since(start))
	}

	// Check if we need to flush after this write
	p.checkFlushConditions()
}

// Get retrieves the value for a key from all MemTables
// Checks the active MemTable first, then the immutables in reverse order
func (p *MemTablePool) Get(key []byte) ([]byte, bool) {
	start := time.Now()
	ctx := context.Background()

	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check active table first
	if value, found := p.active.Get(key); found {
		// Record operation metrics
		if p.metrics != nil {
			p.metrics.RecordOperation(ctx, "get", time.Since(start))
		}
		return value, true
	}

	// Check immutable tables in reverse order (newest first)
	for i := len(p.immutables) - 1; i >= 0; i-- {
		if value, found := p.immutables[i].Get(key); found {
			// Record operation metrics
			if p.metrics != nil {
				p.metrics.RecordOperation(ctx, "get", time.Since(start))
			}
			return value, true
		}
	}

	// Record operation metrics for miss
	if p.metrics != nil {
		p.metrics.RecordOperation(ctx, "get", time.Since(start))
	}

	return nil, false
}

// ImmutableCount returns the number of immutable MemTables
func (p *MemTablePool) ImmutableCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.immutables)
}

// checkFlushConditions checks if we need to flush the active MemTable
func (p *MemTablePool) checkFlushConditions() {
	needsFlush := false

	p.mu.RLock()
	defer p.mu.RUnlock()

	// Skip if a flush is already pending
	if p.flushPending.Load() {
		return
	}

	// Check size condition
	sizeTriggered := p.active.ApproximateSize() >= p.maxSize
	if sizeTriggered {
		needsFlush = true
	}

	// Check age condition
	ageTriggered := p.maxAge > 0 && p.active.Age() > p.maxAge.Seconds()
	if ageTriggered {
		needsFlush = true
	}

	// Mark as needing flush if conditions met
	if needsFlush {
		p.flushPending.Store(true)

		// Record flush trigger metrics
		if p.metrics != nil {
			ctx := context.Background()
			reasonName := getFlushReasonName(sizeTriggered, ageTriggered, false)
			p.metrics.RecordFlushTrigger(ctx, reasonName, p.active.ApproximateSize(), p.active.Age())
		}
	}
}

// SwitchToNewMemTable makes the active MemTable immutable and creates a new active one
// Returns the immutable MemTable that needs to be flushed
func (p *MemTablePool) SwitchToNewMemTable() *MemTable {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Reset the flush pending flag
	p.flushPending.Store(false)

	// Make the current active table immutable
	oldActive := p.active
	oldActive.SetImmutable()

	// Create a new active table
	p.active = NewMemTable()

	// Add the old table to the immutables list
	p.immutables = append(p.immutables, oldActive)

	// Record pool state metrics (calculate total size manually to avoid deadlock)
	if p.metrics != nil {
		ctx := context.Background()
		activeSize := p.active.ApproximateSize()
		immutableCount := len(p.immutables)

		// Calculate total size manually to avoid calling TotalSize() while holding lock
		totalSize := activeSize
		for _, m := range p.immutables {
			totalSize += m.ApproximateSize()
		}

		p.metrics.RecordPoolState(ctx, activeSize, immutableCount, totalSize)
	}

	// Return the table that needs to be flushed
	return oldActive
}

// GetImmutablesForFlush returns a list of immutable MemTables ready for flushing
// and removes them from the pool
func (p *MemTablePool) GetImmutablesForFlush() []*MemTable {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := p.immutables
	p.immutables = make([]*MemTable, 0, p.cfg.MaxMemTables-1)
	return result
}

// IsFlushNeeded returns true if a flush is needed
func (p *MemTablePool) IsFlushNeeded() bool {
	return p.flushPending.Load()
}

// GetNextSequenceNumber returns the next sequence number to use
func (p *MemTablePool) GetNextSequenceNumber() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active.GetNextSequenceNumber()
}

// GetMemTables returns all MemTables (active and immutable)
func (p *MemTablePool) GetMemTables() []*MemTable {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*MemTable, 0, len(p.immutables)+1)
	result = append(result, p.active)
	result = append(result, p.immutables...)
	return result
}

// TotalSize returns the total approximate size of all memtables in the pool
func (p *MemTablePool) TotalSize() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var total int64
	total += p.active.ApproximateSize()

	for _, m := range p.immutables {
		total += m.ApproximateSize()
	}

	return total
}

// SetTelemetry allows post-creation telemetry injection from engine facade
func (p *MemTablePool) SetTelemetry(tel interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if memTableTel, ok := tel.(MemTableMetrics); ok {
		p.metrics = memTableTel
	}
}

// SetActiveMemTable sets the active memtable (used for recovery)
func (p *MemTablePool) SetActiveMemTable(memTable *MemTable) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If there's already an active memtable, make it immutable
	if p.active != nil && p.active.ApproximateSize() > 0 {
		p.active.SetImmutable()
		p.immutables = append(p.immutables, p.active)
	}

	// Set the provided memtable as active
	p.active = memTable
}
