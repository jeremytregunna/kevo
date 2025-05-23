package iterator

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/bounded"
	"github.com/KevoDB/kevo/pkg/common/iterator/composite"
	iteratorpkg "github.com/KevoDB/kevo/pkg/iterator"
	"github.com/KevoDB/kevo/pkg/memtable"
	"github.com/KevoDB/kevo/pkg/sstable"
)

// Factory provides methods to create iterators for the storage engine
type Factory struct {
	// Telemetry metrics for iterator operations (optional)
	metrics iteratorpkg.IteratorMetrics
}

// NewFactory creates a new iterator factory
func NewFactory() *Factory {
	return &Factory{
		metrics: iteratorpkg.NewNoopIteratorMetrics(), // Default to no-op, will be replaced by SetTelemetry
	}
}

// SetTelemetry allows post-creation telemetry injection from engine facade
func (f *Factory) SetTelemetry(tel interface{}) {
	if iterMetrics, ok := tel.(iteratorpkg.IteratorMetrics); ok {
		f.metrics = iterMetrics
	}
}

// CreateIterator creates a hierarchical iterator that combines
// memtables and sstables in the correct priority order
func (f *Factory) CreateIterator(
	memTables []*memtable.MemTable,
	ssTables []*sstable.Reader,
) iterator.Iterator {
	return f.createBaseIterator(memTables, ssTables)
}

// CreateRangeIterator creates an iterator limited to a specific key range
func (f *Factory) CreateRangeIterator(
	memTables []*memtable.MemTable,
	ssTables []*sstable.Reader,
	startKey, endKey []byte,
) iterator.Iterator {
	baseIter := f.createBaseIterator(memTables, ssTables)
	boundedIter := bounded.NewBoundedIterator(baseIter, startKey, endKey)
	
	// Record range iterator creation telemetry
	if f.metrics != nil {
		// Note: For bounded iterators, we rely on the underlying hierarchical iterator's telemetry
		// The bounded iterator acts as a filter on top of the hierarchical iterator
		f.metrics.RecordIteratorType(context.Background(), "bounded_range", len(memTables)+len(ssTables))
	}
	
	return boundedIter
}

// createBaseIterator creates the base hierarchical iterator
func (f *Factory) createBaseIterator(
	memTables []*memtable.MemTable,
	ssTables []*sstable.Reader,
) iterator.Iterator {
	// If there are no sources, return an empty iterator
	if len(memTables) == 0 && len(ssTables) == 0 {
		return newEmptyIterator()
	}

	// Create individual iterators in newest-to-oldest order
	iterators := make([]iterator.Iterator, 0, len(memTables)+len(ssTables))

	// Add memtable iterators (newest to oldest)
	for _, mt := range memTables {
		iterators = append(iterators, memtable.NewIteratorAdapter(mt.NewIterator()))
	}

	// Add sstable iterators (newest to oldest)
	for i := len(ssTables) - 1; i >= 0; i-- {
		iterators = append(iterators, sstable.NewIteratorAdapter(ssTables[i].NewIterator()))
	}

	// Create hierarchical iterator
	hierarchicalIter := composite.NewHierarchicalIterator(iterators)
	
	// Wrap with telemetry if metrics are available
	if f.metrics != nil {
		return newTelemetryWrapper(hierarchicalIter, f.metrics)
	}
	
	return hierarchicalIter
}

// newEmptyIterator creates an iterator that contains no entries
func newEmptyIterator() iterator.Iterator {
	return &emptyIterator{}
}

// Simple empty iterator implementation
type emptyIterator struct{}

func (e *emptyIterator) SeekToFirst()            {}
func (e *emptyIterator) SeekToLast()             {}
func (e *emptyIterator) Seek(target []byte) bool { return false }
func (e *emptyIterator) Next() bool              { return false }
func (e *emptyIterator) Key() []byte             { return nil }
func (e *emptyIterator) Value() []byte           { return nil }
func (e *emptyIterator) Valid() bool             { return false }
func (e *emptyIterator) IsTombstone() bool       { return false }

// telemetryWrapper wraps any iterator with telemetry instrumentation
type telemetryWrapper struct {
	iterator.Iterator
	metrics iteratorpkg.IteratorMetrics
}

// newTelemetryWrapper creates a new telemetry-instrumented iterator wrapper
func newTelemetryWrapper(iter iterator.Iterator, metrics iteratorpkg.IteratorMetrics) iterator.Iterator {
	wrapper := &telemetryWrapper{
		Iterator: iter,
		metrics:  metrics,
	}
	
	// Record iterator creation
	if metrics != nil {
		metrics.RecordIteratorType(context.Background(), "hierarchical_composite", 1)
	}
	
	return wrapper
}

// Seek wraps the underlying Seek with telemetry
func (t *telemetryWrapper) Seek(target []byte) bool {
	start := time.Now()
	ctx := context.Background()
	
	found := t.Iterator.Seek(target)
	
	// Record seek operation telemetry
	if t.metrics != nil {
		t.recordSeekMetrics(ctx, start, found, 1) // 1 as approximate keys scanned
	}
	
	return found
}

// Next wraps the underlying Next with telemetry
func (t *telemetryWrapper) Next() bool {
	start := time.Now()
	ctx := context.Background()
	
	valid := t.Iterator.Next()
	
	// Record next operation telemetry
	if t.metrics != nil {
		t.recordNextMetrics(ctx, start, valid)
	}
	
	return valid
}

// recordSeekMetrics records telemetry for seek operations with panic protection
func (t *telemetryWrapper) recordSeekMetrics(ctx context.Context, start time.Time, found bool, keysScanned int64) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
		}
	}()

	t.metrics.RecordSeek(ctx, time.Since(start), found, keysScanned)
}

// recordNextMetrics records telemetry for next operations with panic protection
func (t *telemetryWrapper) recordNextMetrics(ctx context.Context, start time.Time, valid bool) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
		}
	}()

	t.metrics.RecordNext(ctx, time.Since(start), valid)
}
