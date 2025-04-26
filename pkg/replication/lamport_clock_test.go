package replication

import (
	"sync"
	"testing"
)

func TestLamportClockTick(t *testing.T) {
	clock := NewLamportClock()

	// Initial tick should return 1
	if ts := clock.Tick(); ts != 1 {
		t.Errorf("First tick should return 1, got %d", ts)
	}

	// Second tick should return 2
	if ts := clock.Tick(); ts != 2 {
		t.Errorf("Second tick should return 2, got %d", ts)
	}
}

func TestLamportClockUpdate(t *testing.T) {
	clock := NewLamportClock()

	// Update with lower value should increment
	ts := clock.Update(0)
	if ts != 1 {
		t.Errorf("Update with lower value should return 1, got %d", ts)
	}

	// Update with same value should increment
	ts = clock.Update(1)
	if ts != 2 {
		t.Errorf("Update with same value should return 2, got %d", ts)
	}

	// Update with higher value should use that value and increment
	ts = clock.Update(10)
	if ts != 11 {
		t.Errorf("Update with higher value should return 11, got %d", ts)
	}

	// Subsequent tick should continue from updated value
	ts = clock.Tick()
	if ts != 12 {
		t.Errorf("Tick after update should return 12, got %d", ts)
	}
}

func TestLamportClockCurrent(t *testing.T) {
	clock := NewLamportClock()

	// Initial current should be 0
	if ts := clock.Current(); ts != 0 {
		t.Errorf("Initial current should be 0, got %d", ts)
	}

	// After tick, current should reflect new value
	clock.Tick()
	if ts := clock.Current(); ts != 1 {
		t.Errorf("Current after tick should be 1, got %d", ts)
	}

	// Current should not increment the clock
	if ts := clock.Current(); ts != 1 {
		t.Errorf("Multiple calls to Current should return same value, got %d", ts)
	}
}

func TestLamportClockConcurrency(t *testing.T) {
	clock := NewLamportClock()
	iterations := 1000
	var wg sync.WaitGroup

	// Run multiple goroutines calling Tick concurrently
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			clock.Tick()
		}()
	}
	wg.Wait()

	// After iterations concurrent ticks, value should be iterations
	if ts := clock.Current(); ts != uint64(iterations) {
		t.Errorf("After %d concurrent ticks, expected value %d, got %d",
			iterations, iterations, ts)
	}
}
