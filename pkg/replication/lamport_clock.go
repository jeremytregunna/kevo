package replication

import (
	"sync"
)

// LamportClock implements a logical clock based on Lamport timestamps
// for maintaining causal ordering of events in a distributed system.
type LamportClock struct {
	counter uint64
	mu      sync.Mutex
}

// NewLamportClock creates a new LamportClock instance
func NewLamportClock() *LamportClock {
	return &LamportClock{counter: 0}
}

// Tick increments the clock and returns the new timestamp value
func (c *LamportClock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter++
	return c.counter
}

// Update updates the clock based on a received timestamp,
// ensuring the local clock is at least as large as the received timestamp,
// then increments and returns the new value
func (c *LamportClock) Update(received uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.counter {
		c.counter = received
	}
	c.counter++
	return c.counter
}

// Current returns the current timestamp without incrementing the clock
func (c *LamportClock) Current() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counter
}
