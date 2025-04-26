package replication

// No imports needed

// processorIndex finds the index of a processor in the processors slice
// Returns -1 if not found
func (r *WALReplicator) processorIndex(target EntryProcessor) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	for i, p := range r.processors {
		if p == target {
			return i
		}
	}
	return -1
}

// RemoveProcessor removes an EntryProcessor from the replicator
func (r *WALReplicator) RemoveProcessor(processor EntryProcessor) {
	if processor == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Find the processor in the slice
	idx := -1
	for i, p := range r.processors {
		if p == processor {
			idx = i
			break
		}
	}

	// If found, remove it
	if idx >= 0 {
		// Remove the element by replacing it with the last element and truncating
		lastIdx := len(r.processors) - 1
		if idx < lastIdx {
			r.processors[idx] = r.processors[lastIdx]
		}
		r.processors = r.processors[:lastIdx]
	}
}