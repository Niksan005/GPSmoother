package service

import (
	"sync"
)

// OffsetTrackerImpl implements the OffsetTracker interface
type OffsetTrackerImpl struct {
	offsets    map[int32]int64
	stateMutex sync.RWMutex
}

// NewOffsetTracker creates a new offset tracker
func NewOffsetTracker() *OffsetTrackerImpl {
	return &OffsetTrackerImpl{
		offsets: make(map[int32]int64),
	}
}

// UpdateOffset updates the last processed offset for a partition
func (t *OffsetTrackerImpl) UpdateOffset(partition int32, offset int64) {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()
	t.offsets[partition] = offset
}

// GetLastProcessedOffsets returns a copy of the last processed offsets
func (t *OffsetTrackerImpl) GetLastProcessedOffsets() map[int32]int64 {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	
	offsets := make(map[int32]int64)
	for k, v := range t.offsets {
		offsets[k] = v
	}
	return offsets
} 