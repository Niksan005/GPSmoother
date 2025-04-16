package service

import (
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// HealthCheckerImpl implements the HealthChecker interface
type HealthCheckerImpl struct {
	logger         *logrus.Logger
	lastCheck      time.Time
	stateMutex     sync.RWMutex
	timeout        time.Duration
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(logger *logrus.Logger) *HealthCheckerImpl {
	return &HealthCheckerImpl{
		logger:    logger,
		lastCheck: time.Now(),
		timeout:   2 * time.Minute,
	}
}

// CheckHealth verifies if the processor is healthy
func (h *HealthCheckerImpl) CheckHealth() error {
	h.stateMutex.RLock()
	defer h.stateMutex.RUnlock()

	if time.Since(h.lastCheck) > h.timeout {
		return ErrProcessorStuck
	}
	return nil
}

// UpdateLastCheck updates the timestamp of the last health check
func (h *HealthCheckerImpl) UpdateLastCheck() {
	h.stateMutex.Lock()
	defer h.stateMutex.Unlock()
	h.lastCheck = time.Now()
}

// GetLastCheck returns the timestamp of the last health check
func (h *HealthCheckerImpl) GetLastCheck() time.Time {
	h.stateMutex.RLock()
	defer h.stateMutex.RUnlock()
	return h.lastCheck
}

// ErrProcessorStuck is returned when the processor appears to be stuck
var ErrProcessorStuck = errors.New("processor appears to be stuck") 