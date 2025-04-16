package service

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// GPSDataProcessor defines the contract for processing GPS data
type GPSDataProcessor interface {
	ProcessBatch(messages [][]byte) ([]ProcessedGPSData, []error)
	SmoothGPSData(points []ProcessedGPSData) ([]ProcessedGPSData, error)
}

// MessageReader defines the contract for reading messages
type MessageReader interface {
	ReadMessageBatch(ctx context.Context, batchSize int) (map[int32][]kafkago.Message, error)
	SeekToOffset(partition int32, offset int64) error
	CommitMessages(ctx context.Context, partition int32, messages []kafkago.Message) error
}

// MessageWriter defines the contract for writing messages
type MessageWriter interface {
	WriteMessage(ctx context.Context, key []byte, value []byte) error
}

// HealthChecker defines the contract for health checking
type HealthChecker interface {
	CheckHealth() error
	UpdateLastCheck()
	GetLastCheck() time.Time
}

// OffsetTracker defines the contract for tracking message offsets
type OffsetTracker interface {
	UpdateOffset(partition int32, offset int64)
	GetLastProcessedOffsets() map[int32]int64
} 