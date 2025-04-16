package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/niksan/gpsmoother/processor/internal/config"
	"github.com/niksan/gpsmoother/processor/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Processor represents the GPS data processor service
type Processor struct {
	messageReader  MessageReader
	messageWriter  MessageWriter
	gpsProcessor   GPSDataProcessor
	logger         *logrus.Logger
	wg             sync.WaitGroup
	batchSize      int
	offsetTracker  OffsetTracker
	healthChecker  HealthChecker
	workerCount    int
	processingMap  sync.Map // Track currently processing offsets
}

// Config holds processor service configuration
type Config struct {
	KafkaConfig kafka.Config
	OSRM        config.OSRMConfig
	BatchSize   int
	WorkerCount int
}

// NewProcessor creates a new processor service
func NewProcessor(cfg Config, logger *logrus.Logger) (*Processor, error) {
	kafkaClient, err := kafka.NewClient(cfg.KafkaConfig, logger)
	if err != nil {
		return nil, err
	}

	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 3
	}

	offsetTracker := NewOffsetTracker()
	healthChecker := NewHealthChecker(logger)

	return &Processor{
		messageReader: kafkaClient,
		messageWriter: kafkaClient,
		gpsProcessor:  NewGPSProcessor(logger, cfg.OSRM.URL),
		logger:        logger,
		batchSize:     cfg.BatchSize,
		offsetTracker: offsetTracker,
		healthChecker: healthChecker,
		workerCount:   workerCount,
	}, nil
}

// Start begins processing GPS data
func (p *Processor) Start(ctx context.Context) {
	p.wg.Add(2)
	go p.processMessages(ctx)
	go p.healthCheck(ctx)
}

// Stop gracefully shuts down the processor
func (p *Processor) Stop() error {
	p.wg.Wait()
	return nil
}

// healthCheck periodically checks the health of the processor
func (p *Processor) healthCheck(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.healthChecker.CheckHealth(); err != nil {
				p.logger.Error("Health check failed: ", err)
			}
		}
	}
}

// isAlreadyProcessing checks if a batch of messages is already being processed
func (p *Processor) isAlreadyProcessing(partition int32, firstOffset int64) bool {
	key := p.getProcessingKey(partition, firstOffset)
	_, exists := p.processingMap.LoadOrStore(key, struct{}{})
	return exists
}

// markProcessingComplete marks a batch as no longer being processed
func (p *Processor) markProcessingComplete(partition int32, firstOffset int64) {
	key := p.getProcessingKey(partition, firstOffset)
	p.processingMap.Delete(key)
}

// getProcessingKey generates a unique key for tracking processing status
func (p *Processor) getProcessingKey(partition int32, offset int64) string {
	return fmt.Sprintf("%d:%d", partition, offset)
}

// processMessages continuously reads and processes messages from Kafka
func (p *Processor) processMessages(ctx context.Context) {
	defer p.wg.Done()

	p.logger.Info("Starting message processing loop")
	
	workerPool := make(chan struct{}, p.workerCount)
	
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping message processing")
			return
		default:
			p.logger.WithFields(logrus.Fields{
				"batch_size": p.batchSize,
				"active_workers": len(workerPool),
				"max_workers": p.workerCount,
			}).Debug("Attempting to read next batch of messages")

			p.healthChecker.UpdateLastCheck()

			partitionMessages, err := p.messageReader.ReadMessageBatch(ctx, p.batchSize)
			if err != nil {
				p.logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("Error reading messages from Kafka")
				continue
			}

			if len(partitionMessages) == 0 {
				p.logger.Debug("No messages received in any partition")
				continue
			}

			p.logger.WithFields(logrus.Fields{
				"partition_count": len(partitionMessages),
				"total_messages": func() int {
					total := 0
					for _, msgs := range partitionMessages {
						total += len(msgs)
					}
					return total
				}(),
			}).Info("Received messages from partitions")

			for partition, messages := range partitionMessages {
				firstOffset := messages[0].Offset
				lastOffset := messages[len(messages)-1].Offset
				
				p.logger.WithFields(logrus.Fields{
					"partition": partition,
					"message_count": len(messages),
					"first_offset": firstOffset,
					"last_offset": lastOffset,
					"current_processed_offset": p.offsetTracker.GetLastProcessedOffsets()[partition],
				}).Debug("Processing messages for partition")

				// Skip if already processing this batch
				if p.isAlreadyProcessing(partition, firstOffset) {
					p.logger.WithFields(logrus.Fields{
						"partition": partition,
						"offset": firstOffset,
					}).Debug("Skipping already processing batch")
					continue
				}

				select {
				case workerPool <- struct{}{}:
					p.wg.Add(1)
					go func(partition int32, messages []kafkago.Message) {
						defer p.wg.Done()
						defer func() { <-workerPool }()
						defer p.markProcessingComplete(partition, firstOffset)

						p.processPartitionMessages(ctx, partition, messages)
					}(partition, messages)
				default:
					p.logger.WithFields(logrus.Fields{
						"active_workers": len(workerPool),
						"max_workers": p.workerCount,
					}).Debug("Worker pool full, waiting for available worker")
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}

// processPartitionMessages processes messages from a single partition
func (p *Processor) processPartitionMessages(ctx context.Context, partition int32, messages []kafkago.Message) {
	if len(messages) == 0 {
		return
	}

	// Check if we've already processed this batch
	firstOffset := messages[0].Offset
	lastOffset := messages[len(messages)-1].Offset
	currentOffset := p.offsetTracker.GetLastProcessedOffsets()[partition]

	p.logger.WithFields(logrus.Fields{
		"partition": partition,
		"first_offset": firstOffset,
		"last_offset": lastOffset,
		"current_processed_offset": currentOffset,
		"message_count": len(messages),
	}).Info("Starting batch processing")

	if currentOffset >= firstOffset {
		p.logger.WithFields(logrus.Fields{
			"partition": partition,
			"first_offset": firstOffset,
			"current_offset": currentOffset,
		}).Debug("Skipping already processed messages")
		return
	}

	// Convert messages to byte arrays for batch processing
	messageBytes := make([][]byte, len(messages))
	for i, msg := range messages {
		messageBytes[i] = msg.Value
	}

	// Process the batch of messages
	processedBatch, processingErrors := p.gpsProcessor.ProcessBatch(messageBytes)
	if len(processingErrors) > 0 {
		p.logger.WithFields(logrus.Fields{
			"partition": partition,
			"error_count": len(processingErrors),
			"first_offset": firstOffset,
			"last_offset": lastOffset,
		}).Error("Errors occurred during batch processing")
		return
	}

	p.logger.WithFields(logrus.Fields{
		"partition": partition,
		"processed_count": len(processedBatch),
		"first_offset": firstOffset,
		"last_offset": lastOffset,
	}).Info("Successfully processed batch")

	// Smooth the processed GPS data
	smoothedBatch, err := p.gpsProcessor.SmoothGPSData(processedBatch)
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"error": err,
			"partition": partition,
			"first_offset": firstOffset,
			"last_offset": lastOffset,
		}).Error("Failed to smooth GPS data")
		return
	}

	p.logger.WithFields(logrus.Fields{
		"partition": partition,
		"smoothed_count": len(smoothedBatch),
		"first_offset": firstOffset,
		"last_offset": lastOffset,
	}).Info("Successfully smoothed batch")

	// Write the processed data back to Kafka
	successfulWrites := 0
	for _, point := range smoothedBatch {
		pointBytes, err := json.Marshal(point)
		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"error": err,
				"partition": partition,
				"device_id": point.DeviceID,
			}).Error("Failed to marshal processed data")
			continue
		}

		if err := p.messageWriter.WriteMessage(ctx, []byte(point.DeviceID), pointBytes); err != nil {
			p.logger.WithFields(logrus.Fields{
				"error": err,
				"partition": partition,
				"device_id": point.DeviceID,
			}).Error("Failed to write processed message")
			continue
		}
		successfulWrites++
	}

	p.logger.WithFields(logrus.Fields{
		"partition": partition,
		"successful_writes": successfulWrites,
		"total_points": len(smoothedBatch),
		"first_offset": firstOffset,
		"last_offset": lastOffset,
	}).Info("Completed writing processed messages")

	// Update the offset tracker with the last message's offset
	p.offsetTracker.UpdateOffset(partition, lastOffset)

	// Commit the messages after successful processing
	if err := p.messageReader.CommitMessages(ctx, partition, messages); err != nil {
		p.logger.WithFields(logrus.Fields{
			"error": err,
			"partition": partition,
			"last_offset": lastOffset,
		}).Error("Failed to commit messages")
		return
	}

	p.logger.WithFields(logrus.Fields{
		"partition": partition,
		"last_offset": lastOffset,
		"new_processed_offset": p.offsetTracker.GetLastProcessedOffsets()[partition],
	}).Info("Successfully committed messages")
}

// GetLastProcessedOffsets returns the last processed offset for each partition
func (p *Processor) GetLastProcessedOffsets() map[int32]int64 {
	return p.offsetTracker.GetLastProcessedOffsets()
} 