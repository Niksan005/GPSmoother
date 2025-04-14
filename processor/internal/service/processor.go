package service

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/niksan/gpsmoother/processor/internal/config"
	"github.com/niksan/gpsmoother/processor/internal/kafka"
	"github.com/sirupsen/logrus"
)

// Processor represents the GPS data processor service
type Processor struct {
	kafkaClient          *kafka.Client
	gpsProcessor         *GPSProcessor
	logger               *logrus.Logger
	wg                   sync.WaitGroup
	batchSize            int
	lastProcessedOffsets map[int32]int64
	stateMutex           sync.RWMutex
	lastHealthCheck      time.Time
}

// Config holds processor service configuration
type Config struct {
	KafkaConfig kafka.Config
	OSRM        config.OSRMConfig
	BatchSize   int
}

// NewProcessor creates a new processor service
func NewProcessor(cfg Config, logger *logrus.Logger) (*Processor, error) {
	kafkaClient, err := kafka.NewClient(cfg.KafkaConfig, logger)
	if err != nil {
		return nil, err
	}

	return &Processor{
		kafkaClient:          kafkaClient,
		gpsProcessor:         NewGPSProcessor(logger, cfg.OSRM.URL),
		logger:               logger,
		batchSize:            cfg.BatchSize,
		lastProcessedOffsets: make(map[int32]int64),
		lastHealthCheck:      time.Now(),
	}, nil
}

// Start begins processing GPS data
func (p *Processor) Start(ctx context.Context) {
	p.wg.Add(2) // Add one more for health check
	go p.processMessages(ctx)
	go p.healthCheck(ctx)
}

// Stop gracefully shuts down the processor
func (p *Processor) Stop() error {
	p.wg.Wait()
	return p.kafkaClient.Close()
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
			p.stateMutex.Lock()
			now := time.Now()
			if now.Sub(p.lastHealthCheck) > 2*time.Minute {
				p.logger.Error("Health check failed - processor appears to be stuck")
				// Here you could trigger a restart or alert
			}
			p.lastHealthCheck = now
			p.stateMutex.Unlock()
		}
	}
}

// processMessages continuously reads and processes messages from Kafka
func (p *Processor) processMessages(ctx context.Context) {
	defer p.wg.Done()

	p.logger.Info("Starting message processing loop")
	
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping message processing")
			return
		default:
			p.logger.WithFields(logrus.Fields{
				"batch_size": p.batchSize,
			}).Debug("Attempting to read next batch of messages")

			// Update health check timestamp
			p.stateMutex.Lock()
			p.lastHealthCheck = time.Now()
			p.stateMutex.Unlock()

			partitionMessages, err := p.kafkaClient.ReadMessageBatch(ctx, p.batchSize)
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

			// Process each partition's messages
			for partition, messages := range partitionMessages {
				p.logger.WithFields(logrus.Fields{
					"partition": partition,
					"message_count": len(messages),
				}).Info("Processing messages for partition")

				// Store the first message's offset before processing
				firstOffset := messages[0].Offset

				// Convert messages to byte slices for processing
				messageBytes := make([][]byte, len(messages))
				for i, msg := range messages {
					messageBytes[i] = msg.Value
				}

				// Process the batch of messages
				processedBatch, processingErrors := p.gpsProcessor.ProcessBatch(messageBytes)

				// If processing was successful, smooth the data and write to output topic
				if len(processingErrors) == 0 {
					// Smooth the GPS data
					smoothedBatch, err := p.gpsProcessor.SmoothGPSData(processedBatch)
					if err != nil {
						p.logger.WithFields(logrus.Fields{
							"error": err,
							"partition": partition,
						}).Error("Failed to smooth GPS data")
						continue
					}

					// Write smoothed data to output topic
					for _, point := range smoothedBatch {
						pointBytes, err := json.Marshal(point)
						if err != nil {
							p.logger.WithFields(logrus.Fields{
								"error": err,
								"partition": partition,
							}).Error("Failed to marshal smoothed point")
							continue
						}

						if err := p.kafkaClient.WriteMessage(ctx, []byte(point.DeviceID), pointBytes); err != nil {
							p.logger.WithFields(logrus.Fields{
								"error": err,
								"partition": partition,
							}).Error("Failed to write smoothed point to output topic")
							continue
						}
					}

					// Update the last processed offset
					p.stateMutex.Lock()
					p.lastProcessedOffsets[partition] = messages[len(messages)-1].Offset
					p.stateMutex.Unlock()

					// Commit the messages
					if err := p.kafkaClient.CommitMessages(ctx, partition, messages); err != nil {
						p.logger.WithFields(logrus.Fields{
							"error": err,
							"partition": partition,
						}).Error("Failed to commit messages")
						continue
					}
				} else {
					// If there were errors, seek back to the first message
					p.logger.WithFields(logrus.Fields{
						"partition": partition,
						"error_count": len(processingErrors),
						"first_offset": firstOffset,
					}).Error("Errors occurred during processing, seeking back to first message")
					
					if err := p.kafkaClient.SeekToOffset(partition, firstOffset); err != nil {
						p.logger.WithFields(logrus.Fields{
							"error": err,
							"partition": partition,
							"offset": firstOffset,
						}).Error("Failed to seek back to first message")
					}
				}

				// Log batch processing summary for this partition
				p.logger.WithFields(logrus.Fields{
					"partition": partition,
					"batch_size": len(messages),
					"processed_count": len(processedBatch),
					"error_count": len(processingErrors),
					"processed_messages": processedBatch,
					"last_offset": messages[len(messages)-1].Offset,
				}).Info("Processed batch of GPS data for partition")
			}
		}
	}
}

// GetLastProcessedOffsets returns the last processed offset for each partition
func (p *Processor) GetLastProcessedOffsets() map[int32]int64 {
	p.stateMutex.RLock()
	defer p.stateMutex.RUnlock()
	
	// Create a copy of the map to avoid concurrent access issues
	offsets := make(map[int32]int64)
	for k, v := range p.lastProcessedOffsets {
		offsets[k] = v
	}
	return offsets
} 