package service

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/niksan/gpsmoother/processor/internal/kafka"
	"github.com/sirupsen/logrus"
)

// Processor represents the GPS data processor service
type Processor struct {
	kafkaClient *kafka.Client
	logger      *logrus.Logger
	wg          sync.WaitGroup
	batchSize   int
}

// Config holds processor service configuration
type Config struct {
	KafkaConfig kafka.Config
	BatchSize   int
}

// NewProcessor creates a new processor service
func NewProcessor(cfg Config, logger *logrus.Logger) (*Processor, error) {
	kafkaClient, err := kafka.NewClient(cfg.KafkaConfig, logger)
	if err != nil {
		return nil, err
	}

	return &Processor{
		kafkaClient: kafkaClient,
		logger:      logger,
		batchSize:   cfg.BatchSize,
	}, nil
}

// Start begins processing GPS data
func (p *Processor) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.processMessages(ctx)
}

// Stop gracefully shuts down the processor
func (p *Processor) Stop() error {
	p.wg.Wait()
	return p.kafkaClient.Close()
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

				processedBatch := make([]ProcessedGPSData, 0, len(messages))
				processingErrors := make([]error, 0)

				for i, msg := range messages {
					p.logger.WithFields(logrus.Fields{
						"partition": partition,
						"message_index": i + 1,
						"total_messages": len(messages),
						"offset": msg.Offset,
					}).Debug("Processing message")

					// Parse raw GPS data from message
					var rawData RawGPSData
					if err := json.Unmarshal(msg.Value, &rawData); err != nil {
						processingErrors = append(processingErrors, err)
						p.logger.WithFields(logrus.Fields{
							"error": err,
							"data":  string(msg.Value),
							"offset": msg.Offset,
							"partition": partition,
						}).Error("Failed to parse GPS data")
						continue
					}

					// Convert string values to appropriate types
					latitude, _ := strconv.ParseFloat(rawData.Latitude, 64)
					longitude, _ := strconv.ParseFloat(rawData.Longitude, 64)
					speed, _ := strconv.ParseFloat(rawData.Speed, 64)
					altitude, _ := strconv.ParseFloat(rawData.Altitude, 64)
					signal, _ := strconv.Atoi(rawData.Signal)
					satellites, _ := strconv.Atoi(rawData.Satellites)
					ts, _ := strconv.ParseInt(rawData.Timestamp, 10, 64)
					timestamp := time.Unix(0, ts*int64(time.Millisecond))

					processedData := ProcessedGPSData{
						Latitude:    latitude,
						Longitude:   longitude,
						Speed:       speed,
						Altitude:    altitude,
						Signal:      signal,
						Satellites:  satellites,
						Timestamp:   timestamp,
						DeviceID:    rawData.DeviceID,
						VehicleID:   rawData.VehicleID,
						Status:      rawData.Status,
						InDepot:     rawData.InDepot,
						FormattedTS: rawData.FormattedTS,
					}

					processedBatch = append(processedBatch, processedData)
				}

				// Log batch processing summary for this partition
				p.logger.WithFields(logrus.Fields{
					"partition": partition,
					"batch_size": len(messages),
					"processed_count": len(processedBatch),
					"error_count": len(processingErrors),
					"processed_messages": processedBatch,
					"partition_messages": messages,
				}).Info("Processed batch of GPS data for partition")
			}
		}
	}
} 