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
}

// Config holds processor service configuration
type Config struct {
	KafkaConfig kafka.Config
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

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping message processing")
			return
		default:
			msg, err := p.kafkaClient.ReadMessage(ctx)
			if err != nil {
				p.logger.Errorf("Error reading message: %v", err)
				continue
			}

			// Parse raw GPS data from message
			var rawData struct {
				Latitude    string `json:"lat"`
				Longitude   string `json:"lon"`
				Timestamp   string `json:"ts"`
				DeviceID    string `json:"dev_id"`
				Speed       string `json:"speed"`
				Altitude    string `json:"alt"`
				Signal      string `json:"signal"`
				Satellites  string `json:"sats"`
				Status      string `json:"status"`
				InDepot     string `json:"in_depot"`
				VehicleID   string `json:"veh_id"`
				FormattedTS string `json:"formatted_ts"`
			}

			if err := json.Unmarshal(msg.Value, &rawData); err != nil {
				p.logger.WithFields(logrus.Fields{
					"error": err,
					"data":  string(msg.Value),
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

			p.logger.WithFields(logrus.Fields{
				"device_id":  rawData.DeviceID,
				"vehicle_id": rawData.VehicleID,
				"latitude":   latitude,
				"longitude":  longitude,
				"speed":      speed,
				"altitude":   altitude,
				"signal":     signal,
				"satellites": satellites,
				"status":     rawData.Status,
				"in_depot":   rawData.InDepot,
				"timestamp":  timestamp,
				"formatted":  rawData.FormattedTS,
			}).Info("Received GPS data")

			// TODO: Process the GPS data here
			// This is where you'll implement the actual GPS data processing logic
		}
	}
} 