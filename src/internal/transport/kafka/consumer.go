package kafka

import (
	"context"
	"log"
	"time"

	"github.com/Niksan005/GPSmoother/internal/config"
	"github.com/segmentio/kafka-go"
)

// Consumer represents a Kafka consumer
type Consumer struct {
	reader *kafka.Reader
	config *config.KafkaConfig
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.KafkaConfig) (*Consumer, error) {
	log.Printf("Creating Kafka consumer with configuration:")
	log.Printf("  Brokers: %v", cfg.Brokers)
	log.Printf("  Topic: %s", cfg.Topic)
	log.Printf("  GroupID: %s", cfg.GroupID)
	log.Printf("  AutoOffsetReset: %s", cfg.AutoOffsetReset)
	
	// Create a simple dialer
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Test connection before creating reader
	conn, err := dialer.DialContext(context.Background(), "tcp", cfg.Brokers[0])
	if err != nil {
		log.Printf("Failed to connect to Kafka: %v", err)
		return nil, err
	}
	defer conn.Close()
	log.Printf("Successfully connected to Kafka broker")
	
	// Create a simple reader configuration
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		ReadBackoffMin: time.Millisecond * 100,
		ReadBackoffMax: time.Second * 1,
		Dialer:         dialer,
		Logger:         kafka.LoggerFunc(log.Printf),
		ErrorLogger:    kafka.LoggerFunc(log.Printf),
		CommitInterval: time.Second,
	})

	return &Consumer{
		reader: reader,
		config: cfg,
	}, nil
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic %s", c.config.Topic)

	for {
		select {
		case <-ctx.Done():
			return c.reader.Close()
		default:
			log.Printf("Attempting to read messages...")
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				
				log.Printf("Error reading message: %v", err)
				
				// Handle EOF and connection errors
				if err.Error() == "fetching message: EOF" || 
				   err.Error() == "connection reset by peer" ||
				   err.Error() == "connection refused" {
					log.Printf("Connection issue detected, retrying in 5 seconds...")
					time.Sleep(5 * time.Second)
					continue
				}
				
				return err
			}
			
			log.Printf("Received message: %s", string(msg.Value))
		}
	}
}

// Close closes the Kafka reader
func (c *Consumer) Close() error {
	return c.reader.Close()
} 