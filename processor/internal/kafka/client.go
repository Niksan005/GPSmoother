package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Client represents a Kafka client that can read and write messages
type Client struct {
	reader *kafka.Reader
	writer *kafka.Writer
	logger *logrus.Logger
}

// Config holds Kafka client configuration
type Config struct {
	Brokers          string
	Topic            string
	GroupID          string
	ProtocolVersion  string
	MinBytes         int
	MaxBytes         int
	MaxWait          time.Duration
	HeartbeatInterval time.Duration
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	RetentionTime     time.Duration
	MaxAttempts       int
}

// NewClient creates a new Kafka client
func NewClient(cfg Config, logger *logrus.Logger) (*Client, error) {
	// Initialize Kafka writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers),
		Topic:    cfg.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	// Initialize Kafka dialer
	dialer := &kafka.Dialer{
		ClientID: "gps-processor",
	}

	// Initialize Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Brokers},
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
		Dialer:  dialer,
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
			kafka.RoundRobinGroupBalancer{},
		},
		MinBytes:         cfg.MinBytes,
		MaxBytes:         cfg.MaxBytes,
		MaxWait:          cfg.MaxWait,
		ReadLagInterval:  -1,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SessionTimeout:    cfg.SessionTimeout,
		RebalanceTimeout:  cfg.RebalanceTimeout,
		RetentionTime:     cfg.RetentionTime,
		StartOffset:       kafka.FirstOffset,
		MaxAttempts:       cfg.MaxAttempts,
	})

	return &Client{
		reader: reader,
		writer: writer,
		logger: logger,
	}, nil
}

// ReadMessage reads a message from Kafka
func (c *Client) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

// WriteMessage writes a message to Kafka
func (c *Client) WriteMessage(ctx context.Context, key, value []byte) error {
	return c.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

// Close closes the Kafka client
func (c *Client) Close() error {
	if err := c.reader.Close(); err != nil {
		c.logger.Errorf("Error closing Kafka reader: %v", err)
		return err
	}
	if err := c.writer.Close(); err != nil {
		c.logger.Errorf("Error closing Kafka writer: %v", err)
		return err
	}
	return nil
} 