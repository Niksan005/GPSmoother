package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Client represents a Kafka client that can read and write messages
type Client struct {
	readers map[int32]*kafka.Reader
	writer  *kafka.Writer
	logger  *logrus.Logger
}

// Config holds Kafka client configuration
type Config struct {
	Brokers          string
	InputTopic       string
	OutputTopic      string
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
	// Initialize Kafka writer for output topic
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers),
		Topic:    cfg.OutputTopic,
		Balancer: &kafka.Hash{},
	}

	// Initialize Kafka dialer
	dialer := &kafka.Dialer{
		ClientID: "gps-processor",
	}

	// Get topic partitions
	conn, err := kafka.Dial("tcp", cfg.Brokers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(cfg.InputTopic)
	if err != nil {
		return nil, err
	}

	// Initialize readers for each partition
	readers := make(map[int32]*kafka.Reader)
	for _, p := range partitions {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{cfg.Brokers},
			Topic:   cfg.InputTopic,
			Partition: int(p.ID),
			GroupID: cfg.GroupID,
			Dialer:  dialer,
			MinBytes:         cfg.MinBytes,
			MaxBytes:         cfg.MaxBytes,
			MaxWait:          cfg.MaxWait,
			ReadLagInterval:  -1,
			HeartbeatInterval: cfg.HeartbeatInterval,
			SessionTimeout:    cfg.SessionTimeout,
			RebalanceTimeout:  cfg.RebalanceTimeout,
			RetentionTime:     cfg.RetentionTime,
			StartOffset:       kafka.LastOffset,
			MaxAttempts:       cfg.MaxAttempts,
		})
		readers[int32(p.ID)] = reader
	}

	return &Client{
		readers: readers,
		writer:  writer,
		logger:  logger,
	}, nil
}

// WriteMessage writes a message to Kafka
func (c *Client) WriteMessage(ctx context.Context, key, value []byte) error {
	return c.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

// ReadMessageBatch reads a batch of messages from all Kafka partitions
func (c *Client) ReadMessageBatch(ctx context.Context, batchSize int) (map[int32][]kafka.Message, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be greater than 0")
	}

	partitionMessages := make(map[int32][]kafka.Message)
	
	for partitionID, reader := range c.readers {
		messages := make([]kafka.Message, 0, batchSize)
		readCount := 0

		// Try to read up to batchSize messages
		for readCount < batchSize {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					break // Stop reading if context is done
				}
				return partitionMessages, err
			}
			messages = append(messages, msg)
			readCount++
		}

		// Only process if we got a full batch
		if len(messages) == batchSize {
			// Log the messages before processing
			c.logger.WithFields(logrus.Fields{
				"partition": partitionID,
				"message_count": len(messages),
				"batch_size": batchSize,
			}).Info("Batch of messages read from Kafka partition")
			
			partitionMessages[partitionID] = messages
		} else if len(messages) > 0 {
			// If we got some messages but not a full batch, seek back to the start of the batch
			if err := reader.SetOffset(messages[0].Offset); err != nil {
				c.logger.WithError(err).Error("Failed to seek back to batch start")
			}
		}
	}
	
	return partitionMessages, nil
}

// CommitMessages commits the offset for a batch of messages
func (c *Client) CommitMessages(ctx context.Context, partition int32, messages []kafka.Message) error {
	reader, ok := c.readers[partition]
	if !ok {
		return fmt.Errorf("no reader found for partition %d", partition)
	}

	if len(messages) == 0 {
		return nil
	}

	// Commit the offset for the last message in the batch
	lastMsg := messages[len(messages)-1]
	if err := reader.CommitMessages(ctx, lastMsg); err != nil {
		c.logger.WithFields(logrus.Fields{
			"error": err,
			"partition": partition,
			"offset": lastMsg.Offset,
		}).Error("Failed to commit messages")
		return err
	}

	c.logger.WithFields(logrus.Fields{
		"partition": partition,
		"offset": lastMsg.Offset,
	}).Info("Successfully committed messages")

	return nil
}

// Close closes the Kafka client
func (c *Client) Close() error {
	for _, reader := range c.readers {
		if err := reader.Close(); err != nil {
			c.logger.Errorf("Error closing Kafka reader: %v", err)
			return err
		}
	}
	if err := c.writer.Close(); err != nil {
		c.logger.Errorf("Error closing Kafka writer: %v", err)
		return err
	}
	return nil
}

// SeekToOffset seeks to a specific offset in a partition
func (c *Client) SeekToOffset(partition int32, offset int64) error {
	reader, ok := c.readers[partition]
	if !ok {
		return fmt.Errorf("no reader found for partition %d", partition)
	}

	if err := reader.SetOffset(offset); err != nil {
		c.logger.WithFields(logrus.Fields{
			"error": err,
			"partition": partition,
			"offset": offset,
		}).Error("Failed to seek to offset")
		return err
	}

	c.logger.WithFields(logrus.Fields{
		"partition": partition,
		"offset": offset,
	}).Info("Successfully sought to offset")

	return nil
} 