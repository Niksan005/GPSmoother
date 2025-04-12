package config

import (
	"time"
)

// Config represents the application configuration
type Config struct {
	Kafka KafkaConfig
}

// KafkaConfig represents Kafka-specific configuration
type KafkaConfig struct {
	Brokers         []string
	Topic           string
	GroupID         string
	AutoOffsetReset string
	SessionTimeout  time.Duration
	BatchSize       int           // Number of messages to process in a batch
	BatchTimeout    time.Duration // Maximum time to wait for a batch to fill
	ReadTimeout     time.Duration // Timeout for reading individual messages
}

// NewConfig creates a new Config instance with default values
func NewConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:         []string{"kafka:9092"},
			Topic:           "gps-data",
			GroupID:         "gpsmoother-group-1",
			AutoOffsetReset: "earliest",
			SessionTimeout:  30 * time.Second,
			BatchSize:       10,
			BatchTimeout:    30 * time.Second,
			ReadTimeout:     60 * time.Second,
		},
	}
} 