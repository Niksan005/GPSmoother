package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/niksan/gpsmoother/processor/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func parseProtocolVersion(version string) (int, int, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return 0, 0, fmt.Errorf("invalid version format: %s", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return major, minor, nil
}

func main() {
	// Load configuration
	cfg, err := config.LoadConfig(config.GetConfigPath())
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Setup logger
	logger := logrus.New()
	if cfg.Logging.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{})
	}
	logger.SetOutput(os.Stdout)

	// Set log level
	level, err := logrus.ParseLevel(cfg.Logging.Level)
	if err != nil {
		logger.Warnf("Invalid log level '%s', defaulting to 'info'", cfg.Logging.Level)
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	// Parse Kafka protocol version
	major, minor, err := parseProtocolVersion(cfg.Kafka.ProtocolVersion)
	if err != nil {
		logger.Fatalf("Invalid Kafka protocol version: %v", err)
	}
	logger.Infof("Using Kafka protocol version %d.%d", major, minor)

	// Initialize Kafka writer
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers),
		Topic:    cfg.Kafka.Topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	// Initialize Kafka dialer
	dialer := &kafka.Dialer{
		ClientID: "gps-processor",
	}

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Kafka.Brokers},
		Topic:   cfg.Kafka.Topic,
		GroupID: cfg.Kafka.GroupID,
		Dialer:  dialer,
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
			kafka.RoundRobinGroupBalancer{},
		},
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
		ReadLagInterval: -1,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout: 10 * time.Second,
		RebalanceTimeout: 60 * time.Second,
		RetentionTime: 24 * time.Hour,
		StartOffset: kafka.FirstOffset,
		MaxAttempts: 3,
	})

	defer kafkaReader.Close()

	router := mux.NewRouter()

	// Health check endpoint
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}).Methods("GET")

	// Start Kafka message processing in a goroutine
	go func() {
		for {
			msg, err := kafkaReader.ReadMessage(context.Background())
			if err != nil {
				logger.Errorf("Error reading message: %v", err)
				continue
			}
			logger.Infof("Received message: %s", string(msg.Value))
			// TODO: Process the GPS data here
		}
	}()

	// Start HTTP server
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	logger.Infof("Starting processor service on %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		logger.Fatalf("Failed to start server: %v", err)
	}
} 