package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niksan/gpsmoother/processor/internal/config"
	"github.com/niksan/gpsmoother/processor/internal/kafka"
	"github.com/niksan/gpsmoother/processor/internal/server"
	"github.com/niksan/gpsmoother/processor/internal/service"
	"github.com/sirupsen/logrus"
)

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

	// Create Kafka client configuration
	kafkaConfig := kafka.Config{
		Brokers:          cfg.Kafka.Brokers,
		InputTopic:       cfg.Kafka.InputTopic,
		OutputTopic:      cfg.Kafka.OutputTopic,
		GroupID:          cfg.Kafka.GroupID,
		ProtocolVersion:  cfg.Kafka.ProtocolVersion,
		MinBytes:         10e3, // 10KB
		MaxBytes:         10e6, // 10MB
		MaxWait:          1 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    10 * time.Second,
		RebalanceTimeout:  60 * time.Second,
		RetentionTime:     24 * time.Hour,
		MaxAttempts:       3,
	}

	// Create processor service
	processor, err := service.NewProcessor(service.Config{
		KafkaConfig: kafkaConfig,
		OSRM:        cfg.OSRM,
		BatchSize:   cfg.Kafka.BatchSize,
	}, logger)
	if err != nil {
		logger.Fatalf("Failed to create processor: %v", err)
	}

	// Create HTTP server
	httpServer := server.NewServer(server.Config{
		Host: cfg.Server.Host,
		Port: cfg.Server.Port,
	}, logger)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processor
	processor.Start(ctx)

	// Start HTTP server in a goroutine
	go func() {
		if err := httpServer.Start(); err != nil {
			logger.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Start graceful shutdown
	logger.Info("Shutting down...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Stop processor
	if err := processor.Stop(); err != nil {
		logger.Errorf("Error stopping processor: %v", err)
	}

	// Stop HTTP server
	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Errorf("Error stopping server: %v", err)
	}

	logger.Info("Shutdown complete")
} 