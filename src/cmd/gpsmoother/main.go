package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Niksan005/GPSmoother/internal/config"
	"github.com/Niksan005/GPSmoother/internal/transport/kafka"
)

func main() {
	// Create a context that will be canceled on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Starting GPSmoother service...")

	// Start health check server
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("Health check server error: %v", err)
		}
	}()

	// Load configuration
	cfg := config.NewConfig()

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&cfg.Kafka)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming messages in a goroutine
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Error in Kafka consumer: %v", err)
			stop() // Signal shutdown on error
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("Shutting down GPSmoother service...")
} 