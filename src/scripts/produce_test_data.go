package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Kafka writer configuration
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "gps-data",
		Balancer: &kafka.LeastBytes{},
	}

	defer w.Close()

	// Generate and send test GPS data
	for i := 0; i < 50; i++ {
		// Generate random GPS coordinates around a central point
		lat := 40.7128 + (rand.Float64()-0.5)*0.01 // Around New York
		lng := -74.0060 + (rand.Float64()-0.5)*0.01
		speed := rand.Float64() * 100 // Random speed between 0-100

		// Create GPS data message
		message := fmt.Sprintf(`{
			"timestamp": "%s",
			"latitude": %.6f,
			"longitude": %.6f,
			"speed": %.2f,
			"vehicle_id": "test-vehicle-1"
		}`, time.Now().Format(time.RFC3339), lat, lng, speed)

		// Send message to Kafka
		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(message),
			},
		)
		if err != nil {
			log.Fatalf("Failed to write message: %v", err)
		}

		fmt.Printf("Sent message: %s\n", message)
		time.Sleep(100 * time.Millisecond) // Wait between messages
	}

	fmt.Println("Finished sending test data")
} 